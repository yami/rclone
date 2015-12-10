// Package oss provides an interface to Aliyun object storage (OSS)
package oss

// FIXME need to prevent anything but ListDir working for oss://

import (
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"time"
	"strconv"

	"github.com/baiyubin/aliyun-oss-go-sdk/oss"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/swift"
)

// Register with Fs
func init() {
	fs.Register(&fs.Info{
		Name:  "oss",
		NewFs: NewFs,
		
		Options: []fs.Option{{
			Name: "access_key_id",
			Help: "OSS AccessKeyID - leave blank for anonymous access.",
		}, {
			Name: "access_key_secret",
			Help: "OSS AccessKeySecret (password) - leave blank for anonymous access.",
		}, {
			Name: "region",
			Help: "Region to connect to.",
			Examples: []fs.OptionExample{{
				Value: "oss-cn-hangzhou",
				Help:  "hangzhou",
			}},
		}, {
			Name: "endpoint",
			Help: "Endpoint for OSS API.\nLeave blank if using AWS to use the default endpoint for the region.\nSpecify if using an OSS clone such as Ceph.",
		}, {
			Name: "location_constraint",
			Help: "Location constraint - must be set to match the Region. Used when creating buckets only.",
			Examples: []fs.OptionExample{{
				Value: "",
				Help:  "Empty for US Region, Northern Virginia or Pacific Northwest.",
			}},
		}},
	})
}

// Constants
const (
	metaMtime     = "mtime" // the meta key to store mtime in - eg X-Amz-Meta-Mtime
	listChunkSize = 1000    // number of items to read at once
	maxRetries    = 10      // number of retries to make of operations
	httpDateFormat = "Mon, 02 Jan 2006 15:04:05 GMT" // HTTP Date format
)

// Fs represents a remote OSS server
type Fs struct {
	name               string           // the name of the remote
	bucket             string           // the name of bucket
	c                  *oss.Client      // the OSS Client
	b                  *oss.Bucket      // the OSS Bucket
	perm               string           // permissions for new buckets
	root               string           // root of the bucket - ignore all objects above this
	locationConstraint string           // location constraint of new buckets
}

// Object describes an OSS object
type Object struct {
	// Will definitely have everything but meta which may be nil
	//
	// List will read everything but meta - to fill that in need to call
	// readMetaData
	fs           *Fs                // what this object is part of
	remote       string             // The remote path
	etag         string             // md5sum of the object
	bytes        int64              // size of the object
	lastModified time.Time          // Last modified
	meta         map[string]string // The object metadata if known - may be nil
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	if f.root == "" {
		return f.bucket
	}
	return f.bucket + "/" + f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.root == "" {
		return fmt.Sprintf("OSS bucket %s", f.bucket)
	}
	return fmt.Sprintf("OSS bucket %s path %s", f.bucket, f.root)
}

// Pattern to match a oss path
var matcher = regexp.MustCompile(`^([^/]*)(.*)$`)

// parseParse parses a oss 'url'
func ossParsePath(path string) (bucket, directory string, err error) {
	parts := matcher.FindStringSubmatch(path)
	if parts == nil {
		err = fmt.Errorf("Couldn't parse bucket out of oss path %q", path)
	} else {
		bucket, directory = parts[1], parts[2]
		directory = strings.Trim(directory, "/")
	}
	return
}

func ossClient(name string) (*oss.Client, error) {
	accessKeyID := fs.ConfigFile.MustValue(name, "access_key_id")
	accessKeySecret := fs.ConfigFile.MustValue(name, "access_key_secret")

	endpoint := fs.ConfigFile.MustValue(name, "endpoint")

	c, err := oss.NewClient(endpoint, accessKeyID, accessKeySecret)

	return c, err
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(name, root string) (fs.Fs, error) {
	bucket, directory, err := ossParsePath(root)
	if err != nil {
		return nil, err
	}
	c, err := ossClient(name)
	if err != nil {
		return nil, err
	}

	var b *oss.Bucket
	if bucket != "" {
		err = c.CreateBucket(bucket)
		if err != nil {
			return nil, err
		}

		b, err = c.Bucket(bucket)
		if err != nil {
			return nil, err
		}
	}

	f := &Fs{
		name:   name,
		bucket: bucket,
		c:      c,
		b:      b,
		root:               directory,
		locationConstraint: fs.ConfigFile.MustValue(name, "location_constraint"),
	}
	if f.root != "" {
		f.root += "/"
		// Check to see if the object exists

		_, err = f.b.GetObjectDetailedMeta(directory)
		if err == nil {
			remote := path.Base(directory)
			f.root = path.Dir(directory)
			if f.root == "." {
				f.root = ""
			} else {
				f.root += "/"
			}
			obj := f.NewFsObject(remote)
			// return a Fs Limited to this object
			return fs.NewLimited(f, obj), nil
		}
	}

	return f, nil
}

// Return an FsObject from a path
//
// May return nil if an error occurred
func (f *Fs) newFsObjectWithInfo(remote string, info *oss.ObjectProperties) fs.Object {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		o.lastModified = info.LastModified
		o.etag = info.ETag
		o.bytes = int64(info.Size)
	} else {
		err := o.readMetaData() // reads info and meta, returning an error
		if err != nil {
			// logged already FsDebug("Failed to read info: %s", err)
			return nil
		}
	}
	return o
}

// NewFsObject returns an FsObject from a path
//
// May return nil if an error occurred
func (f *Fs) NewFsObject(remote string) fs.Object {
	return f.newFsObjectWithInfo(remote, nil)
}

// list the objects into the function supplied
//
// If directories is set it only sends directories
func (f *Fs) list(directories bool, fn func(string, *oss.ObjectProperties)) {
	maxKeys := int64(listChunkSize)
	delimiter := ""
	if directories {
		delimiter = "/"
	}
	var marker string
	for {
		// FIXME need to implement ALL loop
		resp, err := f.b.ListObjects(
			oss.Prefix(f.root),
			oss.Delimiter(delimiter),
			oss.MaxKeys(int(maxKeys)),
			oss.Marker(marker))

		if err != nil {
			fs.Stats.Error()
			fs.ErrorLog(f, "Couldn't read bucket %q: %s", f.bucket, err)
			break
		} else {
			rootLength := len(f.root)
			if directories {
				for _, commonPrefix := range resp.CommonPrefixes {
					remote := commonPrefix
					if !strings.HasPrefix(remote, f.root) {
						fs.Log(f, "Odd name received %q", remote)
						continue
					}
					remote = remote[rootLength:]
					if strings.HasSuffix(remote, "/") {
						remote = remote[:len(remote)-1]
					}
					fn(remote, &oss.ObjectProperties{Key: remote})
				}
			} else {
				for _, object := range resp.Objects {
					if !strings.HasPrefix(object.Key, f.root) {
						fs.Log(f, "Odd name received %q", object.Key)
						continue
					}
					remote := object.Key[rootLength:]
					fn(remote, &object)
				}
			}

			if !resp.IsTruncated {
				break
			}

			marker = resp.NextMarker
		}
	}
}

// List walks the path returning a channel of FsObjects
func (f *Fs) List() fs.ObjectsChan {
	out := make(fs.ObjectsChan, fs.Config.Checkers)
	if f.bucket == "" {
		// Return no objects at top level list
		close(out)
		fs.Stats.Error()
		fs.ErrorLog(f, "Can't list objects at root - choose a bucket using lsd")
	} else {
		go func() {
			defer close(out)
			f.list(false, func(remote string, object *oss.ObjectProperties) {
				if fs := f.newFsObjectWithInfo(remote, object); fs != nil {
					out <- fs
				}
			})
		}()
	}
	return out
}

// ListDir lists the buckets
func (f *Fs) ListDir() fs.DirChan {
	out := make(fs.DirChan, fs.Config.Checkers)
	if f.bucket == "" {
		// List the buckets
		go func() {
			defer close(out)

			resp, err := f.c.ListBuckets()
			if err != nil {
				fs.Stats.Error()
				fs.ErrorLog(f, "Couldn't list buckets: %s", err)
			} else {
				for _, bucket := range resp.Buckets {
					out <- &fs.Dir{
						Name:  bucket.Name,
						When:  bucket.CreationDate,
						Bytes: -1,
						Count: -1,
					}
				}
			}
		}()
	} else {
		// List the directories in the path in the bucket
		go func() {
			defer close(out)
			f.list(true, func(remote string, object *oss.ObjectProperties) {
				out <- &fs.Dir{
					Name:  remote,
					Bytes: int64(object.Size),
					Count: 0,
				}
			})
		}()
	}
	return out
}

// Put the FsObject into the bucket
func (f *Fs) Put(in io.Reader, remote string, modTime time.Time, size int64) (fs.Object, error) {
	// Temporary Object under construction
	fs := &Object{
		fs:     f,
		remote: remote,
	}
	return fs, fs.Update(in, modTime, size)
}

// Mkdir creates the bucket if it doesn't exist
func (f *Fs) Mkdir() error {
	return f.c.CreateBucket(f.bucket)
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir() error {
	if f.root != "" {
		return nil
	}

	return f.c.DeleteBucket(f.bucket)
}

// Precision of the remote
func (f *Fs) Precision() time.Duration {
	return time.Nanosecond
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given
//
// It returns the destination Object and a possible error
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debug(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	srcFs := srcObj.fs

	_, err := f.b.CopyObject(srcFs.root + srcObj.remote, f.root + remote, oss.MetadataDirective(oss.META_COPY))
	if err != nil {
		return nil, err
	}
	return f.NewFsObject(remote), err
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Fs {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

var matchMd5 = regexp.MustCompile(`^[0-9a-f]{32}$`)

// Md5sum returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Md5sum() (string, error) {
	etag := strings.Trim(strings.ToLower(o.etag), `"`)
	// Check the etag is a valid md5sum
	if !matchMd5.MatchString(etag) {
		// fs.Debug(o, "Invalid md5sum (probably multipart uploaded) - ignoring: %q", etag)
		return "", nil
	}
	return etag, nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.bytes
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData() (err error) {
	if o.meta != nil {
		return nil
	}

	key := o.fs.root + o.remote

	metas, err := o.fs.b.GetObjectDetailedMeta(key)
	if err != nil {
		fs.Debug(o, "Failed to read info: %s", err)
		return err
	}
	var size int64

	if metas[oss.HTTP_HEADER_CONTENT_LENGTH] != "" {
		size, err = strconv.ParseInt(metas[oss.HTTP_HEADER_CONTENT_LENGTH], 10, 0)
		if err != nil {
			fs.Debug(o, "Failed to convert content length: %s", metas[oss.HTTP_HEADER_CONTENT_LENGTH])
			return err
		}
	}

	o.etag = metas[oss.HTTP_HEADER_ETAG]
	o.bytes = size

	o.meta = metas
	for k, v := range metas {
		lower_key := strings.ToLower(k)
		if strings.HasPrefix(lower_key, "x-oss-meta-") {
			o.meta[strings.TrimPrefix(lower_key, "x-oss-meta-")] = v
		}
	}

	if metas[oss.HTTP_HEADER_LAST_MODIFIED] == "" {
		fs.Log(o, "Failed to read last modified from HEAD: %s", err)
		o.lastModified = time.Now()
	} else {
		o.lastModified, err = time.Parse(httpDateFormat, metas[oss.HTTP_HEADER_LAST_MODIFIED])
	}
	return err
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime() time.Time {
	err := o.readMetaData()
	if err != nil {
		fs.Log(o, "Failed to read metadata: %s", err)
		return time.Now()
	}
	// read mtime out of metadata if available
	mtime, ok := o.meta[metaMtime]
	if !ok  {
		return o.lastModified
	}
	modTime, err := swift.FloatStringToTime(mtime)
	if err != nil {
		fs.Log(o, "Failed to read mtime from object: %s", err)
		return o.lastModified
	}
	return modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(modTime time.Time) {
	err := o.readMetaData()
	if err != nil {
		fs.Stats.Error()
		fs.ErrorLog(o, "Failed to read metadata: %s", err)
		return
	}
	err = o.fs.b.SetObjectMeta(o.fs.root + o.remote, oss.Meta(metaMtime, swift.TimeToFloatString(modTime)))
	if err != nil {
		fs.Stats.Error()
		fs.ErrorLog(o, "Failed to update remote mtime: %s", err)
	}
}

// Storable raturns a boolean indicating if this object is storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open() (in io.ReadCloser, err error) {
	resp, err := o.fs.b.GetObjectReader(o.fs.root + o.remote)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Update the Object from in with modTime and size
func (o *Object) Update(in io.Reader, modTime time.Time, size int64) error {
	// Set the mtime in the meta data
	options := []oss.Option {
		oss.Meta(metaMtime, swift.TimeToFloatString(modTime)),
		oss.ContentType(fs.MimeType(o)),
	}

	key := o.fs.root + o.remote
	err := o.fs.b.PutObject(key, in, options...)
	if err != nil {
		return err
	}

	// Read the metadata from the newly created object
	o.meta = nil // wipe old metadata
	err = o.readMetaData()
	return err
}

// Remove an object
func (o *Object) Remove() error {
	return o.fs.b.DeleteObject(o.fs.root + o.remote)
}

// Check the interfaces are satisfied
var (
	_ fs.Fs     = &Fs{}
	_ fs.Copier = &Fs{}
	_ fs.Object = &Object{}
)

