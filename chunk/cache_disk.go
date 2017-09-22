package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	. "github.com/claudetech/loggo/default"

	"github.com/GitbookIO/syncgroup"
)

// NewDiskCache create a new memory cache for chunks
func NewDiskCache(cacheDir string) (Cache, error) {
	if err := os.MkdirAll(cacheDir, os.ModePerm); err != nil {
		return nil, err
	}

	abs, err := filepath.Abs(cacheDir)
	if err != nil {
		return nil, err
	}

	c := &DiskCache{
		cacheDir: abs,
		lock:     syncgroup.NewMutexGroup(),
	}
	return c, nil
}

// DiskCache implements the Cache interface for caching chunks on disk
type DiskCache struct {
	cacheDir string
	lock     *syncgroup.MutexGroup
}

// Clear removes all cached files
func (c *DiskCache) Clear() error {
	// delete directory
	if err := os.RemoveAll(c.cacheDir); err != nil {
		return err
	}
	// recreate directory
	return os.MkdirAll(c.cacheDir, os.ModePerm)
}

// Load a chunk from disk
func (c *DiskCache) Load(id string) []byte {
	c.lock.RLock(id)
	defer c.lock.RUnlock(id)

	fn := c.getFilename(id)
	fl, err := os.Open(fn)
	if err != nil {
		return nil
	}
	defer fl.Close()

	data, err := ioutil.ReadAll(fl)
	if err != nil {
		Log.Warningf("error reading file %s, err: %s", id, err)
		return nil
	}

	if len(data) == 0 {
		return nil
	}

	return data
}

// Store caches a chunk in disk
func (c *DiskCache) Store(id string, bytes []byte) error {
	c.lock.Lock(id)
	defer c.lock.Unlock(id)

	fn := c.getFilename(id)
	fl, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer fl.Close()

	if _, err = fl.Write(bytes); err != nil {
		Log.Warningf("error writing file %s, err: %s", id, err)
		return err
	}

	return nil
}

// Remove a chunk from disk
func (c *DiskCache) Remove(id string) error {
	c.lock.Lock(id)
	defer c.lock.Unlock(id)

	fn := c.getFilename(id)
	return os.Remove(fn)
}

func (c *DiskCache) getFilename(id string) string {
	hs := sha256.New()
	hs.Write([]byte(id))
	hid := hex.EncodeToString(hs.Sum(nil))
	return path.Join(c.cacheDir, hid)
}
