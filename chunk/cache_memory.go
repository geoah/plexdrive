package chunk

import (
	"sync"
)

// NewMemoryCache create a new memory cache for chunks
func NewMemoryCache() (Cache, error) {
	c := &MemoryCache{
		chunks: make(map[string][]byte),
	}
	return c, nil
}

// MemoryCache implements the Cache interface for caching chunks in-memory
type MemoryCache struct {
	chunks map[string][]byte
	lock   sync.RWMutex
}

// Clear is not implemented as it is only called on startup
func (c *MemoryCache) Clear() error {
	return nil
}

// Load a chunk from memory
func (c *MemoryCache) Load(id string) []byte {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if chunk, exists := c.chunks[id]; exists {
		return chunk
	}
	return nil
}

// Store caches a chunk in memory
func (c *MemoryCache) Store(id string, bytes []byte) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	c.chunks[id] = bytes
	return nil
}

// Remove a chunk from memory
func (c *MemoryCache) Remove(id string) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	delete(c.chunks, id)
	return nil
}
