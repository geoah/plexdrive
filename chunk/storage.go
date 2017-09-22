package chunk

import (
	"errors"

	. "github.com/claudetech/loggo/default"
)

// ErrTimeout is a timeout error
var ErrTimeout = errors.New("timeout")

// Storage is a chunk storage
type Storage struct {
	ChunkSize int64
	MaxChunks int
	cache     Cache
	stack     *Stack
}

// Item represents a chunk in RAM
type Item struct {
	id    string
	bytes []byte
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int, cache Cache) *Storage {
	storage := Storage{
		ChunkSize: chunkSize,
		MaxChunks: maxChunks,
		cache:     cache,
		stack:     NewStack(maxChunks),
	}

	return &storage
}

// Clear removes all old chunks on disk (will be called on each program start)
func (s *Storage) Clear() error {
	return s.cache.Clear()
}

// Load a chunk from ram or creates it
func (s *Storage) Load(id string) []byte {
	if chunk := s.cache.Load(id); chunk != nil {
		s.stack.Touch(id)
		return chunk
	}
	return nil
}

// Store stores a chunk in the RAM and adds it to the disk storage queue
func (s *Storage) Store(id string, bytes []byte) error {
	deleteID := s.stack.Pop()
	if "" != deleteID {
		if err := s.cache.Remove(id); err != nil {
			Log.Warningf("Could not delete chunk %s, err: %s", deleteID, err)
		} else {
			Log.Debugf("Deleted chunk %s", deleteID)
		}
	}

	if err := s.cache.Store(id, bytes); err != nil {
		return err
	}

	s.stack.Push(id)
	return nil
}
