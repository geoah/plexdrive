package chunk

// Cache provides an abstraction for different types of chunk caching
type Cache interface {
	Clear() error
	Load(id string) []byte
	Store(id string, bytes []byte) error
	Remove(id string) error
}
