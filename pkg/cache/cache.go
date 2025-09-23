package cache

type FileCache interface {
	Get(key string) (interface{}, error)
	Set(key string, value interface{}) error
	Delete(key string) error
}

type fileCache struct {
	cache map[string]interface{}
}

