package cache

import "time"

type FileCacheMetadata struct {
	Size         int64
	LastModified time.Time
	Key          string
}
