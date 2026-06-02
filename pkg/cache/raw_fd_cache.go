package cache

import (
	"container/list"
	"os"
	"sync"
)

type rawPageFDCache struct {
	mu      sync.Mutex
	max     int
	entries map[string]*list.Element
	lru     *list.List
}

type rawPageFDCacheEntry struct {
	path string
	file *os.File
	info os.FileInfo
}

type rawPageHandle struct {
	file   *os.File
	pooled bool
	hit    bool
}

func (h rawPageHandle) Close() error {
	if h.file == nil || h.pooled {
		return nil
	}
	return h.file.Close()
}

func newRawPageFDCache(max int) *rawPageFDCache {
	if max <= 0 {
		return nil
	}
	return &rawPageFDCache{
		max:     max,
		entries: make(map[string]*list.Element, max),
		lru:     list.New(),
	}
}

func (c *rawPageFDCache) open(path string) (rawPageHandle, error) {
	if c == nil {
		file, err := os.Open(path)
		return rawPageHandle{file: file}, err
	}
	if path == "" {
		return rawPageHandle{}, os.ErrInvalid
	}

	info, err := os.Stat(path)
	if err != nil {
		return rawPageHandle{}, err
	}

	c.mu.Lock()
	if handle, ok := c.cachedHandleLocked(path, info); ok {
		c.mu.Unlock()
		return handle, nil
	}
	c.mu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		return rawPageHandle{}, err
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return rawPageHandle{}, err
	}
	info = openedInfo

	_ = fadviseSequential(file.Fd())

	c.mu.Lock()
	if handle, ok := c.cachedHandleLocked(path, info); ok {
		c.mu.Unlock()
		_ = file.Close()
		return handle, nil
	}

	entry := &rawPageFDCacheEntry{path: path, file: file, info: info}
	c.entries[path] = c.lru.PushFront(entry)
	for len(c.entries) > c.max {
		c.removeElementLocked(c.lru.Back())
	}
	c.mu.Unlock()
	return rawPageHandle{file: file, pooled: true}, nil
}

func (c *rawPageFDCache) cachedHandleLocked(path string, info os.FileInfo) (rawPageHandle, bool) {
	elem := c.entries[path]
	if elem == nil {
		return rawPageHandle{}, false
	}
	entry := elem.Value.(*rawPageFDCacheEntry)
	if sameFileInfo(entry.info, info) {
		c.lru.MoveToFront(elem)
		return rawPageHandle{file: entry.file, pooled: true, hit: true}, true
	}
	c.removeElementLocked(elem)
	atomicAddRawFDCacheStale()
	return rawPageHandle{}, false
}

func (c *rawPageFDCache) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, elem := range c.entries {
		entry := elem.Value.(*rawPageFDCacheEntry)
		_ = entry.file.Close()
	}
	c.entries = make(map[string]*list.Element)
	c.lru.Init()
}

func (c *rawPageFDCache) removeElementLocked(elem *list.Element) {
	if c == nil || elem == nil {
		return
	}
	c.lru.Remove(elem)
	entry := elem.Value.(*rawPageFDCacheEntry)
	delete(c.entries, entry.path)
	_ = entry.file.Close()
}

func sameFileInfo(a os.FileInfo, b os.FileInfo) bool {
	if a == nil || b == nil {
		return false
	}
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}
