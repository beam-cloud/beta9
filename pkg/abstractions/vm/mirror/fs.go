package mirror

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

// Source serves the stored bytes of mirror objects.
type Source interface {
	// ReadObject fills dest with the object's stored bytes from offset.
	ReadObject(ctx context.Context, object Object, offset int64, dest []byte) error
}

// Placement presents the mirrored tree as one guest home directory. The
// manifest carries no ownership: every entry under a placement belongs to
// its user.
type Placement struct {
	Path string // absolute guest path of the home directory
	UID  uint32
	GID  uint32
	Mode uint32 // of the home directory itself; 0700 when zero
}

// RootHome is where a VM is used over SSH and exec. Any other account is the
// image's own, so whoever mounts the filesystem names its home.
var RootHome = Placement{Path: "/root", Mode: 0o700}

type FSOptions struct {
	// Placements are the homes the tree appears as; RootHome when empty.
	Placements []Placement
	// DecodedCacheBytes bounds the decompressed extents kept for reuse.
	DecodedCacheBytes int
}

const (
	defaultDecodedCacheBytes = 64 << 20
	fsTimeout                = 24 * time.Hour
	fsBlockSize              = 4096
)

type scaffoldNode struct {
	children []scaffoldChild // sorted by name
}

type scaffoldChild struct {
	name string
	node uint64
}

// FS is the read-only filesystem a worker mounts as the mirror lower layer.
// It is stateless: a node ID is a position in the manifest, so the server
// keeps nothing per inode however much of the tree a guest walks, and IDs
// are the same on every mount of a generation.
//
// The mount root holds only the directories leading to each placement; the
// manifest root appears as each placement's home.
type FS struct {
	fuse.RawFileSystem
	manifest   *Manifest
	source     Source
	placements []Placement
	scaffold   []scaffoldNode // node ID - 1
	entries    uint64
	revoked    atomic.Bool

	flights singleflight.Group
	mu      sync.Mutex
	mounts  []*mounted
	decoded map[uint32]*list.Element
	lru     *list.List
	held    int
	budget  int
	starts  map[uint32][]uint64 // file -> raw offset of each extent
}

// mounted is one mount of the filesystem. gone is set once its server has
// stopped, after which the server's device is closed and must not be
// written to.
type mounted struct {
	server *fuse.Server
	gone   atomic.Bool
}

type decodedExtent struct {
	extent uint32
	data   []byte
}

func NewFS(manifest *Manifest, source Source, opts FSOptions) (*FS, error) {
	placements := opts.Placements
	if len(placements) == 0 {
		placements = []Placement{RootHome}
	}
	f := &FS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		manifest:      manifest,
		source:        source,
		entries:       uint64(manifest.Entries()),
		decoded:       map[uint32]*list.Element{},
		lru:           list.New(),
		budget:        opts.DecodedCacheBytes,
		starts:        map[uint32][]uint64{},
	}
	if f.budget <= 0 {
		f.budget = defaultDecodedCacheBytes
	}

	// Scaffold directories get the low node IDs; the views follow.
	type dir struct{ children map[string]int }
	dirs := []*dir{{children: map[string]int{}}}
	homes := map[string]bool{}
	type home struct {
		parent int
		name   string
	}
	var mounts []home
	for _, placement := range placements {
		clean := path.Clean(placement.Path)
		if !path.IsAbs(placement.Path) || clean == "/" || homes[clean] {
			return nil, fmt.Errorf("invalid mirror placement %q", placement.Path)
		}
		homes[clean] = true
		if placement.Mode == 0 {
			placement.Mode = 0o700
		}
		placement.Path = clean
		f.placements = append(f.placements, placement)
		parts := strings.Split(clean[1:], "/")
		at := 0
		for _, part := range parts[:len(parts)-1] {
			next, ok := dirs[at].children[part]
			if !ok {
				next = len(dirs)
				dirs = append(dirs, &dir{children: map[string]int{}})
				dirs[at].children[part] = next
			}
			at = next
		}
		mounts = append(mounts, home{parent: at, name: parts[len(parts)-1]})
	}
	for _, placement := range f.placements {
		for other := range homes {
			if strings.HasPrefix(other, placement.Path+"/") {
				return nil, fmt.Errorf("mirror placement %q is inside %q", other, placement.Path)
			}
		}
	}
	f.scaffold = make([]scaffoldNode, len(dirs))
	for i, d := range dirs {
		for name, child := range d.children {
			f.scaffold[i].children = append(f.scaffold[i].children, scaffoldChild{name: name, node: uint64(child) + 1})
		}
	}
	for view, mount := range mounts {
		node := &f.scaffold[mount.parent]
		node.children = append(node.children, scaffoldChild{name: mount.name, node: f.nodeID(view, 0)})
	}
	for i := range f.scaffold {
		children := f.scaffold[i].children
		sort.Slice(children, func(a, b int) bool { return children[a].name < children[b].name })
	}
	return f, nil
}

func (f *FS) String() string { return "beam-mirror" }

// ErrRevoked is what reading a file returns once its mirror is purged.
var ErrRevoked = errors.New("mirror was purged")

// Init records the server of a mount, which is how Revoke reaches the
// kernel.
func (f *FS) Init(server *fuse.Server) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mounts = append(f.mounts, &mounted{server: server})
}

func (f *FS) unmounted(server *fuse.Server) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, mount := range f.mounts {
		if mount.server == server {
			mount.gone.Store(true)
			f.mounts = append(f.mounts[:i:i], f.mounts[i+1:]...)
			return
		}
	}
}

// Revoke stops the filesystem from returning file contents, for a mirror
// that has been purged while mounted. Names and attributes stay, so that the
// overlay above keeps working for everything the guest wrote itself.
//
// The kernel keeps what it has read from an immutable file and serves it
// again without asking, so it is told to drop every file's data. A read that
// was already under way holds its pages locked in that cache, and the kernel
// waits for it before dropping them. What a guest holds in its own memory is
// beyond reach.
func (f *FS) Revoke() {
	f.revoked.Store(true)
	f.mu.Lock()
	f.decoded, f.held = map[uint32]*list.Element{}, 0
	f.lru.Init()
	mounts := f.mounts
	f.mu.Unlock()

	for _, mount := range mounts {
		for view := range f.placements {
			for index := range uint32(f.entries) {
				if mount.gone.Load() {
					break
				}
				if f.manifest.Kind(index) == KindFile {
					// ENOENT is the answer for a file the kernel has not
					// looked up.
					mount.server.InodeNotify(f.nodeID(view, index), 0, 0)
				}
			}
		}
	}
}

func (f *FS) Revoked() bool { return f.revoked.Load() }

// Manifest is the generation the filesystem serves.
func (f *FS) Manifest() *Manifest { return f.manifest }

// Placements are the homes the tree appears as.
func (f *FS) Placements() []Placement { return append([]Placement(nil), f.placements...) }

func (f *FS) nodeID(view int, entry uint32) uint64 {
	return uint64(len(f.scaffold)) + 1 + uint64(view)*f.entries + uint64(entry)
}

// resolve splits a node ID into a scaffold directory (view < 0) or a
// manifest entry seen through a placement.
func (f *FS) resolve(node uint64) (view int, entry uint32, ok bool) {
	if node == 0 {
		return 0, 0, false
	}
	if node <= uint64(len(f.scaffold)) {
		return -1, uint32(node - 1), true
	}
	offset := node - uint64(len(f.scaffold)) - 1
	if offset >= uint64(len(f.placements))*f.entries {
		return 0, 0, false
	}
	return int(offset / f.entries), uint32(offset % f.entries), true
}

func (f *FS) attr(node uint64, out *fuse.Attr) {
	view, index, _ := f.resolve(node)
	*out = fuse.Attr{Ino: node, Blksize: fsBlockSize, Nlink: 1}
	if view < 0 {
		out.Mode, out.Nlink = syscall.S_IFDIR|0o755, 2
		return
	}
	entry := f.manifest.Entry(index)
	placement := f.placements[view]
	out.Owner = fuse.Owner{Uid: placement.UID, Gid: placement.GID}
	out.Size = entry.Size
	out.Blocks = (entry.Size + 511) / 512
	seconds, nanos := entry.MtimeNs/1e9, entry.MtimeNs%1e9
	if nanos < 0 {
		seconds, nanos = seconds-1, nanos+1e9
	}
	out.Mtime, out.Mtimensec = uint64(seconds), uint32(nanos)
	out.Atime, out.Atimensec, out.Ctime, out.Ctimensec = out.Mtime, out.Mtimensec, out.Mtime, out.Mtimensec
	switch entry.Kind {
	case KindDir:
		out.Mode, out.Nlink, out.Size = syscall.S_IFDIR|entry.Mode, 2, fsBlockSize
		if index == 0 {
			out.Mode = syscall.S_IFDIR | placement.Mode
		}
	case KindSymlink:
		out.Mode = syscall.S_IFLNK | 0o777
	default:
		out.Mode = syscall.S_IFREG | entry.Mode
	}
}

func (f *FS) fillEntry(node uint64, out *fuse.EntryOut) {
	out.NodeId, out.Generation = node, 1
	f.attr(node, &out.Attr)
	out.SetEntryTimeout(fsTimeout)
	out.SetAttrTimeout(fsTimeout)
}

func (f *FS) child(parent uint64, name string) (uint64, bool) {
	view, index, ok := f.resolve(parent)
	if !ok {
		return 0, false
	}
	if view < 0 {
		children := f.scaffold[index].children
		i := sort.Search(len(children), func(i int) bool { return children[i].name >= name })
		if i < len(children) && children[i].name == name {
			return children[i].node, true
		}
		return 0, false
	}
	entry, ok := f.manifest.Lookup(index, name)
	if !ok {
		return 0, false
	}
	return f.nodeID(view, entry), true
}

func (f *FS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	node, ok := f.child(header.NodeId, name)
	if !ok {
		// A cached negative entry is safe: the tree never changes.
		out.SetEntryTimeout(fsTimeout)
		return fuse.ENOENT
	}
	f.fillEntry(node, out)
	return fuse.OK
}

func (f *FS) Forget(nodeid, nlookup uint64) {}

func (f *FS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	if _, _, ok := f.resolve(input.NodeId); !ok {
		return fuse.ENOENT
	}
	f.attr(input.NodeId, &out.Attr)
	out.SetTimeout(fsTimeout)
	return fuse.OK
}

func (f *FS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	view, index, ok := f.resolve(header.NodeId)
	if !ok || view < 0 || f.manifest.Kind(index) != KindSymlink {
		return nil, fuse.EINVAL
	}
	return f.manifest.Target(index), fuse.OK
}

func (f *FS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	if input.Mask&2 != 0 {
		return fuse.EROFS
	}
	return fuse.OK
}

// Open and OpenDir are not implemented on purpose: the kernel then stops
// sending opens and releases, keeps file data cached across opens, and
// caches directory listings, all of which hold for an immutable tree.
func (f *FS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (f *FS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (f *FS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {}
func (f *FS) ReleaseDir(input *fuse.ReleaseIn)                      {}

func (f *FS) readDir(input *fuse.ReadIn, out *fuse.DirEntryList, plus bool) fuse.Status {
	view, index, ok := f.resolve(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}
	add := func(position uint64, name string, node uint64, mode uint32) bool {
		entry := fuse.DirEntry{Name: name, Ino: node, Mode: mode, Off: position + 1}
		if !plus {
			return out.AddDirEntry(entry)
		}
		lookup := out.AddDirLookupEntry(entry)
		if lookup == nil {
			return false
		}
		f.fillEntry(node, lookup)
		return true
	}
	if view < 0 {
		children := f.scaffold[index].children
		for at := input.Offset; at < uint64(len(children)); at++ {
			if !add(at, children[at].name, children[at].node, syscall.S_IFDIR) {
				break
			}
		}
		return fuse.OK
	}
	dir := f.manifest.Entry(index)
	if dir.Kind != KindDir {
		return fuse.ENOTDIR
	}
	for at := input.Offset; at < uint64(dir.Count); at++ {
		child := dir.First + uint32(at)
		mode := uint32(syscall.S_IFREG)
		switch f.manifest.Kind(child) {
		case KindDir:
			mode = syscall.S_IFDIR
		case KindSymlink:
			mode = syscall.S_IFLNK
		}
		if !add(at, string(f.manifest.Name(child)), f.nodeID(view, child), mode) {
			break
		}
	}
	return fuse.OK
}

func (f *FS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return f.readDir(input, out, false)
}

func (f *FS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return f.readDir(input, out, true)
}

func (f *FS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	*out = fuse.StatfsOut{
		Blocks:  (f.manifest.TotalSize() + fsBlockSize - 1) / fsBlockSize,
		Files:   f.entries * uint64(len(f.placements)),
		Bsize:   fsBlockSize,
		Frsize:  fsBlockSize,
		NameLen: MaxNameLen,
	}
	return fuse.OK
}

func (f *FS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	view, index, ok := f.resolve(input.NodeId)
	if !ok || view < 0 {
		return nil, fuse.EISDIR
	}
	ctx := &fuse.Context{Caller: input.Caller, Cancel: cancel}
	n, err := f.ReadFile(ctx, index, input.Offset, buf)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fuse.EINTR
		}
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf[:n]), fuse.OK
}

// extentStarts returns the raw file offset at which each extent of a
// multi-extent file begins.
func (f *FS) extentStarts(index uint32, entry Entry) []uint64 {
	f.mu.Lock()
	starts, ok := f.starts[index]
	f.mu.Unlock()
	if ok {
		return starts
	}
	starts = make([]uint64, entry.Count)
	var offset uint64
	for i := range entry.Count {
		starts[i] = offset
		offset += uint64(f.manifest.Extent(entry.First + i).RawLen)
	}
	f.mu.Lock()
	if len(f.starts) >= 256 {
		for other := range f.starts {
			delete(f.starts, other)
			break
		}
	}
	f.starts[index] = starts
	f.mu.Unlock()
	return starts
}

// ReadFile reads from file entry index at offset, like io.ReaderAt but
// returning a short count without an error at the end of the file.
func (f *FS) ReadFile(ctx context.Context, index uint32, offset uint64, dest []byte) (int, error) {
	entry := f.manifest.Entry(index)
	if entry.Kind != KindFile {
		return 0, syscall.EISDIR
	}
	if f.revoked.Load() {
		return 0, ErrRevoked
	}
	if offset >= entry.Size {
		return 0, nil
	}
	if remaining := entry.Size - offset; uint64(len(dest)) > remaining {
		dest = dest[:remaining]
	}
	at, start := uint32(0), uint64(0)
	if entry.Count > 1 {
		starts := f.extentStarts(index, entry)
		at = uint32(sort.Search(len(starts), func(i int) bool { return starts[i] > offset }) - 1)
		start = starts[at]
	}
	read := 0
	for read < len(dest) {
		number := entry.First + at
		extent := f.manifest.Extent(number)
		skip := offset + uint64(read) - start
		want := min(uint64(len(dest)-read), uint64(extent.RawLen)-skip)
		if extent.Compressed() {
			raw, err := f.decodedExtent(ctx, number, extent)
			if err != nil {
				return read, err
			}
			copy(dest[read:], raw[skip:skip+want])
		} else {
			object := f.manifest.Object(extent.Object)
			if err := f.source.ReadObject(ctx, object, int64(uint64(extent.Offset)+skip), dest[read:read+int(want)]); err != nil {
				return read, err
			}
		}
		read += int(want)
		start += uint64(extent.RawLen)
		at++
	}
	return read, nil
}

func (f *FS) decodedExtent(ctx context.Context, number uint32, extent Extent) ([]byte, error) {
	f.mu.Lock()
	if element, ok := f.decoded[number]; ok {
		f.lru.MoveToFront(element)
		data := element.Value.(*decodedExtent).data
		f.mu.Unlock()
		return data, nil
	}
	f.mu.Unlock()

	// The flight is shared, so one caller's cancellation must not fail the
	// others: the fetch runs detached from any single request.
	result := f.flights.DoChan(fmt.Sprint(number), func() (any, error) {
		detached := context.WithoutCancel(ctx)
		// A guest decides how many extents are read at once, so each read
		// takes what it is about to hold from the budget first.
		weight := int64(extent.StoredLen) + int64(extent.RawLen)
		if err := acquireBudget(detached, decodeBudget, weight); err != nil {
			return nil, fmt.Errorf("wait to read a mirror extent: %w", err)
		}
		defer decodeBudget.Release(weight)
		stored := make([]byte, extent.StoredLen)
		object := f.manifest.Object(extent.Object)
		if err := f.source.ReadObject(detached, object, int64(extent.Offset), stored); err != nil {
			return nil, err
		}
		raw, err := decompress(stored, uint64(extent.RawLen))
		if err != nil {
			return nil, fmt.Errorf("decompress extent of object %s: %w", object.Digest, err)
		}
		f.mu.Lock()
		if _, ok := f.decoded[number]; !ok && !f.revoked.Load() {
			f.decoded[number] = f.lru.PushFront(&decodedExtent{extent: number, data: raw})
			f.held += len(raw)
			for f.held > f.budget && f.lru.Len() > 1 {
				oldest := f.lru.Remove(f.lru.Back()).(*decodedExtent)
				delete(f.decoded, oldest.extent)
				f.held -= len(oldest.data)
			}
		}
		f.mu.Unlock()
		return raw, nil
	})
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			return nil, outcome.Err
		}
		return outcome.Val.([]byte), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Mount serves the filesystem at mountPoint and returns once it is mounted.
func Mount(mountPoint string, fsys *FS) (*fuse.Server, error) {
	server, err := fuse.NewServer(fsys, mountPoint, &fuse.MountOptions{
		FsName:               "beam-mirror",
		Name:                 "mirror",
		Options:              []string{"ro", "default_permissions", "nosuid", "nodev"},
		AllowOther:           true,
		DisableXAttrs:        true,
		EnableSymlinkCaching: true,
		MaxBackground:        512,
		MaxWrite:             1 << 20,
		MaxReadAhead:         1 << 20,
	})
	if err != nil {
		return nil, err
	}
	go func() {
		server.Serve()
		fsys.unmounted(server)
	}()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return nil, err
	}
	return server, nil
}

// ObjectCache keeps verified objects for later reads.
type ObjectCache interface {
	// ReadObject reports whether it filled dest from the cached object.
	ReadObject(ctx context.Context, object Object, offset int64, dest []byte) bool
	StoreObject(ctx context.Context, object Object, data []byte) error
}

// ContentStore is a cache keyed by the SHA-256 of what it holds, shared by
// every tenant on a node.
type ContentStore interface {
	ReadContent(ctx context.Context, key string, offset int64, dest []byte) bool
	StoreContent(ctx context.Context, key string, content []byte) error
}

// ScopedCache keeps one mirror's objects in a ContentStore.
//
// A manifest is written by a tenant and names objects by digest. Looking
// those digests up in a store that other tenants fill would hand a tenant
// any cached object whose digest it can name, without it ever having held
// the bytes. So an object is stored with its mirror's scope appended, under
// the SHA-256 of the whole: only a reader with the same scope computes that
// key, and only someone holding the bytes can have stored under it.
//
// The key can be computed from the digest and size alone because SHA-256
// can be continued from a digest. The object is stored followed by its own
// SHA-256 padding, and at that point the hash state is the object's digest.
// What follows is the scope and the object's size, so that a manifest which
// declares a wrong size for a digest finds nothing rather than the padding.
type ScopedCache struct {
	Store ContentStore
	Scope []byte
}

// sha256Padding is what SHA-256 appends to a message of size bytes.
func sha256Padding(size uint64) []byte {
	padding := make([]byte, 1+(119-size%64)%64, 72)
	padding[0] = 0x80
	return binary.BigEndian.AppendUint64(padding, size*8)
}

func (c ScopedCache) suffix(size uint64) []byte {
	suffix := make([]byte, 0, len(c.Scope)+8)
	suffix = append(suffix, c.Scope...)
	return binary.BigEndian.AppendUint64(suffix, size)
}

func (c ScopedCache) key(object Object) string {
	// crypto/sha256's saved state: magic, the eight state words, the
	// unprocessed block, and the count of bytes hashed.
	state := make([]byte, 0, 4+sha256.Size+sha256.BlockSize+8)
	state = append(state, "sha\x03"...)
	state = append(state, object.Digest[:]...)
	state = append(state, make([]byte, sha256.BlockSize)...)
	state = binary.BigEndian.AppendUint64(state, object.Size+uint64(len(sha256Padding(object.Size))))
	hash := sha256.New()
	if err := hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		return ""
	}
	hash.Write(c.suffix(object.Size))
	return hex.EncodeToString(hash.Sum(nil))
}

func (c ScopedCache) ReadObject(ctx context.Context, object Object, offset int64, dest []byte) bool {
	key := c.key(object)
	return key != "" && c.Store.ReadContent(ctx, key, offset, dest)
}

func (c ScopedCache) StoreObject(ctx context.Context, object Object, data []byte) error {
	key := c.key(object)
	if key == "" {
		return errors.New("cannot derive a cache key")
	}
	size := uint64(len(data))
	content := make([]byte, 0, len(data)+72+len(c.Scope)+8)
	content = append(content, data...)
	content = append(content, sha256Padding(size)...)
	content = append(content, c.suffix(size)...)
	return c.Store.StoreContent(ctx, key, content)
}

// CachedSource reads objects from a cache and, on a miss, downloads the
// whole object, checks it against its name and puts it in the cache. An
// object that does not match its digest is never served.
type CachedSource struct {
	Cache ObjectCache
	// Download opens the object in the store.
	Download func(ctx context.Context, digest Digest) (io.ReadCloser, error)
	Attempts int
	// RecentBytes bounds the downloaded objects held in memory, which is
	// what serves reads when the cache cannot take an object.
	RecentBytes int

	flights singleflight.Group
	mu      sync.Mutex
	recent  map[Digest]*list.Element
	order   list.List
	held    int
}

type recentObject struct {
	digest Digest
	data   []byte
}

var ErrObjectMismatch = errors.New("mirror object does not match its digest")

const (
	downloadBudgetBytes = 128 << 20
	// An extent is at most MaxExtentLen stored and as much decoded.
	decodeBudgetBytes = 128 << 20
	budgetWait        = 2 * time.Minute
)

// Guests decide how much is read at once, so what the mounts in this process
// hold for reads in progress comes out of two budgets they all share, taken
// before anything is allocated: downloadBudget for objects on their way from
// the store, and decodeBudget for extents being decompressed. A manifest can
// be larger than a file object and takes the whole download budget if it
// exceeds it.
var (
	downloadBudget = semaphore.NewWeighted(downloadBudgetBytes)
	decodeBudget   = semaphore.NewWeighted(decodeBudgetBytes)
)

func acquireBudget(ctx context.Context, budget *semaphore.Weighted, weight int64) error {
	ctx, cancel := context.WithTimeout(ctx, budgetWait)
	defer cancel()
	return budget.Acquire(ctx, weight)
}

// downloadWeight is what an object takes from the download budget.
func downloadWeight(object Object) int64 {
	return min(int64(object.Size)+1, downloadBudgetBytes)
}

func (s *CachedSource) ReadObject(ctx context.Context, object Object, offset int64, dest []byte) error {
	if object.Size > MaxManifestSize {
		return ErrObjectMismatch
	}
	if offset < 0 || uint64(offset)+uint64(len(dest)) > object.Size {
		return fmt.Errorf("read past the end of object %s", object.Digest)
	}
	if data := s.remembered(object.Digest); data != nil {
		copy(dest, data[offset:])
		return nil
	}
	if s.Cache != nil && s.Cache.ReadObject(ctx, object, offset, dest) {
		return nil
	}
	result := s.flights.DoChan(string(object.Digest[:]), func() (any, error) {
		return s.fetch(context.WithoutCancel(ctx), object)
	})
	select {
	case outcome := <-result:
		if outcome.Err != nil {
			return outcome.Err
		}
		copy(dest, outcome.Val.([]byte)[offset:])
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Forget drops the downloaded objects held in memory.
func (s *CachedSource) Forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recent, s.held = nil, 0
	s.order.Init()
}

func (s *CachedSource) remembered(digest Digest) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	element, ok := s.recent[digest]
	if !ok {
		return nil
	}
	s.order.MoveToFront(element)
	return element.Value.(*recentObject).data
}

func (s *CachedSource) fetch(ctx context.Context, object Object) ([]byte, error) {
	if data := s.remembered(object.Digest); data != nil {
		return data, nil
	}
	weight := downloadWeight(object)
	if err := acquireBudget(ctx, downloadBudget, weight); err != nil {
		return nil, fmt.Errorf("wait to fetch mirror object %s: %w", object.Digest, err)
	}
	defer downloadBudget.Release(weight)

	data, err := s.retrieve(ctx, object)
	if err != nil {
		return nil, err
	}
	s.remember(object.Digest, data)
	if s.Cache != nil {
		_ = s.Cache.StoreObject(ctx, object, data)
	}
	return data, nil
}

// prefetchPoll is how often a prefetch looks for room in the download budget.
const prefetchPoll = 50 * time.Millisecond

// Prefetch puts an object in the cache ahead of any read of it, unless the
// cache has it already, and reports whether it downloaded it. It takes room
// in the download budget only while no read is waiting for some, and keeps
// nothing in memory.
func (s *CachedSource) Prefetch(ctx context.Context, object Object) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.Cache == nil || s.cached(ctx, object) {
		return false, nil
	}
	weight := downloadWeight(object)
	for !downloadBudget.TryAcquire(weight) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(prefetchPoll):
		}
	}
	defer downloadBudget.Release(weight)
	// A read may have fetched it in the meantime.
	if s.cached(ctx, object) {
		return false, nil
	}
	data, err := s.retrieve(ctx, object)
	if err != nil {
		return false, err
	}
	if err := s.Cache.StoreObject(ctx, object, data); err != nil {
		return false, err
	}
	return true, nil
}

// Manifest reads and parses a generation's manifest. The cache is trusted
// for file contents as for every other reader of it, but the manifest decides
// what the whole filesystem contains, so its bytes are checked against its
// name wherever they come from. Its size comes out of the download budget
// before any of it is held, and only the parsed manifest is kept.
func (s *CachedSource) Manifest(ctx context.Context, object Object) (*Manifest, error) {
	if object.Size == 0 || object.Size > MaxManifestSize {
		return nil, ErrObjectMismatch
	}
	weight := downloadWeight(object)
	if err := acquireBudget(ctx, downloadBudget, weight); err != nil {
		return nil, fmt.Errorf("wait to fetch mirror manifest %s: %w", object.Digest, err)
	}
	defer downloadBudget.Release(weight)

	if s.Cache != nil && s.cached(ctx, object) {
		stored := make([]byte, object.Size)
		if s.Cache.ReadObject(ctx, object, 0, stored) {
			if DigestOf(stored) != object.Digest {
				return nil, ErrObjectMismatch
			}
			return ParseBounded(stored)
		}
	}
	stored, err := s.retrieve(ctx, object)
	if err != nil {
		return nil, err
	}
	if s.Cache != nil {
		_ = s.Cache.StoreObject(ctx, object, stored)
	}
	return ParseBounded(stored)
}

// cached reports whether a read would find the object without downloading
// it.
func (s *CachedSource) cached(ctx context.Context, object Object) bool {
	s.mu.Lock()
	_, recent := s.recent[object.Digest]
	s.mu.Unlock()
	return recent || s.Cache.ReadObject(ctx, object, int64(object.Size)-1, make([]byte, 1))
}

// retrieve downloads an object and checks it, retrying failed attempts.
func (s *CachedSource) retrieve(ctx context.Context, object Object) ([]byte, error) {
	attempts := max(s.Attempts, 1)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		data, err := s.download(attemptCtx, object)
		cancel()
		if err == nil {
			return data, nil
		}
		lastErr = err
		if attempt < attempts {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
	}
	return nil, fmt.Errorf("fetch mirror object %s: %w", object.Digest, lastErr)
}

func (s *CachedSource) download(ctx context.Context, object Object) ([]byte, error) {
	body, err := s.Download(ctx, object.Digest)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	// One byte more than expected tells a longer object from a matching one.
	data := make([]byte, object.Size+1)
	n, err := io.ReadFull(body, data)
	if err != io.ErrUnexpectedEOF && err != io.EOF {
		if err == nil {
			err = ErrObjectMismatch
		}
		return nil, err
	}
	data = data[:n]
	if uint64(n) != object.Size || DigestOf(data) != object.Digest {
		return nil, ErrObjectMismatch
	}
	return data, nil
}

func (s *CachedSource) remember(digest Digest, data []byte) {
	budget := s.RecentBytes
	if budget <= 0 {
		budget = 128 << 20
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recent == nil {
		s.recent = map[Digest]*list.Element{}
	}
	if _, ok := s.recent[digest]; ok {
		return
	}
	s.recent[digest] = s.order.PushFront(&recentObject{digest: digest, data: data})
	s.held += len(data)
	for s.held > budget && s.order.Len() > 1 {
		oldest := s.order.Remove(s.order.Back()).(*recentObject)
		delete(s.recent, oldest.digest)
		s.held -= len(oldest.data)
	}
}
