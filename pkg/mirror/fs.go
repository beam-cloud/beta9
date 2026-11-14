package mirror

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
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

// DefaultPlacements are the homes of the two users a VM is used as: root
// over SSH and beam on the desktop.
func DefaultPlacements() []Placement {
	return []Placement{
		{Path: "/root", UID: 0, GID: 0, Mode: 0o700},
		{Path: "/home/beam", UID: 1000, GID: 1000, Mode: 0o750},
	}
}

type FSOptions struct {
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
	decoded map[uint32]*list.Element
	lru     *list.List
	held    int
	budget  int
	starts  map[uint32][]uint64 // file -> raw offset of each extent
}

type decodedExtent struct {
	extent uint32
	data   []byte
}

func NewFS(manifest *Manifest, source Source, opts FSOptions) (*FS, error) {
	placements := opts.Placements
	if len(placements) == 0 {
		placements = DefaultPlacements()
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

// Revoke stops the filesystem from returning file contents, for a mirror
// that has been purged while mounted. Names and attributes stay, so that the
// overlay above keeps working for everything the guest wrote itself. Pages
// the kernel or the guest already cached are beyond its reach.
func (f *FS) Revoke() {
	f.revoked.Store(true)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.decoded, f.held = map[uint32]*list.Element{}, 0
	f.lru.Init()
}

func (f *FS) Revoked() bool { return f.revoked.Load() }

// Manifest is the generation the filesystem serves.
func (f *FS) Manifest() *Manifest { return f.manifest }

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
		stored := make([]byte, extent.StoredLen)
		object := f.manifest.Object(extent.Object)
		if err := f.source.ReadObject(context.WithoutCancel(ctx), object, int64(extent.Offset), stored); err != nil {
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
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		return nil, err
	}
	return server, nil
}
