package cache

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	mirrorFsName = "mirrorFs"
)

func NewMirrorFs(localDir string, syncInterval time.Duration) *MirrorFs {
	return &MirrorFs{
		localDir:     localDir,
		syncInterval: syncInterval,
		virtualFiles: make(map[string]*FileCacheMetadata),
		pendingSync:  make(map[string]time.Time),
	}
}

type MirrorFs struct {
	srv          *fuse.Server
	localDir     string
	syncInterval time.Duration

	// Virtual files from cloud metadata
	virtualFiles map[string]*FileCacheMetadata
	virtualMu    sync.RWMutex

	// Files pending sync to cloud
	pendingSync map[string]time.Time
	pendingMu   sync.RWMutex
}

func (s *MirrorFs) Start(mountpoint string) error {
	if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
		os.MkdirAll(mountpoint, 0755)
	}

	// Ensure local directory exists
	if _, err := os.Stat(s.localDir); os.IsNotExist(err) {
		os.MkdirAll(s.localDir, 0755)
	}

	root := &mirrorRoot{fs: s}
	opts := &fusefs.Options{
		MountOptions: fuse.MountOptions{
			Name:         mirrorFsName,
			FsName:       mirrorFsName,
			MaxReadAhead: 1024 * 1024 * 128,
		},
	}

	raw := fusefs.NewNodeFS(root, opts)
	srv, err := fuse.NewServer(raw, mountpoint, &opts.MountOptions)
	if err != nil {
		return err
	}
	s.srv = srv

	// Start sync goroutine
	go s.syncLoop()

	go srv.Serve()
	return srv.WaitMount()
}

func (s *MirrorFs) Stop() error {
	if s.srv == nil {
		return nil
	}
	err := s.srv.Unmount()
	s.srv = nil
	return err
}

// Add virtual file from cloud metadata
func (s *MirrorFs) AddVirtualFile(path string, metadata *FileCacheMetadata) {
	s.virtualMu.Lock()
	defer s.virtualMu.Unlock()
	s.virtualFiles[path] = metadata
}

// Mark file for sync after modification
func (s *MirrorFs) markForSync(path string) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pendingSync[path] = time.Now().Add(s.syncInterval)
}

// Sync loop - periodically sync pending files to cloud
func (s *MirrorFs) syncLoop() {
	ticker := time.NewTicker(s.syncInterval / 2)
	defer ticker.Stop()

	for range ticker.C {
		s.pendingMu.Lock()
		now := time.Now()
		for path, syncTime := range s.pendingSync {
			if now.After(syncTime) {
				// TODO: Implement actual cloud sync
				// For now, just remove from pending
				delete(s.pendingSync, path)
			}
		}
		s.pendingMu.Unlock()
	}
}

type mirrorRoot struct {
	fusefs.Inode
	fs *MirrorFs
}

func (r *mirrorRoot) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	var stat syscall.Stat_t
	if err := syscall.Stat(r.fs.localDir, &stat); err == nil {
		out.FromStat(&stat)
		return 0
	}
	out.Mode = fuse.S_IFDIR | 0755
	return 0
}

func (r *mirrorRoot) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{}

	// Add local files
	if files, err := os.ReadDir(r.fs.localDir); err == nil {
		for _, file := range files {
			var mode uint32 = fuse.S_IFREG
			if file.IsDir() {
				mode = fuse.S_IFDIR
			}
			entries = append(entries, fuse.DirEntry{
				Name: file.Name(),
				Mode: mode,
			})
		}
	}

	// Add virtual files that don't exist locally
	r.fs.virtualMu.RLock()
	for path := range r.fs.virtualFiles {
		// Only show files in root directory for simplicity
		if !strings.Contains(path, "/") {
			localPath := filepath.Join(r.fs.localDir, path)
			if _, err := os.Stat(localPath); os.IsNotExist(err) {
				entries = append(entries, fuse.DirEntry{
					Name: path,
					Mode: fuse.S_IFREG,
				})
			}
		}
	}
	r.fs.virtualMu.RUnlock()

	return fusefs.NewListDirStream(entries), 0
}

func (r *mirrorRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	localPath := filepath.Join(r.fs.localDir, name)

	// Check if file exists locally
	var stat syscall.Stat_t
	if err := syscall.Stat(localPath, &stat); err == nil {
		var child fusefs.InodeEmbedder
		if stat.Mode&syscall.S_IFDIR != 0 {
			child = &mirrorDir{fs: r.fs, localPath: localPath}
		} else {
			child = &mirrorFile{fs: r.fs, localPath: localPath, isVirtual: false}
		}

		stable := fusefs.StableAttr{}
		if stat.Mode&syscall.S_IFDIR != 0 {
			stable.Mode = fuse.S_IFDIR
		} else {
			stable.Mode = fuse.S_IFREG
		}

		out.FromStat(&stat)
		return r.NewInode(ctx, child, stable), 0
	}

	// Check if it's a virtual file
	r.fs.virtualMu.RLock()
	metadata, exists := r.fs.virtualFiles[name]
	r.fs.virtualMu.RUnlock()

	if exists {
		child := &mirrorFile{
			fs:        r.fs,
			localPath: localPath,
			isVirtual: true,
			metadata:  metadata,
		}

		stable := fusefs.StableAttr{Mode: fuse.S_IFREG}
		out.Size = uint64(metadata.Size)
		out.Mode = fuse.S_IFREG | 0644
		t := uint64(metadata.LastModified.Unix())
		out.Mtime, out.Ctime, out.Atime = t, t, t

		return r.NewInode(ctx, child, stable), 0
	}

	return nil, syscall.ENOENT
}

type mirrorDir struct {
	fusefs.Inode
	fs        *MirrorFs
	localPath string
}

func (d *mirrorDir) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	var stat syscall.Stat_t
	if err := syscall.Stat(d.localPath, &stat); err == nil {
		out.FromStat(&stat)
		return 0
	}
	out.Mode = fuse.S_IFDIR | 0755
	return 0
}

type mirrorFile struct {
	fusefs.Inode
	fs        *MirrorFs
	localPath string
	isVirtual bool
	metadata  *FileCacheMetadata
}

func (f *mirrorFile) Open(ctx context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	// Convert virtual file to local file on first write
	if f.isVirtual && (flags&syscall.O_WRONLY != 0 || flags&syscall.O_RDWR != 0) {
		if err := f.materializeFile(); err != nil {
			return nil, 0, syscall.EIO
		}
		f.isVirtual = false
	}

	file, err := os.OpenFile(f.localPath, int(flags), 0644)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	return &mirrorFileHandle{file: file, mirrorFile: f}, 0, 0
}

func (f *mirrorFile) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if !f.isVirtual {
		var stat syscall.Stat_t
		if err := syscall.Stat(f.localPath, &stat); err == nil {
			out.FromStat(&stat)
			return 0
		}
	}

	// Virtual file attributes
	if f.metadata != nil {
		out.Mode = fuse.S_IFREG | 0644
		out.Size = uint64(f.metadata.Size)
		t := uint64(f.metadata.LastModified.Unix())
		out.Mtime, out.Ctime, out.Atime = t, t, t
	}

	return 0
}

// Download virtual file content to local path
func (f *mirrorFile) materializeFile() error {
	// TODO: Implement actual download from cloud storage
	// For now, create empty file
	return os.WriteFile(f.localPath, []byte{}, 0644)
}

type mirrorFileHandle struct {
	file       *os.File
	mirrorFile *mirrorFile
}

func (fh *mirrorFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if fh.mirrorFile.isVirtual {
		// TODO: Implement virtual file reading from cloud
		return fuse.ReadResultData([]byte{}), 0
	}

	n, err := fh.file.ReadAt(dest, off)
	if err != nil && err != os.ErrClosed {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (fh *mirrorFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := fh.file.WriteAt(data, off)
	if err != nil {
		return 0, syscall.EIO
	}

	// Mark file for sync to cloud
	relPath, _ := filepath.Rel(fh.mirrorFile.fs.localDir, fh.mirrorFile.localPath)
	fh.mirrorFile.fs.markForSync(relPath)

	return uint32(n), 0
}

func (fh *mirrorFileHandle) Release(ctx context.Context) syscall.Errno {
	fh.file.Close()
	return 0
}

func (fh *mirrorFileHandle) Flush(ctx context.Context) syscall.Errno {
	if err := fh.file.Sync(); err != nil {
		return syscall.EIO
	}
	return 0
}
