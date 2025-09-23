package cache

import (
	"context"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type SimpleFUSE struct {
	srv *fuse.Server
}

func (s *SimpleFUSE) Start(mountpoint string) error {
	root := &root{}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:   "beta9cache",
			FsName: "beta9cache",
		},
	}
	raw := fs.NewNodeFS(root, opts)
	srv, err := fuse.NewServer(raw, mountpoint, &opts.MountOptions)
	if err != nil {
		return err
	}
	s.srv = srv
	go srv.Serve()
	return srv.WaitMount()
}

func (s *SimpleFUSE) Stop() error {
	if s.srv == nil {
		return nil
	}
	err := s.srv.Unmount()
	s.srv = nil
	return err
}

type root struct{ fs.Inode }

func (r *root) OnAdd(ctx context.Context) {
	f := &helloFile{data: []byte("hello\n")}
	ch := r.NewPersistentInode(ctx, f, fs.StableAttr{Mode: fuse.S_IFREG})
	r.AddChild("hello.txt", ch, true)
}

func (r *root) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | 0555
	return 0
}

// Example file... delete later
type helloFile struct {
	fs.Inode
	data []byte
}

func (f *helloFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return f, 0, 0
}

func (f *helloFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(f.data)) {
		return fuse.ReadResultData(nil), 0
	}
	n := copy(dest, f.data[off:])
	return fuse.ReadResultData(dest[:n]), 0
}

func (f *helloFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFREG | 0444
	out.Size = uint64(len(f.data))
	t := uint64(time.Now().Unix())
	out.Mtime, out.Ctime, out.Atime = t, t, t
	return 0
}
