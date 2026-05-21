package cache

import (
	"context"
	"fmt"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type CacheFSNode struct {
	Path     string
	ID       string
	PID      string
	Name     string
	Target   string
	Hash     string
	Attr     fuse.Attr
	Prefetch *bool
}
type FSNode struct {
	fs.Inode
	filesystem *CacheFS
	bfsNode    *CacheFSNode
	attr       fuse.Attr
}

func (n *FSNode) log(format string, v ...interface{}) {
	if n.filesystem.verbose {
		Logger.Infof(fmt.Sprintf("(%s) %s", n.bfsNode.Path, format), v...)
	}
}

func (n *FSNode) OnAdd(ctx context.Context) {
	n.log("OnAdd called")
}

func (n *FSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.log("Getattr called")

	node := n.bfsNode

	out.Ino = node.Attr.Ino
	out.Size = node.Attr.Size
	out.Blocks = node.Attr.Blocks
	out.Atime = node.Attr.Atime
	out.Mtime = node.Attr.Mtime
	out.Ctime = node.Attr.Ctime
	out.Mode = node.Attr.Mode
	out.Nlink = node.Attr.Nlink
	out.Owner = node.Attr.Owner
	out.Atimensec = node.Attr.Atimensec
	out.Mtimensec = node.Attr.Mtimensec
	out.Ctimensec = node.Attr.Ctimensec

	return fs.OK
}

func metaToAttr(metadata *FSMetadata) fuse.Attr {
	return fuse.Attr{
		Ino:       metadata.Ino,
		Size:      metadata.Size,
		Blocks:    metadata.Blocks,
		Atime:     metadata.Atime,
		Mtime:     metadata.Mtime,
		Ctime:     metadata.Ctime,
		Atimensec: metadata.Atimensec,
		Mtimensec: metadata.Mtimensec,
		Ctimensec: metadata.Ctimensec,
		Mode:      metadata.Mode,
		Nlink:     metadata.Nlink,
		Owner: fuse.Owner{
			Uid: metadata.Uid,
			Gid: metadata.Gid,
		},
		Rdev:    metadata.Rdev,
		Blksize: metadata.Blksize,
		Padding: metadata.Padding,
	}
}

func (n *FSNode) inodeFromFsId(ctx context.Context, fsId string) (*fs.Inode, *fuse.Attr, error) {
	metadata, err := n.filesystem.Registry.GetFsNode(ctx, fsId)
	if err != nil {
		return nil, nil, syscall.ENOENT
	}

	// Fill out the child node's attributes
	attr := metaToAttr(metadata)

	// Create a new Inode on lookup
	node := n.NewInode(ctx,
		&FSNode{filesystem: n.filesystem, bfsNode: &CacheFSNode{
			Path:     metadata.Path,
			ID:       metadata.ID,
			PID:      metadata.PID,
			Name:     metadata.Name,
			Hash:     metadata.Hash,
			Attr:     attr,
			Target:   "",
			Prefetch: nil,
		}, attr: attr},
		fs.StableAttr{Mode: metadata.Mode, Ino: metadata.Ino, Gen: metadata.Gen},
	)

	return node, &attr, nil
}

func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fullPath := path.Join(n.bfsNode.Path, name) // Construct the full of this file path from root
	n.log("Lookup called with path: %s", fullPath)

	// Force caching of a specific full path if the path contains a special illegal character '%'
	// This is a hack to trigger caching from external callers without going through the GRPC service directly
	if strings.Contains(fullPath, "%") {
		sourcePath := strings.ReplaceAll(fullPath, "%", "/")

		n.log("Storing content from source with path: %s", sourcePath)

		cacheSource := struct {
			Path string
		}{
			Path: sourcePath,
		}
		_, err := n.filesystem.Client.StoreContentFromFUSE(cacheSource, struct {
			RoutingKey string
			Lock       bool
		}{
			RoutingKey: sourcePath,
			Lock:       true,
		})
		if err != nil {
			return nil, syscall.ENOENT
		}

		node, attr, err := n.inodeFromFsId(ctx, GenerateFsID(sourcePath))
		if err != nil {
			return nil, syscall.ENOENT
		}

		out.Attr = *attr
		return node, fs.OK
	}

	node, attr, err := n.inodeFromFsId(ctx, GenerateFsID(fullPath))
	if err != nil {
		return nil, syscall.ENOENT
	}

	out.Attr = *attr
	return node, fs.OK
}

func (n *FSNode) Opendir(ctx context.Context) syscall.Errno {
	n.log("Opendir called")
	return 0
}

func (n *FSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.log("Open called with flags: %v", flags)

	// Enable DirectIO if specified
	if n.filesystem.Config.CacheFS.DirectIO {
		fuseFlags |= fuse.FOPEN_DIRECT_IO
		fuseFlags &= ^uint32(fuse.FOPEN_KEEP_CACHE)
		return nil, fuseFlags, fs.OK
	}

	return nil, 0, fs.OK
}

func (n *FSNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n.log("Read called with offset: %v, length: %v", off, len(dest))

	// Don't try to read 0 byte files
	if n.bfsNode.Attr.Size == 0 {
		return fuse.ReadResultData(dest[:0]), fs.OK
	}

	sourcePath := n.bfsNode.Path

	buffer, err := n.filesystem.Client.GetContent(n.bfsNode.Hash, off, int64(len(dest)), struct {
		RoutingKey string
	}{
		RoutingKey: sourcePath,
	})
	if err != nil {
		if err == ErrContentNotFound {

			cacheSource := struct {
				Path string
			}{
				Path: sourcePath,
			}
			_, err = n.filesystem.Client.StoreContentFromFUSE(cacheSource, struct {
				RoutingKey string
				Lock       bool
			}{
				RoutingKey: sourcePath,
				Lock:       true,
			})
			// If multiple clients try to store the same file, some may get ErrUnableToAcquireLock
			// In this case, we should tell the client to retry the Read instead of returning an error
			if err != nil && err == ErrUnableToAcquireLock {
				return nil, syscall.EAGAIN
			} else if err != nil {
				return nil, syscall.EIO
			}

			buffer, err = n.filesystem.Client.GetContent(n.bfsNode.Hash, off, int64(len(dest)), struct {
				RoutingKey string
			}{
				RoutingKey: sourcePath,
			})
			if err != nil {
				return nil, syscall.EIO
			}

			return fuse.ReadResultData(buffer), fs.OK
		}

		return nil, syscall.EIO
	}

	return fuse.ReadResultData(buffer), fs.OK
}

func (n *FSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	n.log("Readlink called")

	if n.bfsNode.Target == "" {
		return nil, syscall.EINVAL
	}

	// In this case, we don't need to read the file
	return []byte(n.bfsNode.Target), fs.OK
}

func (n *FSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	n.log("Readdir called")

	children, err := n.filesystem.Registry.GetFsNodeChildren(ctx, GenerateFsID(n.bfsNode.Path))
	if err != nil {
		return nil, fs.ENOATTR
	}

	dirEntries := []fuse.DirEntry{}
	for _, child := range children {
		dirEntries = append(dirEntries, fuse.DirEntry{
			Mode: child.Mode,
			Name: child.Name,
			Ino:  child.Ino,
		})
	}

	return fs.NewListDirStream(dirEntries), fs.OK
}

func (n *FSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.log("Create called with name: %s, flags: %v, mode: %v", name, flags, mode)

	inode, errno = n.createChildNode(ctx, name, mode, out)
	return inode, nil, 0, errno
}

func (n *FSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.log("Mkdir called with name: %s, mode: %v", name, mode)

	return n.createChildNode(ctx, name, fuse.S_IFDIR|mode, out)
}

func (n *FSNode) createChildNode(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	fullPath := path.Join(n.bfsNode.Path, name)
	newFsId := GenerateFsID(fullPath)
	ino, err := SHA1StringToUint64(newFsId)
	if err != nil {
		return nil, syscall.EIO
	}

	now := time.Now()
	nowSec := uint64(now.Unix())
	nowNsec := uint32(now.Nanosecond())
	metadata := &FSMetadata{
		PID:       n.bfsNode.ID,
		ID:        newFsId,
		Name:      name,
		Path:      fullPath,
		Ino:       ino,
		Mode:      mode,
		Atime:     nowSec,
		Mtime:     nowSec,
		Ctime:     nowSec,
		Atimensec: nowNsec,
		Mtimensec: nowNsec,
		Ctimensec: nowNsec,
		Size:      0,
		Hash:      "",
	}

	if err := n.filesystem.Registry.SetFsNode(ctx, newFsId, metadata); err != nil {
		return nil, syscall.EIO
	}

	if err := n.filesystem.Registry.AddFsNodeChild(ctx, n.bfsNode.ID, newFsId); err != nil {
		_ = n.filesystem.Registry.RemoveFsNode(ctx, newFsId)
		return nil, syscall.EIO
	}

	attr := metaToAttr(metadata)
	inode := n.NewInode(ctx, &FSNode{filesystem: n.filesystem, bfsNode: &CacheFSNode{
		Path: fullPath,
		ID:   newFsId,
		PID:  n.bfsNode.ID,
		Name: name,
		Attr: attr,
	}, attr: attr}, fs.StableAttr{Mode: mode, Ino: metadata.Ino, Gen: metadata.Gen})

	out.Attr = attr

	return inode, fs.OK
}

func (n *FSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	n.log("Rmdir called with name: %s", name)

	// Construct the full path for the directory to be removed
	fullPath := path.Join(n.bfsNode.Path, name)

	// Generate the FsID for the directory
	fsId := GenerateFsID(fullPath)

	// Check if the directory is empty
	children, err := n.filesystem.Registry.GetFsNodeChildren(ctx, fsId)
	if err != nil {
		return syscall.EIO
	}
	if len(children) > 0 {
		return syscall.ENOTEMPTY
	}

	// Remove the directory from the coordinator
	err = n.filesystem.Registry.RemoveFsNode(ctx, fsId)
	if err != nil {
		return syscall.EIO
	}

	// Remove the directory from the parent's children
	err = n.filesystem.Registry.RemoveFsNodeChild(ctx, n.bfsNode.ID, fsId)
	if err != nil {
		return syscall.EIO
	}

	return fs.OK
}

func (n *FSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	n.log("Unlink called with name: %s", name)

	// Construct the full path for the file to be deleted
	fullPath := path.Join(n.bfsNode.Path, name)

	// Generate the FsID for the file
	fsId := GenerateFsID(fullPath)

	// Remove the file from the coordinator
	err := n.filesystem.Registry.RemoveFsNode(ctx, fsId)
	if err != nil {
		return syscall.EIO
	}

	// Remove the file from the parent's children
	err = n.filesystem.Registry.RemoveFsNodeChild(ctx, n.bfsNode.ID, fsId)
	if err != nil {
		return syscall.EIO
	}

	return fs.OK
}

func (n *FSNode) Rename(ctx context.Context, oldName string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	n.log("Rename called with oldName: %s, newName: %s, flags: %v", oldName, newName, flags)

	targetParent, ok := newParent.(*FSNode)
	if !ok || targetParent == nil {
		return syscall.EIO
	}

	oldFullPath := path.Join(n.bfsNode.Path, oldName)
	newFullPath := path.Join(targetParent.bfsNode.Path, newName)

	oldFsId := GenerateFsID(oldFullPath)
	newFsId := GenerateFsID(newFullPath)
	if oldFsId == newFsId {
		return fs.OK
	}

	metadata, err := n.filesystem.Registry.GetFsNode(ctx, oldFsId)
	if err != nil {
		return syscall.ENOENT
	}

	if _, err := n.filesystem.Registry.GetFsNode(ctx, newFsId); err == nil {
		if err := n.filesystem.Registry.RemoveFsNode(ctx, newFsId); err != nil {
			return syscall.EIO
		}
		if err := n.filesystem.Registry.RemoveFsNodeChild(ctx, targetParent.bfsNode.ID, newFsId); err != nil {
			return syscall.EIO
		}
	}

	metadata.ID = newFsId
	metadata.PID = targetParent.bfsNode.ID
	metadata.Name = newName
	metadata.Path = newFullPath

	if err := n.filesystem.Registry.SetFsNode(ctx, newFsId, metadata); err != nil {
		return syscall.EIO
	}

	if err := n.filesystem.Registry.AddFsNodeChild(ctx, targetParent.bfsNode.ID, newFsId); err != nil {
		return syscall.EIO
	}

	if err := n.filesystem.Registry.RemoveFsNodeChild(ctx, n.bfsNode.ID, oldFsId); err != nil {
		return syscall.EIO
	}

	if err := n.filesystem.Registry.RemoveFsNode(ctx, oldFsId); err != nil {
		return syscall.EIO
	}

	return fs.OK
}
