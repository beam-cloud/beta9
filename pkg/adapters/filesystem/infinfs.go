package filesystem

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

/*

Implementation of: https://www.usenix.org/system/files/fast22-lv.pdf

Components:
  - metadata engine
  - caching layer
  - fuse
*/

type InfiniFileSystemOpts struct {
	Verbose bool
}

func Mount(opts FileSystemOpts) (func() error, <-chan error, error) {
	log.Printf("Mounting to %s\n", opts.MountPoint)

	if _, err := os.Stat(opts.MountPoint); os.IsNotExist(err) {
		err = os.MkdirAll(opts.MountPoint, 0755)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create mount point directory: %v", err)
		}

		log.Println("Mount point directory created.")
	}

	infiniFs, err := NewFileSystem(InfiniFileSystemOpts{Verbose: opts.Verbose})
	if err != nil {
		return nil, nil, fmt.Errorf("could not create filesystem: %v", err)
	}

	root, _ := infiniFs.Root()
	attrTimeout := time.Second * 60
	entryTimeout := time.Second * 60
	fsOptions := &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
	}
	server, err := fuse.NewServer(fs.NewNodeFS(root, fsOptions), opts.MountPoint, &fuse.MountOptions{
		MaxBackground:        512,
		DisableXAttrs:        true,
		EnableSymlinkCaching: true,
		SyncRead:             false,
		RememberInodes:       true,
		MaxReadAhead:         1 << 17,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("could not create server: %v", err)
	}

	serverError := make(chan error, 1)
	startServer := func() error {
		go func() {
			go server.Serve()

			if err := server.WaitMount(); err != nil {
				serverError <- err
				return
			}

			server.Wait()
			close(serverError)
		}()

		return nil
	}

	return startServer, serverError, nil
}

func NewFileSystem(opts InfiniFileSystemOpts) (*InfiniFileSystem, error) {
	cfs := &InfiniFileSystem{
		s:       s,
		verbose: opts.Verbose,
	}

	rootNode := s.Metadata.Get("/")
	if rootNode == nil {
		return nil, errors.New("missing root node")
	}

	cfs.root = &FSNode{
		filesystem: cfs,
		attr:       rootNode.Attr,
		ifsNode:    rootNode,
	}

	return cfs, nil
}

func (cfs *InfiniFileSystem) Root() (fs.InodeEmbedder, error) {
	if cfs.root == nil {
		return nil, fmt.Errorf("root not initialized")
	}
	return cfs.root, nil
}

type InfiniFileSystem struct {
	s       FileSystemStorage
	root    *FSNode
	verbose bool
}

/*************************************************************/
/* Metadata storage keys */
var (
	infinifsPrefix                   string = "infinifs:"
	infinifsDirectoryAccessMetadata  string = "infinifs:dam:%s:%s"
	infinifsDirectoryContentMetadata string = "infinifs:dcm:%s"
	infinifsFileMetadata             string = "infinifs:fm:%s:%s"
)

var InfinfsMetadataKeys = &infinfsMetadataKeys{}

type infinfsMetadataKeys struct{}

func (imk *infinfsMetadataKeys) InfinifsPrefix() string {
	return infinifsPrefix
}

func (imk *infinfsMetadataKeys) InfinifsDirectoryAccessMetadata(pid, name string) string {
	return fmt.Sprintf(infinifsDirectoryAccessMetadata, pid, name)
}

func (imk *infinfsMetadataKeys) InfinifsDirectoryContentMetadata(id string) string {
	return fmt.Sprintf(infinifsDirectoryContentMetadata, id)
}

func (imk *infinfsMetadataKeys) InfinifsFileMetadata(pid, name string) string {
	return fmt.Sprintf(infinifsDirectoryContentMetadata, pid, name)
}

type RedisMetadataEngine struct {
}

/*************************************************************/
/* FUSE interface */

type InfiniFsNodeType string

const (
	DirNode     InfiniFsNodeType = "dir"
	FileNode    InfiniFsNodeType = "file"
	SymLinkNode InfiniFsNodeType = "symlink"
)

type InfiniFsNode struct {
	NodeType    InfiniFsNodeType
	Path        string
	Attr        fuse.Attr
	Target      string
	ContentHash string
	DataPos     int64 // Position of the nodes data in the final binary
	DataLen     int64 // Length of the nodes data
}

// IsDir returns true if the InfiniFsNode represents a directory.
func (n *InfiniFsNode) IsDir() bool {
	return n.NodeType == DirNode
}

// IsSymlink returns true if the InfiniFsNode represents a symlink.
func (n *InfiniFsNode) IsSymlink() bool {
	return n.NodeType == SymLinkNode
}

type FSNode struct {
	fs.Inode
	filesystem *InfiniFileSystem
	ifsNode    *InfiniFsNode
	attr       fuse.Attr
}

func (n *FSNode) log(format string, v ...interface{}) {
	if n.filesystem.verbose {
		log.Printf(fmt.Sprintf("[INFINFS] (%s) %s", n.ifsNode.Path, format), v...)
	}
}

func (n *FSNode) OnAdd(ctx context.Context) {
	n.log("OnAdd called")
}

func (n *FSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.log("Getattr called")

	node := n.ifsNode

	// Fill in the AttrOut struct
	out.Ino = node.Attr.Ino
	out.Size = node.Attr.Size
	out.Blocks = node.Attr.Blocks
	out.Atime = node.Attr.Atime
	out.Mtime = node.Attr.Mtime
	out.Ctime = node.Attr.Ctime
	out.Mode = node.Attr.Mode
	out.Nlink = node.Attr.Nlink
	out.Owner = node.Attr.Owner

	return fs.OK
}

func (n *FSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.log("Lookup called with name: %s", name)

	// Create the full path of the child node
	childPath := path.Join(n.ifsNode.Path, name)

	// Lookup the child node
	child := n.filesystem.s.Metadata.Get(childPath)
	if child == nil {
		// No child with the requested name exists
		return nil, syscall.ENOENT
	}

	// Fill out the child node's attributes
	out.Attr = child.Attr

	// Create a new Inode for the child
	childInode := n.NewInode(ctx, &FSNode{filesystem: n.filesystem, ifsNode: child, attr: child.Attr}, fs.StableAttr{Mode: child.Attr.Mode, Ino: child.Attr.Ino})

	return childInode, fs.OK
}

func (n *FSNode) Opendir(ctx context.Context) syscall.Errno {
	n.log("Opendir called")
	return 0
}

func (n *FSNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.log("Open called with flags: %v", flags)
	return nil, 0, fs.OK
}

// func (n *FSNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
// 	n.log("Read called with offset: %v", off)

// 	// Don't even try to read 0 byte files
// 	if n.ifsNode.DataLen == 0 {
// 		nRead := 0
// 		return fuse.ReadResultData(dest[:nRead]), fs.OK
// 	}

// 	nRead, err := n.filesystem.s.ReadFile(n.ifsNode, dest, off)
// 	if err != nil {
// 		return nil, syscall.EIO
// 	}

// 	return fuse.ReadResultData(dest[:nRead]), fs.OK
// }

func (n *FSNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	n.log("Readlink called")

	if n.ifsNode.NodeType != SymLinkNode {
		// This node is not a symlink
		return nil, syscall.EINVAL
	}

	// Use the symlink target path directly
	symlinkTarget := n.ifsNode.Target

	// In this case, we don't need to read the file
	return []byte(symlinkTarget), fs.OK
}

func (n *FSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	n.log("Readdir called")

	dirEntries := n.filesystem.s.Metadata.ListDirectory(n.ifsNode.Path)
	return fs.NewListDirStream(dirEntries), fs.OK
}

func (n *FSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	n.log("Create called with name: %s, flags: %v, mode: %v", name, flags, mode)
	return nil, nil, 0, syscall.EROFS
}

func (n *FSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.log("Mkdir called with name: %s, mode: %v", name, mode)
	return nil, syscall.EROFS
}

func (n *FSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	n.log("Rmdir called with name: %s", name)
	return syscall.EROFS
}

func (n *FSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	n.log("Unlink called with name: %s", name)
	return syscall.EROFS
}

func (n *FSNode) Rename(ctx context.Context, oldName string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	n.log("Rename called with oldName: %s, newName: %s, flags: %v", oldName, newName, flags)
	return syscall.EROFS
}
