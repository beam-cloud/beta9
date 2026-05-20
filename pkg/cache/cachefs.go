package cache

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/moby/sys/mountinfo"
)

type StorageLayer interface {
}

// Generates a directory ID based on parent ID and name.
func GenerateFsID(name string) string {
	hash := sha256.Sum256([]byte(name))
	return hex.EncodeToString(hash[:])
}

// SHA1StringToUint64 converts the first 8 bytes of a SHA-1 hash string to a uint64
func SHA1StringToUint64(hash string) (uint64, error) {
	bytes, err := hex.DecodeString(hash[:16]) // first 8 bytes (16 hex characters)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(bytes), nil
}

type FSSystemOpts struct {
	Verbose  bool
	Registry Registry
	Config   ClientConfig
	Client   *Client
}

type CacheFS struct {
	ctx      context.Context
	root     *FSNode
	verbose  bool
	Registry Registry
	Client   *Client
	Config   ClientConfig
}

func Mount(ctx context.Context, opts FSSystemOpts) (func() error, <-chan error, *fuse.Server, error) {
	mountPoint := opts.Config.CacheFS.MountPoint
	Logger.Infof("Mounting to %s", mountPoint)

	if _, err := os.Stat(mountPoint); os.IsNotExist(err) {
		err = os.MkdirAll(mountPoint, 0755)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create mount point directory: %v", err)
		}

		Logger.Info("Mount point directory created.")
	} else if isFuseMount(mountPoint) {
		if err := forceUnmount(mountPoint); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to unmount existing FUSE mount: %v", err)
		}
	}

	cachefs, err := NewFileSystem(ctx, opts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not create filesystem: %v", err)
	}

	root, _ := cachefs.Root()
	attrTimeout := time.Second * 5
	entryTimeout := time.Second * 5
	negativeTimeout := time.Second * 2 // Cache negative lookups to reduce FUSE chatter
	fsOptions := &fs.Options{
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negativeTimeout,
	}

	maxWriteKB := opts.Config.CacheFS.MaxWriteKB
	if maxWriteKB <= 0 {
		maxWriteKB = 1024
	}

	maxReadAheadKB := opts.Config.CacheFS.MaxReadAheadKB
	if maxReadAheadKB <= 0 {
		maxReadAheadKB = 128
	}

	maxBackgroundTasks := opts.Config.CacheFS.MaxBackgroundTasks
	if maxBackgroundTasks <= 0 {
		maxBackgroundTasks = 512
	}

	// Note: CongestionThreshold would be set to 75% of MaxBackground if supported
	// This is a recommended optimization but may not be available in all go-fuse versions

	options := []string{}
	options = append(options, opts.Config.CacheFS.Options...)

	server, err := fuse.NewServer(fs.NewNodeFS(root, fsOptions), mountPoint, &fuse.MountOptions{
		MaxBackground:        maxBackgroundTasks,
		DisableXAttrs:        true,
		EnableSymlinkCaching: true,
		SyncRead:             false,
		RememberInodes:       true,
		MaxReadAhead:         maxReadAheadKB * 1024,
		MaxWrite:             maxWriteKB * 1024,
		Options:              options,
		DirectMount:          opts.Config.CacheFS.DirectMount,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not create server: %v", err)
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

	return startServer, serverError, server, nil
}

func updateReadAheadKB(mountPoint string, valueKB int) error {
	mounts, err := mountinfo.GetMounts(nil)
	if err != nil {
		return fmt.Errorf("failed to get mount info: %w", err)
	}

	var deviceID string
	for _, mount := range mounts {
		if mount.Mountpoint == mountPoint {
			deviceID = fmt.Sprintf("%d:%d", mount.Major, mount.Minor)
			break
		}
	}

	if deviceID == "" {
		return fmt.Errorf("mount point %s not found", mountPoint)
	}

	// Construct path to read_ahead_kb
	readAheadPath := fmt.Sprintf("/sys/class/bdi/%s/read_ahead_kb", deviceID)

	// Update read_ahead_kb
	cmd := exec.Command("sh", "-c", fmt.Sprintf("echo %d > %s", valueKB, readAheadPath))
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to update read_ahead_kb: %w read_ahead_path: %s", err, readAheadPath)
	}

	return nil
}

// NewFileSystem initializes a new CacheFS with root metadata.
func NewFileSystem(ctx context.Context, opts FSSystemOpts) (*CacheFS, error) {
	coordinatorClient := opts.Registry

	bfs := &CacheFS{
		ctx:      ctx,
		verbose:  opts.Verbose,
		Config:   opts.Config,
		Client:   opts.Client,
		Registry: opts.Registry,
	}

	rootID := GenerateFsID("/")
	rootPID := "" // Root node has no parent
	rootPath := "/"

	dirMeta, err := coordinatorClient.GetFsNode(bfs.ctx, rootID)
	if err != nil || dirMeta == nil {
		Logger.Infof("Root node metadata not found, creating it now...")

		dirMeta = &FSMetadata{PID: rootPID, ID: rootID, Path: rootPath, Ino: 1, Mode: fuse.S_IFDIR | 0755}

		err := coordinatorClient.SetFsNode(bfs.ctx, rootID, dirMeta)
		if err != nil {
			Logger.Errorf("Unable to create cachefs root node dir metdata: %+v", err)
			return nil, err
		}
	}

	// Create the actual root filesystem node required by FUSE
	attr := fuse.Attr{
		Ino:  1,
		Mode: dirMeta.Mode,
	}

	rootNode := &FSNode{
		filesystem: bfs,
		attr:       attr,

		bfsNode: &CacheFSNode{
			Path: dirMeta.Path,
			ID:   dirMeta.ID,
			PID:  dirMeta.PID,
			Attr: attr,
		},
	}

	bfs.root = rootNode
	return bfs, nil
}

func (bfs *CacheFS) Root() (fs.InodeEmbedder, error) {
	if bfs.root == nil {
		return nil, fmt.Errorf("root not initialized")
	}
	return bfs.root, nil
}

func isFuseMount(mountPoint string) bool {
	cmd := exec.Command("findmnt", "-n", "-o", "FSTYPE", mountPoint)
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(output), "fuse")
}

func forceUnmount(mountPoint string) error {
	cmd := exec.Command("fusermount", "-uz", mountPoint)
	if _, err := cmd.CombinedOutput(); err != nil {
		return err
	}
	return nil
}
