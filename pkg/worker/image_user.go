package worker

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/moby/sys/user"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// Resolve Docker's USER against the container filesystem, never the worker's accounts.
func resolveImageUser(rootfs string, identity *specs.User) error {
	if identity.Username == "" {
		return nil
	}
	root, err := os.OpenRoot(rootfs)
	if err != nil {
		return err
	}
	defer root.Close()

	var accounts [2]io.Reader
	for i, name := range []string{"etc/passwd", "etc/group"} {
		file, err := openImageFile(root, name)
		if err != nil {
			return err
		}
		if file == nil {
			continue
		}
		defer file.Close()
		accounts[i] = file
	}

	defaults := &user.ExecUser{Uid: int(identity.UID), Gid: int(identity.GID)}
	for _, gid := range identity.AdditionalGids {
		defaults.Sgids = append(defaults.Sgids, int(gid))
	}
	resolved, err := user.GetExecUser(identity.Username, defaults, accounts[0], accounts[1])
	if err != nil {
		return fmt.Errorf("image USER %q: %w", identity.Username, err)
	}
	identity.UID, identity.GID = uint32(resolved.Uid), uint32(resolved.Gid)
	identity.AdditionalGids = nil
	for _, gid := range resolved.Sgids {
		identity.AdditionalGids = append(identity.AdditionalGids, uint32(gid))
	}
	identity.Username = ""
	return nil
}

// openImageFile opens a regular file of an image by its path inside the
// image, following the image's symlinks no further than its root. It returns
// nil for a file the image does not have.
func openImageFile(root *os.Root, name string) (*os.File, error) {
	path, err := securejoin.SecureJoinVFS(".", name, root)
	if err != nil {
		return nil, fmt.Errorf("resolve image %s: %w", name, err)
	}
	file, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open image %s: %w", name, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("image %s is not a regular file", name)
	}
	return file, nil
}
