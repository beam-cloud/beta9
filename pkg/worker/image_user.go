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
		path, err := securejoin.SecureJoinVFS(".", name, root)
		if err != nil {
			return fmt.Errorf("resolve image %s: %w", name, err)
		}
		file, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("open image %s: %w", name, err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("image %s is not a regular file", name)
		}
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
