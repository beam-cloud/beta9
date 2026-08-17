//go:build linux

package worker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type linuxStateVolumeSecurePathOps struct {
	// Test seam for the held leaf fd rejection path. Parent/root dirfds still
	// use unix.Close directly; OpenRegular must close a rejected leaf exactly
	// once so descriptor reuse cannot close an unrelated QMP/NBD fd.
	closeLeafFD func(int) error
}

func newPlatformStateVolumeSecurePathOps() stateVolumeSecurePathOps {
	return linuxStateVolumeSecurePathOps{}
}

const stateVolumeOpenat2Resolve = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS

func stateVolumeSecurePathParts(path string) ([]string, error) {
	clean, err := validateStateVolumeSecureAbsolutePath(path)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimPrefix(clean, string(filepath.Separator)), string(filepath.Separator))
	if len(parts) == 0 {
		return nil, fmt.Errorf("secure state-volume path has no child components")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("secure state-volume path contains invalid component %q", part)
		}
	}
	return parts, nil
}

func stateVolumeSecureOpenDir(path string, create bool, perm os.FileMode, readable bool) (int, error) {
	parts, err := stateVolumeSecurePathParts(path)
	if err != nil {
		return -1, err
	}
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for index, part := range parts {
		flags := uint64(unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW)
		if readable && index == len(parts)-1 {
			flags = uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW)
		}
		next, openErr := unix.Openat2(current, part, &unix.OpenHow{Flags: flags, Resolve: stateVolumeOpenat2Resolve})
		if errors.Is(openErr, unix.ENOENT) && create {
			if mkdirErr := unix.Mkdirat(current, part, uint32(perm.Perm())); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				unix.Close(current)
				return -1, mkdirErr
			}
			next, openErr = unix.Openat2(current, part, &unix.OpenHow{Flags: flags, Resolve: stateVolumeOpenat2Resolve})
		}
		unix.Close(current)
		if openErr != nil {
			return -1, openErr
		}
		current = next
	}
	return current, nil
}

func stateVolumeSecureOpenParent(path string, create bool, perm os.FileMode) (int, string, error) {
	clean, err := validateStateVolumeSecureAbsolutePath(path)
	if err != nil {
		return -1, "", err
	}
	base := filepath.Base(clean)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return -1, "", fmt.Errorf("invalid secure state-volume basename %q", base)
	}
	parent, err := stateVolumeSecureOpenDir(filepath.Dir(clean), create, perm, false)
	return parent, base, err
}

func stateVolumeSecureStatAt(parent int, name string, kind stateVolumeSecurePathKind) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(parent, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	mode := stat.Mode & unix.S_IFMT
	want := uint32(unix.S_IFREG)
	if kind == stateVolumeSecureDirectory {
		want = unix.S_IFDIR
	}
	if mode != want {
		return fmt.Errorf("secure state-volume entry %q has mode %#o, want %#o", name, mode, want)
	}
	return nil
}

func stateVolumeSecureStatFD(fd int, kind stateVolumeSecurePathKind) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return stat, err
	}
	mode := stat.Mode & unix.S_IFMT
	want := uint32(unix.S_IFREG)
	if kind == stateVolumeSecureDirectory {
		want = unix.S_IFDIR
	}
	if mode != want {
		return stat, fmt.Errorf("secure state-volume fd has mode %#o, want %#o", mode, want)
	}
	return stat, nil
}

func stateVolumeSecureOpenEntry(parent int, name string, kind stateVolumeSecurePathKind) (int, unix.Stat_t, error) {
	fd, err := unix.Openat2(parent, name, &unix.OpenHow{
		Flags: uint64(unix.O_PATH | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: stateVolumeOpenat2Resolve,
	})
	if err != nil {
		return -1, unix.Stat_t{}, err
	}
	stat, err := stateVolumeSecureStatFD(fd, kind)
	if err != nil {
		unix.Close(fd)
		return -1, unix.Stat_t{}, err
	}
	return fd, stat, nil
}

func stateVolumeSecureSameInode(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino
}

func (linuxStateVolumeSecurePathOps) Probe(root string) error {
	fd, err := stateVolumeSecureOpenDir(root, false, 0, false)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
			return fmt.Errorf("%w: openat2: %v", errStateVolumeSecurePathsUnavailable, err)
		}
		return err
	}
	return unix.Close(fd)
}

func (linuxStateVolumeSecurePathOps) MkdirAll(path string, perm os.FileMode) error {
	fd, err := stateVolumeSecureOpenDir(path, true, perm, false)
	if err != nil {
		return err
	}
	return unix.Close(fd)
}

func (linuxStateVolumeSecurePathOps) AtomicReplaceRegular(path string, data []byte, perm os.FileMode) error {
	parent, base, err := stateVolumeSecureOpenParent(path, true, 0700)
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	tempName, err := stateVolumeSecureTempName(".state-volume-tmp-")
	if err != nil {
		return err
	}
	fd, err := unix.Openat(parent, tempName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(perm.Perm()))
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), tempName)
	cleanup := true
	defer func() {
		_ = file.Close()
		if cleanup {
			_ = unix.Unlinkat(parent, tempName, 0)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	tempStat, err := stateVolumeSecureStatFD(fd, stateVolumeSecureRegular)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := unix.Renameat2(parent, tempName, parent, base, unix.RENAME_NOREPLACE); errors.Is(err, unix.EEXIST) {
		// Preserve the old inode under tempName while atomically installing the
		// new one. This avoids a check/replace pathname window and lets us verify
		// the published inode before deleting the prior regular journal.
		if err := stateVolumeSecureStatAt(parent, base, stateVolumeSecureRegular); err != nil {
			return err
		}
		if err := unix.Renameat2(parent, tempName, parent, base, unix.RENAME_EXCHANGE); err != nil {
			return err
		}
		publishedFD, publishedStat, verifyErr := stateVolumeSecureOpenEntry(parent, base, stateVolumeSecureRegular)
		if verifyErr == nil {
			unix.Close(publishedFD)
		}
		if verifyErr != nil || !stateVolumeSecureSameInode(tempStat, publishedStat) {
			_ = unix.Renameat2(parent, tempName, parent, base, unix.RENAME_EXCHANGE)
			if verifyErr != nil {
				return verifyErr
			}
			return fmt.Errorf("secure state-volume atomic publication installed the wrong inode")
		}
		if err := unix.Unlinkat(parent, tempName, 0); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	cleanup = false
	return unix.Fsync(parent)
}

func (o linuxStateVolumeSecurePathOps) OpenRegular(path string) (*os.File, error) {
	parent, base, err := stateVolumeSecureOpenParent(path, false, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parent)
	fd, err := unix.Openat2(parent, base, &unix.OpenHow{
		// O_NONBLOCK prevents a forged FIFO/device from blocking before the
		// held descriptor itself is authenticated with fstat.
		Flags: uint64(unix.O_RDONLY | unix.O_NONBLOCK | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: stateVolumeOpenat2Resolve,
	})
	if err != nil {
		return nil, err
	}
	if _, err := stateVolumeSecureStatFD(fd, stateVolumeSecureRegular); err != nil {
		if o.closeLeafFD != nil {
			_ = o.closeLeafFD(fd)
		} else {
			_ = unix.Close(fd)
		}
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func (linuxStateVolumeSecurePathOps) ReadDir(path string) ([]os.DirEntry, error) {
	fd, err := stateVolumeSecureOpenDir(path, false, 0, true)
	if err != nil {
		return nil, err
	}
	dir := os.NewFile(uintptr(fd), path)
	defer dir.Close()
	return dir.ReadDir(-1)
}

func (linuxStateVolumeSecurePathOps) Rename(source, destination string, kind stateVolumeSecurePathKind, replace bool) error {
	sourceParent, sourceBase, err := stateVolumeSecureOpenParent(source, false, 0)
	if err != nil {
		return err
	}
	defer unix.Close(sourceParent)
	sourceFD, sourceStat, err := stateVolumeSecureOpenEntry(sourceParent, sourceBase, kind)
	if err != nil {
		return err
	}
	defer unix.Close(sourceFD)
	destinationParent, destinationBase, err := stateVolumeSecureOpenParent(destination, true, 0700)
	if err != nil {
		return err
	}
	defer unix.Close(destinationParent)
	if err := stateVolumeSecureStatAt(destinationParent, destinationBase, kind); err == nil {
		if !replace {
			return os.ErrExist
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return err
	}
	if err := unix.Renameat(sourceParent, sourceBase, destinationParent, destinationBase); err != nil {
		return err
	}
	destinationFD, destinationStat, verifyErr := stateVolumeSecureOpenEntry(destinationParent, destinationBase, kind)
	if verifyErr == nil {
		unix.Close(destinationFD)
	}
	if verifyErr != nil || !stateVolumeSecureSameInode(sourceStat, destinationStat) {
		// The source basename changed after it was pinned. Restore the moved
		// entry when possible and fail closed; never claim the requested inode
		// was quarantined/retired.
		_ = unix.Renameat(destinationParent, destinationBase, sourceParent, sourceBase)
		if verifyErr != nil {
			return fmt.Errorf("verify secure state-volume rename destination: %w", verifyErr)
		}
		return fmt.Errorf("secure state-volume rename source changed during mutation")
	}
	if err := unix.Fsync(sourceParent); err != nil {
		return err
	}
	return unix.Fsync(destinationParent)
}

func (linuxStateVolumeSecurePathOps) Remove(path string, kind stateVolumeSecurePathKind) error {
	parent, base, err := stateVolumeSecureOpenParent(path, false, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(parent)
	sourceFD, sourceStat, err := stateVolumeSecureOpenEntry(parent, base, kind)
	if errors.Is(err, unix.ENOENT) {
		return nil
	} else if err != nil {
		return err
	}
	defer unix.Close(sourceFD)
	trash, err := stateVolumeSecureTempName(".state-volume-remove-")
	if err != nil {
		return err
	}
	if err := unix.Renameat(parent, base, parent, trash); err != nil {
		return err
	}
	trashFD, trashStat, verifyErr := stateVolumeSecureOpenEntry(parent, trash, kind)
	if verifyErr == nil {
		unix.Close(trashFD)
	}
	if verifyErr != nil || !stateVolumeSecureSameInode(sourceStat, trashStat) {
		_ = unix.Renameat(parent, trash, parent, base)
		if verifyErr != nil {
			return fmt.Errorf("verify secure state-volume removal target: %w", verifyErr)
		}
		return fmt.Errorf("secure state-volume removal source changed during mutation")
	}
	flags := 0
	if kind == stateVolumeSecureDirectory {
		flags = unix.AT_REMOVEDIR
	}
	if err := unix.Unlinkat(parent, trash, flags); err != nil {
		_ = unix.Renameat(parent, trash, parent, base)
		return err
	}
	return unix.Fsync(parent)
}

func (linuxStateVolumeSecurePathOps) SyncDir(path string) error {
	fd, err := stateVolumeSecureOpenDir(path, false, 0, true)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Fsync(fd)
}
