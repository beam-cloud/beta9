package worker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var errStateVolumeSecurePathsUnavailable = errors.New("secure state-volume dirfd operations are unavailable")

type stateVolumeSecurePathKind uint8

const (
	stateVolumeSecureRegular stateVolumeSecurePathKind = iota + 1
	stateVolumeSecureDirectory
)

// stateVolumeSecurePathOps is the inode-pinned mutation boundary for shared
// state roots. Linux implements it with openat2(RESOLVE_BENEATH |
// RESOLVE_NO_SYMLINKS) and *at syscalls against held directory fds. The
// non-Linux implementation exists only so the Darwin unit suite can exercise
// manager logic; Probe deliberately fails there, so a production worker can
// never advertise state-volume capacity with pathname-only semantics.
type stateVolumeSecurePathOps interface {
	Probe(root string) error
	MkdirAll(path string, perm os.FileMode) error
	AtomicReplaceRegular(path string, data []byte, perm os.FileMode) error
	OpenRegular(path string) (*os.File, error)
	ReadDir(path string) ([]os.DirEntry, error)
	Rename(source, destination string, kind stateVolumeSecurePathKind, replace bool) error
	Remove(path string, kind stateVolumeSecurePathKind) error
	SyncDir(path string) error
}

func newStateVolumeSecurePathOps() stateVolumeSecurePathOps {
	return newPlatformStateVolumeSecurePathOps()
}

func validateStateVolumeSecureAbsolutePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" || !filepath.IsAbs(path) {
		return "", fmt.Errorf("secure state-volume path %q is not absolute", path)
	}
	clean := filepath.Clean(path)
	if clean == string(filepath.Separator) || clean != path {
		return "", fmt.Errorf("secure state-volume path %q is not a canonical child", path)
	}
	return clean, nil
}

func stateVolumeSecureTempName(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value[:]), nil
}
