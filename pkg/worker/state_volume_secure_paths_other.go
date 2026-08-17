//go:build !linux

package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// pathnameStateVolumeSecurePathOps is deliberately unavailable to production.
// It lets the Darwin unit suite exercise higher-level state machines without
// pretending point-in-time lstat checks are a safe shared-host mutation seam.
type pathnameStateVolumeSecurePathOps struct{ mu sync.Mutex }

func newPlatformStateVolumeSecurePathOps() stateVolumeSecurePathOps {
	return &pathnameStateVolumeSecurePathOps{}
}

func (*pathnameStateVolumeSecurePathOps) Probe(string) error {
	return errStateVolumeSecurePathsUnavailable
}

func stateVolumePathnameNoSymlink(path string, allowMissing bool) error {
	// Darwin's /var is itself a system symlink to /private/var, so walking
	// every ancestor with lstat would reject every t.TempDir fixture. This
	// fallback is test-only (Probe always fails): resolve the existing ancestry
	// for basic containment and let each operation lstat/reject its leaf. Linux
	// production never uses this pathname seam.
	_, err := canonicalStateVolumePath(path)
	if os.IsNotExist(err) && allowMissing {
		return nil
	}
	return err
}

func (p *pathnameStateVolumeSecurePathOps) MkdirAll(path string, perm os.FileMode) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := stateVolumePathnameNoSymlink(path, true); err != nil {
		return err
	}
	return os.MkdirAll(path, perm)
}

func (p *pathnameStateVolumeSecurePathOps) AtomicReplaceRegular(path string, data []byte, perm os.FileMode) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := stateVolumePathnameNoSymlink(filepath.Dir(path), true); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".state-volume-tmp-")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tempPath, perm); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return fmt.Errorf("refuse to replace non-regular secure state-volume entry %q", path)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return syncStateVolumeDirectory(filepath.Dir(path))
}

func (p *pathnameStateVolumeSecurePathOps) OpenRegular(path string) (*os.File, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := stateVolumePathnameNoSymlink(path, false); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("secure state-volume entry %q is not regular", path)
	}
	return os.Open(path)
}

func (p *pathnameStateVolumeSecurePathOps) ReadDir(path string) ([]os.DirEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := stateVolumePathnameNoSymlink(path, false); err != nil {
		return nil, err
	}
	return os.ReadDir(path)
}

func (p *pathnameStateVolumeSecurePathOps) Rename(source, destination string, kind stateVolumeSecurePathKind, replace bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := stateVolumePathnameNoSymlink(source, false); err != nil {
		return err
	}
	if err := stateVolumePathnameNoSymlink(filepath.Dir(destination), true); err != nil {
		return err
	}
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if kind == stateVolumeSecureRegular && !info.Mode().IsRegular() || kind == stateVolumeSecureDirectory && !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("secure state-volume source %q has unexpected type", source)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); err == nil && !replace {
		return os.ErrExist
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(source, destination)
}

func (p *pathnameStateVolumeSecurePathOps) Remove(path string, kind stateVolumeSecurePathKind) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := stateVolumePathnameNoSymlink(path, false); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if kind == stateVolumeSecureRegular && !info.Mode().IsRegular() || kind == stateVolumeSecureDirectory && !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("secure state-volume entry %q has unexpected type", path)
	}
	return os.Remove(path)
}

func (p *pathnameStateVolumeSecurePathOps) SyncDir(path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := stateVolumePathnameNoSymlink(path, false); err != nil {
		return err
	}
	return syncStateVolumeDirectory(path)
}
