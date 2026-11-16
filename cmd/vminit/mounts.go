//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
)

// Run only disjoint bind destinations together. Tmpfs, ancestors, aliases and
// uncertain paths remain ordered, including resolving paths again after an
// earlier mount could have changed their meaning.
func applyGuestMounts(root string, mounts []microvm.Mount, limit int, resolve func(string, string) (string, error), apply func(int, microvm.Mount) error) error {
	limit = min(max(limit, 1), 16)
	type pending struct {
		index          int
		raw, canonical string
	}
	var batch []pending
	flush := func() error {
		errs := make([]error, len(batch))
		var wg sync.WaitGroup
		for i, item := range batch {
			wg.Add(1)
			go func() { defer wg.Done(); errs[i] = apply(item.index, mounts[item.index]) }()
		}
		wg.Wait()
		batch = nil
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}
	for i := 0; i < len(mounts); {
		mount := mounts[i]
		raw := filepath.Clean(mount.Destination)
		conflict := mount.Type == microvm.MountTmpfs
		for _, old := range batch {
			conflict = conflict || mountPathsOverlap(raw, old.raw)
		}
		var canonical string
		var resolveErr error
		if !conflict && limit > 1 {
			canonical, resolveErr = resolve(root, raw)
		}
		if conflict || limit == 1 || resolveErr != nil {
			if err := flush(); err != nil {
				return err
			}
			if err := apply(i, mount); err != nil {
				return err
			}
			i++
			continue
		}
		for _, old := range batch {
			conflict = conflict || mountPathsOverlap(canonical, old.canonical)
		}
		if conflict {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		batch = append(batch, pending{i, raw, canonical})
		i++
		if len(batch) == limit {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func mountPathsOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, strings.TrimSuffix(b, "/")+"/") || strings.HasPrefix(b, strings.TrimSuffix(a, "/")+"/")
}

func canonicalMountTarget(root, destination string) (string, error) {
	path := filepath.Join(root, destination)
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(path)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			if !mountPathsOverlap(root, resolved) || resolved != root && !strings.HasPrefix(resolved, root+"/") {
				return "", fmt.Errorf("mount target resolves outside new root")
			}
			return resolved, nil
		}
		if !os.IsNotExist(err) || filepath.Dir(path) == path {
			return "", err
		}
		if info, statErr := os.Lstat(path); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			// A dangling link is an unresolved alias, not an absent name.
			return "", err
		}
		suffix = append(suffix, filepath.Base(path))
		path = filepath.Dir(path)
	}
}
