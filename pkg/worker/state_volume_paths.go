package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

func canonicalStateVolumePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("state volume path is empty")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("resolve absolute state volume path %q: %w", path, err)
	}

	probe := abs
	var suffix []string
	for {
		resolved, evalErr := filepath.EvalSymlinks(probe)
		if evalErr == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		if !os.IsNotExist(evalErr) {
			return "", fmt.Errorf("resolve state volume path %q: %w", path, evalErr)
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", fmt.Errorf("resolve existing ancestor of state volume path %q", path)
		}
		suffix = append(suffix, filepath.Base(probe))
		probe = parent
	}
}

func stateVolumePathsOverlap(a, b string) bool {
	rel, err := filepath.Rel(a, b)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return true
	}
	rel, err = filepath.Rel(b, a)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func validateStateVolumePathPair(backingDir, mountPath string) error {
	backing, err := canonicalStateVolumePath(backingDir)
	if err != nil {
		return err
	}
	mount, err := canonicalStateVolumePath(mountPath)
	if err != nil {
		return err
	}
	if stateVolumePathsOverlap(backing, mount) {
		return fmt.Errorf("state volume backing path %q and mount path %q overlap", backing, mount)
	}
	return nil
}

func validateStateVolumeGroupPaths(specs []StateVolumeSpec) error {
	type namedPath struct {
		label string
		path  string
	}
	paths := make([]namedPath, 0, len(specs)*2)
	ids := make(map[string]struct{}, len(specs))
	names := make(map[string]struct{}, len(specs))
	containerMounts := make(map[string]struct{}, len(specs))
	roots := 0
	for _, spec := range specs {
		if strings.TrimSpace(spec.ID) == "" || strings.TrimSpace(spec.Name) == "" {
			return fmt.Errorf("state volume ID and name are required")
		}
		if _, exists := ids[spec.ID]; exists {
			return fmt.Errorf("duplicate state volume ID %q", spec.ID)
		}
		ids[spec.ID] = struct{}{}
		if _, exists := names[spec.Name]; exists {
			return fmt.Errorf("duplicate state volume name %q", spec.Name)
		}
		names[spec.Name] = struct{}{}
		if !filepath.IsAbs(spec.ContainerMountPath) || filepath.Clean(spec.ContainerMountPath) != spec.ContainerMountPath {
			return fmt.Errorf("state volume %q container mount path must be canonical and absolute", spec.ID)
		}
		if _, exists := containerMounts[spec.ContainerMountPath]; exists {
			return fmt.Errorf("duplicate state volume container mount path %q", spec.ContainerMountPath)
		}
		containerMounts[spec.ContainerMountPath] = struct{}{}
		if spec.Root {
			roots++
			if spec.Name != "root" || spec.ContainerMountPath != "/" || spec.ReadOnly {
				return fmt.Errorf("root state volume must be named root, mounted at /, and writable")
			}
		} else if spec.Name == "root" || spec.ContainerMountPath == "/" {
			return fmt.Errorf("root state volume name and mount path are reserved")
		}
		backing, err := canonicalStateVolumePath(spec.BackingDir)
		if err != nil {
			return fmt.Errorf("volume %q backing path: %w", spec.ID, err)
		}
		mount, err := canonicalStateVolumePath(spec.MountPath)
		if err != nil {
			return fmt.Errorf("volume %q mount path: %w", spec.ID, err)
		}
		paths = append(paths,
			namedPath{label: fmt.Sprintf("volume %q backing", spec.ID), path: backing},
			namedPath{label: fmt.Sprintf("volume %q mount", spec.ID), path: mount},
		)
	}
	if roots > 1 {
		return fmt.Errorf("state volume group contains %d root volumes", roots)
	}
	for i := range paths {
		for j := i + 1; j < len(paths); j++ {
			if stateVolumePathsOverlap(paths[i].path, paths[j].path) {
				return fmt.Errorf("%s path %q overlaps %s path %q", paths[i].label, paths[i].path, paths[j].label, paths[j].path)
			}
		}
	}
	return nil
}

func stateVolumeToken(prefix, value string) string {
	digest := sha256.Sum256([]byte(value))
	return prefix + hex.EncodeToString(digest[:8])
}

func stateVolumeGenerationID(containerID, volumeID, operationID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(containerID+"\x00"+volumeID+"\x00"+operationID)).String()
}

func freshStateVolumeID(containerID, role string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("beta9-state-volume\x00"+containerID+"\x00"+role)).String()
}
