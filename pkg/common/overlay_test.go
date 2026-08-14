package common

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestContainerOverlayCleanupLayersPreservesBundleState(t *testing.T) {
	overlayPath := t.TempDir()
	containerID := "container-reset"
	bundlePath := filepath.Join(overlayPath, containerID)
	signalPath := filepath.Join(bundlePath, "criu", "CONTAINER_ID")

	require.NoError(t, os.MkdirAll(filepath.Dir(signalPath), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(bundlePath, "config.json"), []byte("{}"), 0644))
	require.NoError(t, os.WriteFile(signalPath, []byte(containerID), 0644))

	overlay := &ContainerOverlay{containerId: containerID, overlayPath: overlayPath}
	require.NoError(t, overlay.cleanupLayers())
	require.FileExists(t, filepath.Join(bundlePath, "config.json"))
	require.FileExists(t, signalPath)

	require.NoError(t, overlay.Cleanup())
	require.NoDirExists(t, bundlePath)
}

func TestContainerOverlayResetWithUpper(t *testing.T) {
	t.Run("seeds fresh upper before mount", func(t *testing.T) {
		fakeBin := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\nexit 0\n"), 0755))
		t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

		overlayPath := t.TempDir()
		containerID := "container-seeded-reset"
		layerDir := filepath.Join(overlayPath, containerID, "layer-0")
		require.NoError(t, os.MkdirAll(filepath.Join(layerDir, "upper"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(layerDir, "upper", "stale"), []byte("stale"), 0644))
		overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: containerID}, t.TempDir(), overlayPath)

		var seedPath string
		var seededDevice, seededXattr bool
		err := overlay.ResetWithUpper(func(upperPath string) error {
			seedPath = upperPath
			require.NoFileExists(t, filepath.Join(upperPath, "stale"))
			whiteoutPath := filepath.Join(upperPath, ".wh.deleted")
			if err := exec.Command("mknod", whiteoutPath, "c", "0", "0").Run(); err == nil {
				seededDevice = true
			} else {
				require.NoError(t, os.WriteFile(whiteoutPath, []byte("whiteout"), 0600))
			}
			opaquePath := filepath.Join(upperPath, "opaque")
			require.NoError(t, os.Mkdir(opaquePath, 0755))
			seededXattr = unix.Setxattr(opaquePath, "user.beta9.test", []byte("opaque"), 0) == nil
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, filepath.Join(layerDir, "upper"), seedPath)
		whiteoutInfo, err := os.Lstat(filepath.Join(seedPath, ".wh.deleted"))
		require.NoError(t, err)
		if seededDevice {
			require.NotZero(t, whiteoutInfo.Mode()&os.ModeDevice)
		}
		if seededXattr {
			value := make([]byte, len("opaque"))
			size, err := unix.Getxattr(filepath.Join(seedPath, "opaque"), "user.beta9.test", value)
			require.NoError(t, err)
			require.Equal(t, "opaque", string(value[:size]))
		}
		require.NoFileExists(t, filepath.Join(layerDir, "merged", ".wh.deleted"))
		require.Equal(t, filepath.Join(layerDir, "merged"), overlay.TopLayerPath())
	})

	t.Run("removes partial upper after seed failure", func(t *testing.T) {
		overlayPath := t.TempDir()
		containerID := "container-seed-failure"
		overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: containerID}, t.TempDir(), overlayPath)
		seedErr := errors.New("seed failed")

		err := overlay.ResetWithUpper(func(upperPath string) error {
			require.NoError(t, os.WriteFile(filepath.Join(upperPath, "partial"), []byte("partial"), 0644))
			return seedErr
		})

		require.ErrorIs(t, err, seedErr)
		require.NoDirExists(t, filepath.Join(overlayPath, containerID, "layer-0"))
	})

	t.Run("removes partial upper after mount failure", func(t *testing.T) {
		fakeBin := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte("#!/bin/sh\nexit 1\n"), 0755))
		t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))

		overlayPath := t.TempDir()
		containerID := "container-mount-failure"
		overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: containerID}, t.TempDir(), overlayPath)

		err := overlay.ResetWithUpper(func(upperPath string) error {
			return os.WriteFile(filepath.Join(upperPath, "seeded"), []byte("seeded"), 0644)
		})

		require.Error(t, err)
		require.NoDirExists(t, filepath.Join(overlayPath, containerID, "layer-0"))
	})
}
