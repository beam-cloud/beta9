package common

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// fakeMount installs a mount(8) stand-in that appends its arguments to log
// and exits with the code returned by the given shell snippet.
func fakeMount(t *testing.T, body string) (logPath string) {
	t.Helper()
	fakeBin := t.TempDir()
	logPath = filepath.Join(fakeBin, "mount.log")
	script := "#!/bin/sh\necho \"$@\" >> " + logPath + "\n" + body + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(fakeBin, "mount"), []byte(script), 0755))
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logPath
}

func mountCalls(t *testing.T, logPath string) []string {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func TestContainerOverlayEphemeralLayersMountVolatile(t *testing.T) {
	overlayVolatileUnsupported.Store(false)
	t.Cleanup(func() { overlayVolatileUnsupported.Store(false) })

	t.Run("ephemeral layer uses volatile, persistent layer does not", func(t *testing.T) {
		logPath := fakeMount(t, "exit 0")

		ephemeral := NewContainerOverlay(&types.ContainerRequest{ContainerId: "ephemeral"}, t.TempDir(), t.TempDir())
		require.NoError(t, ephemeral.Setup())
		disk := t.TempDir()
		persistent := NewContainerOverlay(&types.ContainerRequest{ContainerId: "persistent"}, t.TempDir(), t.TempDir())
		require.NoError(t, persistent.SetupWithWritable(filepath.Join(disk, "upper"), filepath.Join(disk, "work")))

		calls := mountCalls(t, logPath)
		require.Len(t, calls, 2)
		require.Contains(t, calls[0], ",volatile")
		require.NotContains(t, calls[1], "volatile")
	})

	t.Run("kernel without volatile support falls back once and remembers", func(t *testing.T) {
		overlayVolatileUnsupported.Store(false)
		logPath := fakeMount(t, `case "$*" in *volatile*) echo "mount: wrong fs type, bad option, bad superblock on overlay" >&2; exit 32;; esac; exit 0`)

		overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: "fallback"}, t.TempDir(), t.TempDir())
		require.NoError(t, overlay.Setup())
		require.True(t, overlayVolatileUnsupported.Load())
		require.DirExists(t, filepath.Join(overlay.OverlayPath(), "fallback", "layer-0", "work"))

		second := NewContainerOverlay(&types.ContainerRequest{ContainerId: "fallback-2"}, t.TempDir(), t.TempDir())
		require.NoError(t, second.Setup())

		calls := mountCalls(t, logPath)
		require.Len(t, calls, 3, "volatile attempt, synced retry, then synced only")
		require.Contains(t, calls[0], ",volatile")
		require.NotContains(t, calls[1], "volatile")
		require.NotContains(t, calls[2], "volatile")
	})

	t.Run("a real mount failure is not retried without volatile", func(t *testing.T) {
		overlayVolatileUnsupported.Store(false)
		logPath := fakeMount(t, `echo "mount: /merged: mount point does not exist." >&2; exit 32`)

		overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: "hard-failure"}, t.TempDir(), t.TempDir())
		err := overlay.Setup()
		require.Error(t, err)
		require.Contains(t, err.Error(), "mount point does not exist")
		require.False(t, overlayVolatileUnsupported.Load())
		require.Len(t, mountCalls(t, logPath), 1)
	})
}

func TestContainerOverlayReadOnlyLayers(t *testing.T) {
	overlayVolatileUnsupported.Store(false)
	t.Cleanup(func() { overlayVolatileUnsupported.Store(false) })

	t.Run("sit above the image in every base layer", func(t *testing.T) {
		logPath := fakeMount(t, "exit 0")
		image, top, middle := t.TempDir(), t.TempDir(), t.TempDir()

		ephemeral := NewContainerOverlay(&types.ContainerRequest{ContainerId: "ephemeral"}, image, t.TempDir())
		require.NoError(t, ephemeral.SetReadOnlyLayers(top, middle))
		require.NoError(t, ephemeral.Setup())
		require.Error(t, ephemeral.SetReadOnlyLayers(top), "mounted overlays cannot change layers")
		// A second writable layer stacks on the first, which already has them.
		require.NoError(t, ephemeral.AddEmptyLayer())
		require.Equal(t, image, ephemeral.RootPath())

		disk := t.TempDir()
		persistent := NewContainerOverlay(&types.ContainerRequest{ContainerId: "persistent"}, image, t.TempDir())
		require.NoError(t, persistent.SetReadOnlyLayers(top))
		require.NoError(t, persistent.SetupWithWritable(filepath.Join(disk, "upper"), filepath.Join(disk, "work")))

		calls := mountCalls(t, logPath)
		require.Len(t, calls, 3)
		require.Contains(t, calls[0], "lowerdir="+top+":"+middle+":"+image+",upperdir=")
		require.Contains(t, calls[1], "lowerdir="+filepath.Join(ephemeral.OverlayPath(), "ephemeral", "layer-0", "merged")+",upperdir=")
		require.Contains(t, calls[2], "lowerdir="+top+":"+image+",upperdir=")
	})

	t.Run("reject paths that would change the mount options", func(t *testing.T) {
		for _, dir := range []string{"relative", "/a:/b", "/a,ro", "/a/../b", `/a\040b`} {
			overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: "invalid"}, t.TempDir(), t.TempDir())
			require.Error(t, overlay.SetReadOnlyLayers(dir), dir)
		}
	})

	t.Run("kernel overlay merges them under the writable layer", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("mounting an overlay needs root")
		}
		// t.TempDir paths can be on an overlay themselves, which cannot
		// hold an upper directory; a tmpfs can.
		base := t.TempDir()
		require.NoError(t, unix.Mount("tmpfs", base, "tmpfs", 0, ""))
		t.Cleanup(func() { _ = unix.Unmount(base, unix.MNT_DETACH) })
		image, layer := filepath.Join(base, "image"), filepath.Join(base, "layer")
		for path, content := range map[string]string{
			filepath.Join(image, "etc", "os-release"):   "image",
			filepath.Join(image, "root", ".bashrc"):     "image bashrc",
			filepath.Join(image, "root", "image-only"):  "image",
			filepath.Join(layer, "root", ".bashrc"):     "layer bashrc",
			filepath.Join(layer, "root", "notes", "a"):  "layer",
			filepath.Join(layer, "home", "beam", "doc"): "layer",
		} {
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
			require.NoError(t, os.WriteFile(path, []byte(content), 0644))
		}

		overlay := NewContainerOverlay(&types.ContainerRequest{ContainerId: "merged"}, image, filepath.Join(base, "overlay"))
		require.NoError(t, overlay.SetReadOnlyLayers(layer))
		require.NoError(t, overlay.Setup())
		t.Cleanup(func() { _ = overlay.Cleanup() })
		merged := overlay.TopLayerPath()
		read := func(root, name string) string {
			data, err := os.ReadFile(filepath.Join(root, name))
			require.NoError(t, err)
			return string(data)
		}
		require.Equal(t, "image", read(merged, "etc/os-release"))
		require.Equal(t, "layer bashrc", read(merged, "root/.bashrc"), "the layer wins over the image")
		require.Equal(t, "image", read(merged, "root/image-only"), "directories merge")
		require.Equal(t, "layer", read(merged, "root/notes/a"))
		require.Equal(t, "layer", read(merged, "home/beam/doc"))

		// Writes and deletes land in the writable layer only.
		require.NoError(t, os.WriteFile(filepath.Join(merged, "root", ".bashrc"), []byte("edited"), 0644))
		require.NoError(t, os.Remove(filepath.Join(merged, "root", "notes", "a")))
		require.Equal(t, "edited", read(merged, "root/.bashrc"))
		require.NoFileExists(t, filepath.Join(merged, "root", "notes", "a"))
		require.Equal(t, "layer bashrc", read(layer, "root/.bashrc"))
		require.Equal(t, "layer", read(layer, "root/notes/a"))
		require.Equal(t, "image bashrc", read(image, "root/.bashrc"))
	})
}
