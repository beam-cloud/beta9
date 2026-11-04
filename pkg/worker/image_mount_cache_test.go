package worker

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func idleImageClient(t *testing.T, ids ...string) *ImageClient {
	t.Helper()
	c := &ImageClient{imageMountPath: t.TempDir(), mountedFuseServers: common.NewSafeMap[*fuse.Server]()}
	for _, id := range ids {
		require.NoError(t, os.Mkdir(c.imageMountPoint(id), 0755))
		c.mountedFuseServers.Set(id, &fuse.Server{})
		c.mountLastUsed.Store(id, time.Now().Add(-8*24*time.Hour))
	}
	return c
}

func TestIdleImageMountExpiryKeepsActiveRecentAndUnknownMounts(t *testing.T) {
	c := idleImageClient(t, "expired", "active", "recent", "unknown")
	c.mountLastUsed.Store("recent", time.Now())
	c.mountLastUsed.Delete("unknown")
	var unmounted []string
	n := c.pruneIdleMounts(time.Now().Add(-7*24*time.Hour), func(id string) bool { return id == "active" }, func(id string, _ *fuse.Server) error {
		unmounted = append(unmounted, id)
		return nil
	})
	require.Equal(t, 1, n)
	require.Equal(t, []string{"expired"}, unmounted)
	require.False(t, c.mountedImageReady("expired"))
	_, err := os.Stat(c.imageMountPoint("expired"))
	require.True(t, os.IsNotExist(err))
	for _, id := range []string{"active", "recent", "unknown"} {
		require.True(t, c.mountedImageReady(id))
	}
}

func TestIdleImageMountExpiryNeverForcesBusyUnmount(t *testing.T) {
	c := idleImageClient(t, "busy")
	n := c.pruneIdleMounts(time.Now().Add(-7*24*time.Hour), func(string) bool { return false }, func(string, *fuse.Server) error { return syscall.EBUSY })
	require.Zero(t, n)
	require.True(t, c.mountedImageReady("busy"))
	_, err := os.Stat(c.imageMountPoint("busy"))
	require.NoError(t, err)
}

func TestIdleImageMountGraceStartsWhenContainerReleasesImage(t *testing.T) {
	c := idleImageClient(t, "long-running")
	w := &Worker{imageClient: c, containerInstances: common.NewSafeMap[*ContainerInstance]()}
	w.containerInstances.Set("container", &ContainerInstance{Request: &types.ContainerRequest{ImageId: "long-running"}})
	w.deleteContainer("container")
	inUse := func(string) bool { return false }
	unmount := func(string, *fuse.Server) error { return nil }
	require.Zero(t, c.pruneIdleMounts(time.Now().Add(-7*24*time.Hour), inUse, unmount), "a recently released image needs a full idle grace period")
	require.True(t, c.mountedImageReady("long-running"))
	require.Equal(t, 1, c.pruneIdleMounts(time.Now().Add(time.Minute), inUse, unmount), "an inactive image must remain eligible for eventual expiry")
}

func TestIdleImageMountExpiryFencesConcurrentHotImageLoad(t *testing.T) {
	c := idleImageClient(t, "image")
	entered, release, pruned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(pruned)
		c.pruneIdleMounts(time.Now().Add(-7*24*time.Hour), func(string) bool { return false }, func(string, *fuse.Server) error {
			close(entered)
			<-release
			return syscall.EBUSY
		})
	}()
	<-entered
	loaded := make(chan error, 1)
	go func() {
		_, err := c.PullLazy(context.Background(), &types.ContainerRequest{ImageId: "image"})
		loaded <- err
	}()
	select {
	case <-loaded:
		t.Fatal("hot image load raced the unmount fence")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	<-pruned
	require.NoError(t, <-loaded)
	used, ok := c.mountLastUsed.Load("image")
	require.True(t, ok)
	require.WithinDuration(t, time.Now(), used.(time.Time), time.Second)
	require.Zero(t, c.pruneIdleMounts(time.Now().Add(-7*24*time.Hour), func(string) bool { return false }, func(string, *fuse.Server) error {
		t.Fatal("freshly used mount was expired")
		return nil
	}))
}

func TestIdleImageMountExpiryDoesNotRecursivelyRemoveFiles(t *testing.T) {
	c := idleImageClient(t, "image")
	file := filepath.Join(c.imageMountPoint("image"), "keep")
	require.NoError(t, os.WriteFile(file, []byte("preserve"), 0600))
	c.pruneIdleMounts(time.Now().Add(-7*24*time.Hour), func(string) bool { return false }, func(string, *fuse.Server) error { return nil })
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(data))
	require.False(t, c.mountedImageReady("image"), "a completed unmount cannot remain a cache hit")
}

func TestIdleImageMountExpiryRealFUSE(t *testing.T) {
	if os.Getenv("BETA9_IMAGE_PRUNE_REAL_FUSE") != "1" {
		t.Skip("set BETA9_IMAGE_PRUNE_REAL_FUSE=1 in an isolated privileged Linux container")
	}
	data := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(data, "payload"), []byte("preserve active reader"), 0644))
	c := idleImageClient(t, "image")
	root, err := fs.NewLoopbackRoot(data)
	require.NoError(t, err)
	server, err := fs.Mount(c.imageMountPoint("image"), root, &fs.Options{MountOptions: fuse.MountOptions{DirectMount: true, FsName: "beta9-idle-image-test"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Unmount() })
	require.NoError(t, server.WaitMount())
	c.mountedFuseServers.Set("image", server)
	reader, err := os.Open(filepath.Join(c.imageMountPoint("image"), "payload"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	unmount := func(_ string, server *fuse.Server) error { return server.Unmount() }
	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	require.Zero(t, c.pruneIdleMounts(cutoff, func(string) bool { return false }, unmount), "a real open reader must prevent unmount")
	require.True(t, c.mountedImageReady("image"))
	require.NoError(t, reader.Close())
	require.Equal(t, 1, c.pruneIdleMounts(cutoff, func(string) bool { return false }, unmount))
	require.False(t, c.mountedImageReady("image"))
	_, err = os.Stat(c.imageMountPoint("image"))
	require.True(t, os.IsNotExist(err), "expired mountpoint must stop protecting its archive")
}
