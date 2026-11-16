package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/moby/sys/mountinfo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCacheFSOpenPreservesWriteFlushAndDirectIO(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, access := range []int{os.O_RDONLY, os.O_WRONLY, os.O_RDWR} {
			n := &FSNode{filesystem: &CacheFS{}, bfsNode: &CacheFSNode{}}
			n.filesystem.Config.CacheFS.DirectIO = direct
			_, flags, errno := n.Open(context.Background(), uint32(access))
			require.Zero(t, errno)
			require.Equal(t, access == os.O_RDONLY, flags&fuse.FOPEN_NOFLUSH != 0)
			require.Equal(t, direct, flags&fuse.FOPEN_DIRECT_IO != 0)
		}
	}
}

func TestCacheFSProcessExitWithOpenFile(t *testing.T) {
	const exitReadyMarker = "cachefs exit test ready"
	if os.Getenv("BEAM_TEST_CACHEFS_EXIT") != "" {
		ctx := context.Background()
		metadata := NewMockCacheMetadataStore()
		require.NoError(t, metadata.SetFsNode(ctx, GenerateFsID("/file"), &FSMetadata{
			ID: GenerateFsID("/file"), PID: GenerateFsID("/"), Path: "/file", Name: "file", Ino: 2, Mode: fuse.S_IFREG | 0444,
		}))
		var config ClientConfig
		config.CacheFS.MountPoint = os.Getenv("BEAM_TEST_CACHEFS_MOUNT")
		config.CacheFS.DirectMount = true
		config.CacheFS.DirectIO = os.Getenv("BEAM_TEST_CACHEFS_DIRECT") == "true"
		start, _, _, err := Mount(ctx, FSSystemOpts{Config: config, MetadataStore: metadata})
		require.NoError(t, err)
		require.NoError(t, start())
		file, err := os.Open(filepath.Join(config.CacheFS.MountPoint, "file"))
		require.NoError(t, err)
		defer file.Close() // Keep it live; abrupt exit deliberately bypasses cleanup.
		_, err = file.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.EOF)
		fmt.Fprintln(os.Stdout, exitReadyMarker)
		// Keep a descriptor on our own FUSE mount open while all server threads
		// exit. A synchronous FLUSH here waits forever for this dying process.
		if os.Getenv("BEAM_TEST_CACHEFS_EXIT") == "kill" {
			require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGKILL))
		}
		os.Exit(0)
	}
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux FUSE")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("requires /dev/fuse")
	}
	if os.Geteuid() != 0 {
		t.Skip("requires permission to mount FUSE and recover a blocked test process")
	}
	for _, direct := range []bool{false, true} {
		for _, mode := range []string{"exit", "kill"} {
			t.Run(fmt.Sprintf("%s/direct=%t", mode, direct), func(t *testing.T) {
				mount := t.TempDir()
				defer exec.Command("umount", "-l", mount).Run()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCacheFSProcessExitWithOpenFile$")
				cmd.Env = append(os.Environ(), "BEAM_TEST_CACHEFS_EXIT="+mode, "BEAM_TEST_CACHEFS_MOUNT="+mount, fmt.Sprint("BEAM_TEST_CACHEFS_DIRECT=", direct))
				var output strings.Builder
				cmd.Stdout = &output
				cmd.Stderr = &output
				require.NoError(t, cmd.Start())
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				var err error
				select {
				case err = <-done:
				case <-time.After(5 * time.Second):
					// Abort only this test's mount so a regression cannot wedge
					// the test runner in the same uninterruptible kernel wait.
					mounts, mountErr := mountinfo.GetMounts(nil)
					require.NoError(t, mountErr)
					for _, info := range mounts {
						if info.Mountpoint == mount && info.FSType == "fuse.rawBridge" {
							abort := fmt.Sprintf("/sys/fs/fuse/connections/%d/abort", info.Minor)
							require.NoError(t, os.WriteFile(abort, []byte("1"), 0))
						}
					}
					_ = cmd.Process.Kill()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("could not recover blocked FUSE test process")
					}
					phase := "before reaching process exit"
					if strings.Contains(output.String(), exitReadyMarker) {
						phase = "during process exit"
					}
					t.Fatalf("process blocked on its own cache mount %s: %s", phase, output.String())
				}
				require.Contains(t, output.String(), exitReadyMarker)
				if mode == "kill" {
					var exit *exec.ExitError
					require.True(t, errors.As(err, &exit), "%s", output.String())
					require.Equal(t, syscall.SIGKILL, exit.Sys().(syscall.WaitStatus).Signal(), "%s", output.String())
				} else {
					require.NoError(t, err, "%s", output.String())
				}
			})
		}
	}
}

func TestWriteReadAheadKBTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "read_ahead_kb")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	defer os.Remove(path)

	start := time.Now()
	err := writeReadAheadKB(path, 128, 25*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("writeReadAheadKB took %s, want bounded timeout", elapsed)
	}
}

func TestSpliceSafeMaxWriteFor(t *testing.T) {
	const page = 4096
	cases := []struct{ pipeMax, want int }{
		{1 << 20, (1 << 20) - 2*page}, // stock fs.pipe-max-size: 1 MiB reads must shrink
		{4 << 20, (4 << 20) - 2*page},
		{2 * page, 0},
		{0, 0},
		{(1 << 20) + 100, (1 << 20) - 2*page},
	}
	for _, c := range cases {
		if got := spliceSafeMaxWriteFor(c.pipeMax, page); got != c.want {
			t.Fatalf("spliceSafeMaxWriteFor(%d) = %d, want %d", c.pipeMax, got, c.want)
		}
		// header (16) + payload + one page must fit in the pipe
		if c.want > 0 && 16+c.want+page > c.pipeMax {
			t.Fatalf("pipeMax %d: %d does not leave room for header and extra page", c.pipeMax, c.want)
		}
	}
}
