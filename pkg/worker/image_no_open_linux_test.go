//go:build linux

package worker

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/clip/pkg/clip"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type imageFuseOperationCounts struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *imageFuseOperationCounts) Add(name string, _ time.Duration) {
	c.mu.Lock()
	c.counts[name]++
	c.mu.Unlock()
}

func TestImageNoOpenRPCRealFuse(t *testing.T) {
	if os.Getenv("BETA9_IMAGE_NO_OPEN_REAL_FUSE") != "1" {
		t.Skip("requires privileged Linux FUSE")
	}
	for _, enabled := range []string{"0", "1"} {
		t.Run("enabled="+enabled, func(t *testing.T) {
			t.Setenv("BETA9_CLIP_NO_OPEN_RPC", enabled)
			source, mount := t.TempDir(), t.TempDir()
			payload := make([]byte, 2<<20)
			_, err := rand.Read(payload)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(source, "payload"), payload, 0751))
			require.NoError(t, os.Symlink("payload", filepath.Join(source, "link")))
			largeDirectory := filepath.Join(source, "many")
			require.NoError(t, os.Mkdir(largeDirectory, 0755))
			var expectedNames []string
			for i := 0; i < 600; i++ {
				name := fmt.Sprintf("entry-%04d-with-a-long-name", i)
				expectedNames = append(expectedNames, name)
				require.NoError(t, os.WriteFile(filepath.Join(largeDirectory, name), []byte{byte(i)}, 0644))
			}
			archive := filepath.Join(t.TempDir(), "image.clip")
			require.NoError(t, clip.NewClipArchiver().Create(clip.ClipArchiverOptions{SourcePath: source, OutputFile: archive, ArchivePath: archive}))
			start, failures, server, err := clip.MountArchive(clip.MountOptions{ArchivePath: archive, MountPoint: mount})
			require.NoError(t, err)
			counts := &imageFuseOperationCounts{counts: map[string]int{}}
			server.RecordLatencies(counts)
			t.Cleanup(func() { require.NoError(t, server.Unmount()) })
			require.NoError(t, start())
			require.NoError(t, server.WaitMount())
			select {
			case err := <-failures:
				require.NoError(t, err)
			default:
			}
			if enabled == "1" {
				require.NotZero(t, server.KernelSettings().Flags&fuse.CAP_NO_OPEN_SUPPORT)
			}
			path := filepath.Join(mount, "payload")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, payload, data)
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.EqualValues(t, 0751, info.Mode().Perm())
			link, err := os.Readlink(filepath.Join(mount, "link"))
			require.NoError(t, err)
			require.Equal(t, "payload", link)
			file, err := os.Open(path)
			require.NoError(t, err)
			mapped, err := unix.Mmap(int(file.Fd()), 0, len(payload), unix.PROT_READ, unix.MAP_SHARED)
			require.NoError(t, err)
			require.True(t, bytes.Equal(payload, mapped), "mapped executable reads must retain exact bytes")
			if enabled == "1" {
				require.ErrorIs(t, unix.Unmount(mount, 0), unix.EBUSY, "open image readers must still fence idle-cache expiry")
			}
			require.NoError(t, unix.Munmap(mapped))
			require.NoError(t, file.Close())
			assertImageDirectoryReads(t, filepath.Join(mount, "many"), expectedNames)
			directory, err := os.Open(filepath.Join(mount, "many"))
			require.NoError(t, err)
			require.ErrorIs(t, unix.Unmount(mount, 0), unix.EBUSY, "directory readers must fence idle-cache expiry")
			require.NoError(t, directory.Close())
			var wg sync.WaitGroup
			errors := make(chan error, 16)
			for reader := 0; reader < 16; reader++ {
				wg.Add(1)
				go func(reader int) {
					defer wg.Done()
					for iteration := 0; iteration < 20; iteration++ {
						f, err := os.Open(path)
						if err != nil {
							errors <- err
							return
						}
						offset := (reader*4096 + iteration*8192) % (len(payload) - 1024)
						got := make([]byte, 1024)
						_, readErr := f.ReadAt(got, int64(offset))
						closeErr := f.Close()
						if readErr != nil || closeErr != nil || !bytes.Equal(got, payload[offset:offset+1024]) {
							errors <- fmt.Errorf("concurrent image read mismatch: %v %v", readErr, closeErr)
							return
						}
					}
				}(reader)
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				require.NoError(t, err)
			}
			if enabled == "1" {
				_, err := os.OpenFile(path, os.O_WRONLY, 0)
				require.ErrorIs(t, err, syscall.EROFS, "the kernel must enforce immutable image writes")
			}
			counts.mu.Lock()
			defer counts.mu.Unlock()
			t.Logf("kernel FUSE operation counts: %v", counts.counts)
			if enabled == "1" {
				// Mount readiness opens two internal go-fuse polling files. Only
				// the first ordinary OPEN/FLUSH may remain after those callbacks.
				require.LessOrEqual(t, counts.counts["OPEN"], 4, "the first ENOSYS must disable subsequent file-open RPCs")
				require.LessOrEqual(t, counts.counts["RELEASE"], 2, "only mount-poll handles may need release RPCs")
				require.LessOrEqual(t, counts.counts["FLUSH"], 4, "skipping OPEN must not replace it with per-close FLUSH RPCs")
				require.LessOrEqual(t, counts.counts["OPENDIR"], 1, "the kernel must skip directory-open RPCs after ENOSYS")
				require.Zero(t, counts.counts["RELEASEDIR"], "stateless directories need no release RPC")
			} else {
				require.GreaterOrEqual(t, counts.counts["OPEN"], 320)
			}
		})
	}
}

func assertImageDirectoryReads(t *testing.T, directory string, expected []string) {
	t.Helper()
	fd, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	require.NoError(t, err)
	defer unix.Close(fd)
	// Force small buffers, multiple FUSE reads and a seek back to a cookie
	// returned by the kernel. No per-open state may be needed to resume.
	buf := make([]byte, 256)
	n, err := unix.Getdents(fd, buf)
	require.NoError(t, err)
	require.Positive(t, n)
	cookie, err := unix.Seek(fd, 0, unix.SEEK_CUR)
	require.NoError(t, err)
	n, err = unix.Getdents(fd, buf)
	require.NoError(t, err)
	want := append([]byte(nil), buf[:n]...)
	_, err = unix.Seek(fd, cookie, unix.SEEK_SET)
	require.NoError(t, err)
	n, err = unix.Getdents(fd, buf)
	require.NoError(t, err)
	require.Equal(t, want, buf[:n], "directory cookies must reproduce the next entries")
	_, err = unix.Seek(fd, 0, unix.SEEK_SET)
	require.NoError(t, err)
	var names []string
	for {
		n, err = unix.Getdents(fd, buf)
		require.NoError(t, err)
		if n == 0 {
			break
		}
		_, _, names = syscall.ParseDirent(buf[:n], -1, names)
	}
	sort.Strings(names)
	require.Equal(t, expected, names)
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := os.Open(directory)
			if err != nil {
				errors <- err
				return
			}
			defer f.Close()
			var got []string
			for {
				entries, err := f.ReadDir(7)
				for _, e := range entries {
					got = append(got, e.Name())
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					errors <- err
					return
				}
			}
			sort.Strings(got)
			if len(got) != len(expected) || fmt.Sprint(got) != fmt.Sprint(expected) {
				errors <- fmt.Errorf("concurrent directory reader lost or duplicated entries")
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}
