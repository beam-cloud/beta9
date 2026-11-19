//go:build linux

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// fakeKernel answers the freeze ioctls for directories named after
// filesystems, and can leave a freeze unfinished the way the kernel does.
type fakeKernel struct {
	mu      sync.Mutex
	calls   []string
	frozen  map[string]bool
	stalled map[string]chan struct{}
	entered chan string
}

func newFakeKernel() *fakeKernel {
	return &fakeKernel{frozen: map[string]bool{}, stalled: map[string]chan struct{}{}, entered: make(chan string, 8)}
}

// stall holds the next freeze of name until release is called.
func (k *fakeKernel) stall(t *testing.T, name string) (release func()) {
	hold := make(chan struct{})
	k.mu.Lock()
	k.stalled[name] = hold
	k.mu.Unlock()
	release = sync.OnceFunc(func() { close(hold) })
	t.Cleanup(release)
	return release
}

func (k *fakeKernel) ioctl(dir *os.File, request uint) error {
	name := filepath.Base(dir.Name())
	k.mu.Lock()
	if hold := k.stalled[name]; hold != nil && request == ioctlFIFREEZE {
		delete(k.stalled, name)
		k.mu.Unlock()
		k.entered <- name
		<-hold
		k.mu.Lock()
	}
	defer k.mu.Unlock()
	switch {
	case request == ioctlFIFREEZE && k.frozen[name]:
		return unix.EBUSY
	case request == ioctlFIFREEZE:
		k.frozen[name] = true
		k.calls = append(k.calls, "freeze "+name)
	case !k.frozen[name]:
		return unix.EINVAL
	default:
		delete(k.frozen, name)
		k.calls = append(k.calls, "thaw "+name)
	}
	return nil
}

func (k *fakeKernel) log() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.calls)
}

// fakeFreezer freezes directories named after filesystems: the disks the
// host names, and ahead of each the loop-backed ones, latest mount first.
func fakeFreezer(t *testing.T, kernel *fakeKernel, loops ...string) (*freezer, func(name string) string) {
	root := t.TempDir()
	path := func(name string) string {
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			panic(err)
		}
		return dir
	}
	f := newFreezer()
	f.ioctl = kernel.ioctl
	f.loops = func(string) []*os.File {
		var dirs []*os.File
		for _, name := range loops {
			dir, err := os.Open(path(name))
			if err != nil {
				panic(err)
			}
			dirs = append(dirs, dir)
		}
		return dirs
	}
	return f, path
}

func TestFreezeTakesLoopFilesystemsFirst(t *testing.T) {
	kernel := newFakeKernel()
	f, path := fakeFreezer(t, kernel, "nested", "image")

	require.NoError(t, f.freeze(path("disk")))
	require.NoError(t, f.freeze(path("disk")))
	frozen := []string{"freeze nested", "freeze image", "freeze disk"}
	require.Equal(t, frozen, kernel.log())

	require.NoError(t, f.thaw(path("disk")))
	require.NoError(t, f.thaw(path("disk")))
	require.Equal(t, append(frozen, "thaw disk", "thaw image", "thaw nested"), kernel.log())
}

func TestLoopFilesystemsThawAfterTheLastDisk(t *testing.T) {
	kernel := newFakeKernel()
	f, path := fakeFreezer(t, kernel, "image")

	require.NoError(t, f.freeze(path("disk")))
	require.NoError(t, f.freeze(path("data")))
	frozen := []string{"freeze image", "freeze disk", "freeze data"}
	require.Equal(t, frozen, kernel.log())

	require.NoError(t, f.thaw(path("disk")))
	require.Equal(t, append(frozen, "thaw disk"), kernel.log())
	require.NoError(t, f.thaw(path("data")))
	require.Equal(t, append(frozen, "thaw disk", "thaw data", "thaw image"), kernel.log())
}

func TestFreezeAnswersWhileTheKernelStillFreezes(t *testing.T) {
	kernel := newFakeKernel()
	release := kernel.stall(t, "disk")
	f, path := fakeFreezer(t, kernel, "image")
	f.wait = 20 * time.Millisecond

	require.ErrorContains(t, f.freeze(path("disk")), "did not freeze")
	// The filesystem above the disk stays frozen: its writes are what the
	// freeze of the disk could never finish behind.
	require.Equal(t, []string{"freeze image"}, kernel.log())
	// The kernel freezes one filesystem at a time here, so nothing else can.
	require.ErrorContains(t, f.freeze(path("disk")), "has been freezing")
	require.ErrorContains(t, f.freeze(path("data")), "has been freezing")
	require.NoError(t, f.thaw(path("disk")))

	release()
	undone := []string{"freeze image", "freeze disk", "thaw disk", "thaw image"}
	require.Eventually(t, func() bool { return len(kernel.log()) == len(undone) }, 5*time.Second, time.Millisecond)
	require.Equal(t, undone, kernel.log())

	require.NoError(t, f.freeze(path("disk")))
	require.NoError(t, f.thaw(path("disk")))
}

func TestFreezeGivenUpOnStopsBeforeTheDisk(t *testing.T) {
	kernel := newFakeKernel()
	release := kernel.stall(t, "nested")
	f, path := fakeFreezer(t, kernel, "nested", "image")
	f.wait = 20 * time.Millisecond

	require.ErrorContains(t, f.freeze(path("disk")), "did not freeze")
	release()
	undone := []string{"freeze nested", "thaw nested"}
	require.Eventually(t, func() bool { return len(kernel.log()) == len(undone) }, 5*time.Second, time.Millisecond)
	require.Equal(t, undone, kernel.log())

	require.NoError(t, f.freeze(path("disk")))
	require.Equal(t, append(undone, "freeze nested", "freeze image", "freeze disk"), kernel.log())
}

func TestHostThawUndoesAFreezeInFlight(t *testing.T) {
	kernel := newFakeKernel()
	release := kernel.stall(t, "disk")
	f, path := fakeFreezer(t, kernel, "image")

	result := make(chan error, 1)
	go func() { result <- f.freeze(path("disk")) }()
	require.Equal(t, "disk", <-kernel.entered)
	require.NoError(t, f.thaw(path("disk")))
	release()

	require.ErrorIs(t, <-result, errFreezeAbandoned)
	require.Equal(t, []string{"freeze image", "freeze disk", "thaw disk", "thaw image"}, kernel.log())
}

func TestFreezeTheHostLeavesIsThawed(t *testing.T) {
	kernel := newFakeKernel()
	f, path := fakeFreezer(t, kernel, "image")
	f.limit = 20 * time.Millisecond

	require.NoError(t, f.freeze(path("disk")))
	expired := []string{"freeze image", "freeze disk", "thaw disk", "thaw image"}
	require.Eventually(t, func() bool { return len(kernel.log()) == len(expired) }, 5*time.Second, time.Millisecond)
	require.Equal(t, expired, kernel.log())
	require.NoError(t, f.thaw(path("disk")))
	require.Equal(t, expired, kernel.log())
}

// A guest restored from a snapshot wakes with what the host had frozen.
func TestLostHostThawsEverything(t *testing.T) {
	kernel := newFakeKernel()
	release := kernel.stall(t, "data")
	f, path := fakeFreezer(t, kernel, "image")

	require.NoError(t, f.freeze(path("disk")))
	result := make(chan error, 1)
	go func() { result <- f.freeze(path("data")) }()
	require.Equal(t, "data", <-kernel.entered)

	f.thawAll()
	require.Equal(t, []string{"freeze image", "freeze disk", "thaw disk"}, kernel.log())
	release()
	require.ErrorIs(t, <-result, errFreezeAbandoned)
	require.Equal(t, []string{"freeze image", "freeze disk", "thaw disk", "freeze data", "thaw data", "thaw image"}, kernel.log())
}

func TestLoopDevices(t *testing.T) {
	previous := sysBlockDir
	sysBlockDir = t.TempDir()
	t.Cleanup(func() { sysBlockDir = previous })
	write := func(path, content string) {
		path = filepath.Join(sysBlockDir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content+"\n"), 0o644))
	}

	// A writable loop device whose partition is under a device-mapper volume.
	write("loop0/dev", "7:0")
	write("loop0/ro", "0")
	write("loop0/loop/backing_file", "/root/disk.img")
	write("loop0/loop0p1/dev", "259:1")
	write("dm-3/dev", "253:3")
	holders := filepath.Join(sysBlockDir, "loop0", "loop0p1", "holders")
	require.NoError(t, os.MkdirAll(holders, 0o755))
	require.NoError(t, os.Symlink(filepath.Join(sysBlockDir, "dm-3"), filepath.Join(holders, "dm-3")))
	// One without a file, a read-only one, and a disk that is not a loop device.
	write("loop1/dev", "7:1")
	write("loop2/dev", "7:2")
	write("loop2/ro", "1")
	write("loop2/loop/backing_file", "/snap/core.snap")
	write("vda/dev", "254:0")

	require.Equal(t, map[string]bool{
		"7:0": true, "/dev/loop0": true,
		"259:1": true, "/dev/loop0p1": true,
		"253:3": true, "/dev/dm-3": true,
	}, loopDevices())
}

func TestMountsOf(t *testing.T) {
	table := `29 1 0:23 / / rw,relatime shared:1 - overlay overlay rw,lowerdir=/.beam/image,upperdir=/.beam/disk/overlay/upper
30 29 254:0 / /.beam/disk rw,relatime shared:2 - ext4 /dev/vda rw
52 29 7:0 / /mnt/disk\040image rw,relatime shared:55 - ext4 /dev/loop0 rw
53 29 0:45 /volume /mnt/pool rw,relatime - btrfs /dev/loop0p1 rw,space_cache=v2
54 29 253:3 / /mnt/volume rw,relatime shared:56 master:1 - xfs /dev/mapper/data rw
`
	devices := map[string]bool{"7:0": true, "/dev/loop0": true, "259:1": true, "/dev/loop0p1": true, "253:3": true, "/dev/dm-3": true}
	require.Equal(t, []mountEntry{
		{id: 52, device: "7:0", point: "/mnt/disk image"},
		{id: 53, device: "0:45", point: "/mnt/pool"},
		{id: 54, device: "253:3", point: "/mnt/volume"},
	}, mountsOf(table, devices))
}

func TestMountTablesListEachNamespaceOnce(t *testing.T) {
	previous := procDir
	procDir = t.TempDir()
	t.Cleanup(func() { procDir = previous })
	process := func(pid, namespace string) {
		dir := filepath.Join(procDir, pid, "ns")
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.Symlink("mnt:["+namespace+"]", filepath.Join(dir, "mnt")))
	}
	process("self", "4026531841")
	process("1", "4026531841")
	process("40", "4026532300")
	process("41", "4026532300")
	require.NoError(t, os.MkdirAll(filepath.Join(procDir, "sys"), 0o755))

	require.Equal(t, []mountTable{
		{path: filepath.Join(procDir, "self", "mountinfo")},
		{root: filepath.Join(procDir, "40", "root"), path: filepath.Join(procDir, "40", "mountinfo")},
	}, mountTables())
}

const (
	loopFreezeTestEnv     = "BEAM_VM_LOOP_FREEZE_TEST"
	loopFreezeWorkloadEnv = "BEAM_VM_LOOP_FREEZE_WORKLOAD"
)

// TestFreezeUnderALoopFilesystem freezes a disk as the host does while a
// write to it waits for a loop-backed filesystem on it, which is how the root
// of a guest came to be frozen for good. It needs root, loop devices and a
// machine that can be thrown away: the kernel cannot undo a freeze that never
// finishes, and every loop-backed filesystem of the machine is frozen.
func TestFreezeUnderALoopFilesystem(t *testing.T) {
	if os.Getenv(loopFreezeTestEnv) != "1" {
		t.Skip("freezes filesystems of the machine it runs on: set " + loopFreezeTestEnv + "=1 as root in a disposable VM")
	}
	disk, root, image := loopFreezeLayout(t)
	exerciseLoopFreeze(t, disk, root, image)
}

// loopFreezeLayout lays filesystems out as a guest has them: a root that is
// an overlay on the disk, and on that root the image of another filesystem.
func loopFreezeLayout(t *testing.T) (disk, root, image string) {
	dir := t.TempDir()
	run := func(name string, args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
		require.NoError(t, err, "%s %s: %s", name, strings.Join(args, " "), out)
	}
	mount := func(target string, args ...string) {
		t.Helper()
		require.NoError(t, os.MkdirAll(target, 0o755))
		run("mount", append(args, target)...)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if out, err := exec.CommandContext(ctx, "umount", target).CombinedOutput(); err != nil {
				t.Errorf("umount %s: %v: %s", target, err, out)
			}
		})
	}
	loopMount := func(file, size, target string) {
		t.Helper()
		run("truncate", "-s", size, file)
		run("mkfs.ext4", "-q", "-F", file)
		mount(target, "-o", "loop", file)
	}

	disk, root, image = filepath.Join(dir, "disk"), filepath.Join(dir, "root"), filepath.Join(dir, "image")
	loopMount(filepath.Join(dir, "disk.img"), "1G", disk)
	lower, upper, work := filepath.Join(dir, "lower"), filepath.Join(disk, "upper"), filepath.Join(disk, "work")
	for _, layer := range []string{lower, upper, work} {
		require.NoError(t, os.MkdirAll(layer, 0o755))
	}
	mount(root, "-t", "overlay", "overlay", "-o", fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lower, upper, work))
	loopMount(filepath.Join(root, "image.img"), "256M", image)
	return disk, root, image
}

// exerciseLoopFreeze freezes disk while one process keeps writing to root,
// which is on the disk, what it reads from image, whose file is on root, and
// another keeps image writing to that file.
func exerciseLoopFreeze(t *testing.T, disk, root, image string) {
	source, target, busy := filepath.Join(image, "source"), filepath.Join(root, "copy"), filepath.Join(image, "busy")
	require.NoError(t, os.WriteFile(source, bytes.Repeat([]byte{0xa5}, 32<<20), 0o644))
	unix.Sync()
	startFreezeWorkload(t, "sync", busy)
	copier := startFreezeWorkload(t, "copy", source, target)

	// Frozen on its own, the disk waits for a copy that waits for the image,
	// which waits to write to the disk.
	unordered := newFreezer()
	unordered.loops = func(string) []*os.File { return nil }
	unordered.wait = 5 * time.Second
	var stalled error
	for attempt := 1; attempt <= 20 && stalled == nil; attempt++ {
		if stalled = unordered.freeze(disk); stalled != nil {
			t.Logf("freeze %d without the loop-backed filesystems frozen first: %v", attempt, stalled)
			break
		}
		require.NoError(t, unordered.thaw(disk))
		time.Sleep(20 * time.Millisecond)
	}
	if stalled == nil {
		t.Skip("this kernel finishes a freeze that the workload should have held up")
	}
	require.ErrorContains(t, stalled, "did not freeze")
	// Only the end of that copy lets the kernel finish, and the freezer undo.
	copier.stop()
	require.Eventually(t, func() bool {
		unordered.mu.Lock()
		defer unordered.mu.Unlock()
		return unordered.active == nil
	}, time.Minute, 50*time.Millisecond, "the kernel is still freezing %s: restart this machine", disk)

	copier = startFreezeWorkload(t, "copy", source, target)
	var found []string
	for _, dir := range loopFilesystems(disk) {
		found = append(found, dir.Name())
		dir.Close()
	}
	require.Contains(t, found, image)
	require.NotContains(t, found, disk)

	f := newFreezer()
	copies := copier.rounds.Load()
	for i := range 100 {
		require.NoError(t, f.freeze(disk), "freeze %d", i)
		require.NoError(t, f.thaw(disk), "thaw %d", i)
		time.Sleep(20 * time.Millisecond)
	}
	require.Greater(t, copier.rounds.Load(), copies+10, "the copy stopped competing with the freezes")
}

type freezeWorkload struct {
	cmd    *exec.Cmd
	rounds atomic.Int64
	stop   func()
}

// startFreezeWorkload runs TestFreezeWorkload in a process that can be
// killed, and counts the rounds it reports.
func startFreezeWorkload(t *testing.T, args ...string) *freezeWorkload {
	t.Helper()
	read, write, err := os.Pipe()
	require.NoError(t, err)
	w := &freezeWorkload{cmd: exec.Command(os.Args[0], "-test.run=^TestFreezeWorkload$")}
	w.cmd.Env = append(os.Environ(), loopFreezeWorkloadEnv+"="+strings.Join(args, ":"))
	w.cmd.Stdout, w.cmd.Stderr = write, os.Stderr
	require.NoError(t, w.cmd.Start())
	write.Close()
	go func() {
		defer read.Close()
		buf := make([]byte, 4096)
		for {
			n, err := read.Read(buf)
			w.rounds.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	w.stop = sync.OnceFunc(func() {
		_ = w.cmd.Process.Kill()
		_ = w.cmd.Wait()
	})
	t.Cleanup(w.stop)
	return w
}

// TestFreezeWorkload is one of the processes TestFreezeUnderALoopFilesystem
// freezes a disk under. It writes a byte to stdout for every round.
func TestFreezeWorkload(t *testing.T) {
	args := strings.Split(os.Getenv(loopFreezeWorkloadEnv), ":")
	var round func() error
	switch args[0] {
	case "copy":
		// One write to the target, whose data the kernel reads from the
		// source as it goes: the write is under way for the whole copy.
		source, err := os.Open(args[1])
		require.NoError(t, err)
		info, err := source.Stat()
		require.NoError(t, err)
		target, err := os.OpenFile(args[2], os.O_CREATE|os.O_WRONLY, 0o644)
		require.NoError(t, err)
		round = func() error {
			data, err := unix.Mmap(int(source.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
			if err != nil {
				return err
			}
			_, err = unix.Pwrite(int(target.Fd()), data, 0)
			if unmapErr := unix.Munmap(data); err == nil {
				err = unmapErr
			}
			if err != nil {
				return err
			}
			// Read the source from its device again in the next round.
			return unix.Fadvise(int(source.Fd()), 0, 0, unix.FADV_DONTNEED)
		}
	case "sync":
		// Keeps the filesystem of the file writing to its device.
		file, err := os.OpenFile(args[1], os.O_CREATE|os.O_WRONLY, 0o644)
		require.NoError(t, err)
		block := make([]byte, 4096)
		round = func() error {
			if _, err := unix.Pwrite(int(file.Fd()), block, 0); err != nil {
				return err
			}
			return unix.Fsync(int(file.Fd()))
		}
	default:
		t.Skip("run by TestFreezeUnderALoopFilesystem")
	}
	for {
		require.NoError(t, round())
		_, err := os.Stdout.Write([]byte{'.'})
		require.NoError(t, err)
	}
}
