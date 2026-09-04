package disk

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeHost emulates the side effects of the external tools an attach drives:
// the daemon writes its pidfile and sockets, nbd-client publishes the kernel's
// connection state under sysfs, and everything else succeeds silently.
type fakeHost struct {
	sysBlock string
	mu       sync.Mutex
	commands []string
}

func (h *fakeHost) run(_ context.Context, name string, args ...string) ([]byte, error) {
	h.mu.Lock()
	h.commands = append(h.commands, name+" "+strings.Join(args, " "))
	h.mu.Unlock()
	switch name {
	case "qemu-img":
		return nil, os.WriteFile(args[len(args)-2], []byte("qcow2-stub"), 0o600)
	case "qemu-storage-daemon":
		for i, arg := range args {
			switch {
			case arg == "--pidfile":
				if err := os.WriteFile(args[i+1], []byte("4242\n"), 0o600); err != nil {
					return nil, err
				}
			case strings.HasPrefix(arg, "socket,path="):
				path := strings.SplitN(strings.TrimPrefix(arg, "socket,path="), ",", 2)[0]
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					return nil, err
				}
			case strings.HasPrefix(arg, "addr.type=unix,addr.path="):
				if err := os.WriteFile(strings.TrimPrefix(arg, "addr.type=unix,addr.path="), nil, 0o600); err != nil {
					return nil, err
				}
			}
		}
	case "nbd-client":
		if args[0] == "-d" {
			return nil, os.Remove(filepath.Join(h.sysBlock, filepath.Base(args[1]), "pid"))
		}
		device := filepath.Base(args[4])
		if err := os.WriteFile(filepath.Join(h.sysBlock, device, "pid"), []byte("4243\n"), 0o644); err != nil {
			return nil, err
		}
	case "mknod":
		return nil, os.WriteFile(args[2], nil, 0o600)
	case "stat":
		return []byte("6180:2b:" + strings.TrimPrefix(filepath.Base(args[len(args)-1]), "nbd") + "\n"), nil
	}
	return nil, nil
}

func (h *fakeHost) ran(prefix string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var matches []string
	for _, command := range h.commands {
		if strings.HasPrefix(command, prefix) {
			matches = append(matches, command)
		}
	}
	return matches
}

const testSpareSize = int64(1 << 30)

// newSpareTestManager exposes enough NBD devices for one attached volume and a
// full spare pool on top of the device reserve.
func newSpareTestManager(t *testing.T) (*Manager, *fakeHost) {
	t.Helper()
	return newSpareTestManagerWithDevices(t, spareDeviceReserve+spareTarget+2)
}

func newSpareTestManagerWithDevices(t *testing.T, devices int) (*Manager, *fakeHost) {
	t.Helper()
	// Unix socket paths are limited to ~104 bytes; keep the root short.
	root, err := os.MkdirTemp("", "qd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	sysBlock, dev := filepath.Join(root, "sys"), filepath.Join(root, "dev")
	for i := 0; i < devices; i++ {
		name := "nbd" + strconv.Itoa(i)
		writeTestNBDDevice(t, sysBlock, name, "43:"+strconv.Itoa(i))
		if err := os.WriteFile(filepath.Join(sysBlock, name, "size"), []byte(strconv.FormatInt(testSpareSize/sectorSize, 10)+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	host := &fakeHost{sysBlock: sysBlock}
	manager := NewManager(Config{Root: filepath.Join(root, "r"), SysBlockPath: sysBlock, DevPath: dev, Runner: host.run})
	return manager, host
}

func waitForSpares(t *testing.T, manager *Manager, size int64, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		got := len(manager.spares[size])
		manager.mu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("spare pool did not reach %d volumes", want)
}

func TestAttachAdoptsSpareAndReplenishes(t *testing.T) {
	manager, host := newSpareTestManager(t)
	ctx := context.Background()
	spare, err := manager.buildSpare(ctx, testSpareSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(host.ran("mkfs.ext4")) != 1 || len(host.ran("mount")) != 0 {
		t.Fatalf("a spare must be formatted but not mounted: %v", host.commands)
	}
	manager.spares[testSpareSize] = []*Volume{spare}
	spareKey, spareDir := spare.state.Key, spare.dir
	realSpareDir, _ := filepath.EvalSymlinks(spareDir)

	mountpoint := filepath.Join(manager.root, "mnt")
	volume, err := manager.Attach(ctx, AttachSpec{Key: "disk", VirtualSizeBytes: testSpareSize, Mountpoint: mountpoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if volume != spare || volume.state.Key != "disk" || volume.Mountpoint() != mountpoint {
		t.Fatalf("attach must hand out the rekeyed spare, got %+v", volume.state)
	}
	if got := host.ran("mount"); len(got) != 1 || !strings.Contains(got[0], mountpoint) {
		t.Fatalf("adoption must mount once at %s, ran %v", mountpoint, got)
	}
	if len(host.ran("nbd-client -unix")) != 1 {
		t.Fatalf("adoption must not connect a second device: %v", host.commands)
	}
	if target, err := filepath.EvalSymlinks(manager.volumeDir("disk")); err != nil || target != realSpareDir {
		t.Fatalf("volumes/disk must index the spare directory %s, got %s (%v)", spareDir, target, err)
	}
	if fileExists(manager.runtimeDir(spareKey)) || !fileExists(filepath.Join(manager.runtimeDir("disk"), "qmp.sock")) {
		t.Fatal("runtime directory must move under the adopted key")
	}
	saved, err := loadVolumeState(spareDir)
	if err != nil || saved.Key != "disk" || !saved.Attached || saved.QMPSocket != volume.qsd.qmpSocket {
		t.Fatalf("persisted state = %+v, %v", saved, err)
	}

	// The pool refills in the background, and a second attach reuses the
	// adopted volume's state through the index instead of taking a spare.
	waitForSpares(t, manager, testSpareSize, spareTarget)
	if err := manager.Detach(ctx, "disk"); err != nil {
		t.Fatal(err)
	}
	reattached, err := manager.Attach(ctx, AttachSpec{Key: "disk", VirtualSizeBytes: testSpareSize, Mountpoint: mountpoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reattached.dir != realSpareDir || reattached.state.HeadPath != saved.HeadPath {
		t.Fatalf("re-attach must reuse the adopted directory, got %s", reattached.dir)
	}
	manager.mu.Lock()
	remaining := len(manager.spares[testSpareSize])
	manager.mu.Unlock()
	if remaining != spareTarget {
		t.Fatalf("re-attach must not consume a spare, pool = %d", remaining)
	}
	manager.destroySpares()
}

func TestAttachWithoutSpareBuildsFreshVolume(t *testing.T) {
	manager, host := newSpareTestManager(t)
	ctx := context.Background()
	mountpoint := filepath.Join(manager.root, "mnt")
	volume, err := manager.Attach(ctx, AttachSpec{Key: "disk", VirtualSizeBytes: testSpareSize, Mountpoint: mountpoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if volume.dir != manager.volumeDir("disk") || !volume.state.Formatted {
		t.Fatalf("fresh attach must build in place, got %+v", volume.state)
	}
	if len(host.ran("mkfs.ext4")) != 1 {
		t.Fatalf("fresh attach formats exactly once: %v", host.commands)
	}
	waitForSpares(t, manager, testSpareSize, spareTarget)
	if err := manager.Close(ctx); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.spares[testSpareSize]) != 0 || !manager.closed {
		t.Fatal("Close must destroy the spare pool")
	}
}

func spareCount(manager *Manager, size int64) int {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return len(manager.spares[size])
}

func TestAttachReclaimsSpareWhenDevicesExhausted(t *testing.T) {
	// Spares hold every device. They are built directly because replenish
	// would stop at the reserve.
	manager, host := newSpareTestManagerWithDevices(t, spareTarget)
	ctx := context.Background()
	for i := 0; i < spareTarget; i++ {
		spare, err := manager.buildSpare(ctx, testSpareSize)
		if err != nil {
			t.Fatal(err)
		}
		manager.spares[testSpareSize] = append(manager.spares[testSpareSize], spare)
	}
	if free := manager.freeNBDDevices(); free != 0 {
		t.Fatalf("spares must consume every device, %d free", free)
	}

	// Existing local state makes the attach non-fresh, so it cannot adopt a
	// spare and must find a device of its own.
	if err := os.MkdirAll(manager.volumeDir("disk"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := saveVolumeState(manager.volumeDir("disk"), &volumeState{Key: "disk"}); err != nil {
		t.Fatal(err)
	}
	volume, err := manager.Attach(ctx, AttachSpec{Key: "disk", VirtualSizeBytes: testSpareSize, Mountpoint: filepath.Join(manager.root, "mnt")}, nil)
	if err != nil {
		t.Fatalf("attach must reclaim a spare's device, got %v", err)
	}
	if volume.dir != manager.volumeDir("disk") || volume.nbd == nil {
		t.Fatalf("attach must build in place on a reclaimed device, got %+v", volume.state)
	}
	if got := spareCount(manager, testSpareSize); got != spareTarget-1 {
		t.Fatalf("exactly one spare must be released, pool = %d", got)
	}
	if got := host.ran("nbd-client -d"); len(got) != 1 {
		t.Fatalf("exactly one spare must be disconnected, ran %v", got)
	}
	manager.destroySpares()
}

func TestReplenishStopsAtDeviceReserve(t *testing.T) {
	manager, _ := newSpareTestManagerWithDevices(t, spareDeviceReserve+1)
	manager.replenishSpares(testSpareSize)

	// Wait for the builder to exit; only one build fits above the reserve.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		building := manager.spareBuilds[testSpareSize]
		manager.mu.Unlock()
		if !building {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	manager.mu.Lock()
	building := manager.spareBuilds[testSpareSize]
	manager.mu.Unlock()
	if building {
		t.Fatal("spare builder must stop at the device reserve")
	}
	if got := spareCount(manager, testSpareSize); got > 1 {
		t.Fatalf("at most one spare fits above the reserve, pool = %d", got)
	}
	if free := manager.freeNBDDevices(); free < spareDeviceReserve {
		t.Fatalf("spares must leave %d devices free, %d free", spareDeviceReserve, free)
	}
	manager.destroySpares()
}

func TestRecoverRemovesUnadoptedSpares(t *testing.T) {
	manager, _ := newSpareTestManager(t)
	spareDir := manager.volumeDir(sparePrefix + "abc")
	if err := os.MkdirAll(spareDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := saveVolumeState(spareDir, &volumeState{Key: sparePrefix + "abc", Attached: true, QSDPid: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	// An adopted spare keeps its directory name but carries the real key, and
	// must be treated like any other volume.
	adoptedDir := manager.volumeDir(sparePrefix + "def")
	if err := os.MkdirAll(adoptedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := saveVolumeState(adoptedDir, &volumeState{Key: "disk", Attached: true, QSDPid: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(adoptedDir, manager.volumeDir("disk")); err != nil {
		t.Fatal(err)
	}

	if err := manager.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fileExists(spareDir) {
		t.Fatal("unadopted spare directory must be removed")
	}
	state, err := loadVolumeState(adoptedDir)
	if err != nil || state == nil || state.Attached {
		t.Fatalf("adopted volume must be kept and marked detached, got %+v (%v)", state, err)
	}
	dir, err := manager.resolveVolumeDir("disk")
	if realAdoptedDir, _ := filepath.EvalSymlinks(adoptedDir); err != nil || dir != realAdoptedDir {
		t.Fatalf("index must still resolve to %s, got %s (%v)", adoptedDir, dir, err)
	}
}

func TestResolveVolumeDirDropsDanglingIndex(t *testing.T) {
	manager, _ := newSpareTestManager(t)
	if err := os.MkdirAll(filepath.Join(manager.root, "volumes"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(manager.root, "volumes", "gone"), manager.volumeDir("disk")); err != nil {
		t.Fatal(err)
	}
	dir, err := manager.resolveVolumeDir("disk")
	if err != nil || dir != manager.volumeDir("disk") {
		t.Fatalf("resolveVolumeDir = %s, %v", dir, err)
	}
	if _, err := os.Lstat(manager.volumeDir("disk")); !os.IsNotExist(err) {
		t.Fatal("dangling index must be removed")
	}
}

func TestRecoverRebuildsSparesForRememberedSizes(t *testing.T) {
	manager, host := newSpareTestManager(t)
	ctx := context.Background()
	if _, err := manager.Attach(ctx, AttachSpec{Key: "disk", VirtualSizeBytes: testSpareSize, Mountpoint: filepath.Join(manager.root, "mnt")}, nil); err != nil {
		t.Fatal(err)
	}
	waitForSpares(t, manager, testSpareSize, spareTarget)
	manager.rememberSpareSize(7)
	manager.rememberSpareSize(testSpareSize) // re-recording moves it to the front
	if got := manager.rememberedSpareSizes(); len(got) != 2 || got[0] != testSpareSize || got[1] != 7 {
		t.Fatalf("remembered sizes = %v", got)
	}
	manager.Close(ctx)

	// A restarted worker with the same root warms the pool during Recover.
	restarted := &Manager{root: manager.root, binaries: manager.binaries, sysBlockPath: manager.sysBlockPath, devPath: manager.devPath,
		run: manager.run, maxChainDepth: DefaultMaxChainDepth, volumes: map[string]*Volume{}, spares: map[int64][]*Volume{}, spareBuilds: map[int64]bool{}}
	before := len(host.ran("mkfs.ext4"))
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	waitForSpares(t, restarted, testSpareSize, spareTarget)
	if len(host.ran("mkfs.ext4")) < before+spareTarget {
		t.Fatalf("recover must build %d spares, ran %v", spareTarget, host.commands)
	}
	restarted.Close(ctx)
}
