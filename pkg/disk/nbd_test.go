package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeTestNBDDevice(t *testing.T, sysBlock, name, deviceNumber string) {
	t.Helper()
	dir := filepath.Join(sysBlock, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dev"), []byte(deviceNumber+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureNBDDevicesUsesExistingKernelDevicesAndCreatesPrivateNodes(t *testing.T) {
	sysBlock, dev := t.TempDir(), t.TempDir()
	writeTestNBDDevice(t, sysBlock, "nbd0", "43:0")
	var calls []string
	manager := NewManager(Config{
		SysBlockPath: sysBlock,
		DevPath:      dev,
		Runner: func(_ context.Context, name string, args ...string) ([]byte, error) {
			calls = append(calls, name+" "+strings.Join(args, " "))
			switch name {
			case "mknod":
				return nil, os.WriteFile(args[2], nil, 0o600)
			case "stat":
				return []byte("6180:2b:0\n"), nil
			case "modprobe":
				t.Fatal("modprobe ran despite an existing kernel NBD device")
			}
			return nil, nil
		},
	})

	if err := manager.ensureNBDDevices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || !strings.HasPrefix(calls[0], "mknod ") || !strings.HasPrefix(calls[1], "stat ") {
		t.Fatalf("commands = %#v", calls)
	}
}

func TestEnsureNBDDevicesPreparesOnceUnderConcurrentFirstAttaches(t *testing.T) {
	sysBlock, dev := t.TempDir(), t.TempDir()
	const devices = 4
	for i := 0; i < devices; i++ {
		writeTestNBDDevice(t, sysBlock, "nbd"+strconv.Itoa(i), "43:"+strconv.Itoa(i))
	}
	var mu sync.Mutex
	validations := 0
	manager := NewManager(Config{
		SysBlockPath: sysBlock,
		DevPath:      dev,
		Runner: func(_ context.Context, name string, args ...string) ([]byte, error) {
			switch name {
			case "mknod":
				return nil, os.WriteFile(args[2], nil, 0o600)
			case "stat":
				mu.Lock()
				validations++
				mu.Unlock()
				// Widen the window in which an unserialized caller would start
				// its own pass.
				time.Sleep(2 * time.Millisecond)
				return []byte("6180:2b:" + strings.TrimPrefix(filepath.Base(args[len(args)-1]), "nbd") + "\n"), nil
			}
			return nil, nil
		},
	})

	const callers = 8
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for i := 0; i < callers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- manager.ensureNBDDevices(context.Background())
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if validations != devices {
		t.Fatalf("concurrent first attaches validated devices %d times, want one pass of %d", validations, devices)
	}
}

func TestEnsureNBDDevicesLoadsModuleBeforeCreatingNodes(t *testing.T) {
	sysBlock, dev := t.TempDir(), t.TempDir()
	var calls []string
	manager := NewManager(Config{
		SysBlockPath: sysBlock,
		DevPath:      dev,
		Runner: func(_ context.Context, name string, args ...string) ([]byte, error) {
			calls = append(calls, name+" "+strings.Join(args, " "))
			switch name {
			case "modprobe":
				writeTestNBDDevice(t, sysBlock, "nbd0", "43:0")
			case "mknod":
				return nil, os.WriteFile(args[2], nil, 0o600)
			case "stat":
				return []byte("6180:2b:0\n"), nil
			}
			return nil, nil
		},
	})

	if err := manager.ensureNBDDevices(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 || calls[0] != "modprobe nbd nbds_max=64" {
		t.Fatalf("commands = %#v", calls)
	}
}

func TestEnsureNBDDevicesRejectsMissingDevicesAfterModuleLoad(t *testing.T) {
	manager := NewManager(Config{
		SysBlockPath: t.TempDir(),
		DevPath:      t.TempDir(),
		Runner: func(context.Context, string, ...string) ([]byte, error) {
			return nil, nil
		},
	})

	err := manager.ensureNBDDevices(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no nbd devices appeared") {
		t.Fatalf("ensureNBDDevices() error = %v", err)
	}
}

func TestEnsureNBDDevicesRejectsWrongExistingNode(t *testing.T) {
	sysBlock, dev := t.TempDir(), t.TempDir()
	writeTestNBDDevice(t, sysBlock, "nbd0", "43:0")
	if err := os.WriteFile(filepath.Join(dev, "nbd0"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(Config{
		SysBlockPath: sysBlock,
		DevPath:      dev,
		Runner: func(_ context.Context, name string, _ ...string) ([]byte, error) {
			if name == "stat" {
				return []byte("8180:0:0\n"), nil
			}
			return nil, fmt.Errorf("unexpected command %s", name)
		},
	})

	err := manager.ensureNBDDevices(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not a block device") {
		t.Fatalf("ensureNBDDevices() error = %v", err)
	}
}

func TestEnsureNBDDevicesRejectsMalformedKernelDeviceNumber(t *testing.T) {
	sysBlock := t.TempDir()
	writeTestNBDDevice(t, sysBlock, "nbd0", "invalid")
	manager := NewManager(Config{SysBlockPath: sysBlock, DevPath: t.TempDir()})

	err := manager.ensureNBDDevices(context.Background())
	if err == nil || !strings.Contains(err.Error(), "malformed value") {
		t.Fatalf("ensureNBDDevices() error = %v", err)
	}
}

func TestDisconnectTreatsAlreadyClearedDeviceAsSuccess(t *testing.T) {
	manager := NewManager(Config{
		SysBlockPath: t.TempDir(),
		DevPath:      t.TempDir(),
		Runner: func(context.Context, string, ...string) ([]byte, error) {
			return nil, fmt.Errorf("not connected")
		},
	})

	if err := manager.disconnectNBDDevice(context.Background(), &nbdDevice{name: "nbd0", Path: "/dev/nbd0"}); err != nil {
		t.Fatalf("disconnect already-cleared device: %v", err)
	}
}

func TestAcquireNBDDeviceContinuesAfterKernelContention(t *testing.T) {
	sysBlock, dev := t.TempDir(), t.TempDir()
	writeTestNBDDevice(t, sysBlock, "nbd0", "43:0")
	writeTestNBDDevice(t, sysBlock, "nbd1", "43:1")
	for _, name := range []string{"nbd0", "nbd1"} {
		if err := os.WriteFile(filepath.Join(sysBlock, name, "size"), []byte("8\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dev, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var connects []string
	manager := NewManager(Config{
		Root:         t.TempDir(),
		SysBlockPath: sysBlock,
		DevPath:      dev,
		Runner: func(_ context.Context, name string, args ...string) ([]byte, error) {
			switch name {
			case "stat":
				if strings.HasSuffix(args[len(args)-1], "nbd0") {
					return []byte("6180:2b:0\n"), nil
				}
				return []byte("6180:2b:1\n"), nil
			case "nbd-client":
				deviceName := filepath.Base(args[4])
				connects = append(connects, deviceName)
				if err := os.WriteFile(filepath.Join(sysBlock, deviceName, "pid"), []byte("123\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				if deviceName == "nbd1" {
					return []byte("Failed to setup device, check dmesg"), fmt.Errorf("exit status 1")
				}
				return nil, nil
			default:
				return nil, fmt.Errorf("unexpected command %s", name)
			}
		},
	})

	device, err := manager.acquireNBDDevice(context.Background(), "/tmp/nbd.sock", 4096)
	if err != nil {
		t.Fatalf("acquire after contention: %v", err)
	}
	defer device.release()
	// Workers that do not claim devices take the lowest free one, so the scan
	// starts at the highest.
	if strings.Join(connects, ",") != "nbd1,nbd0" || device.name != "nbd0" {
		t.Fatalf("connected %v and acquired %s, want nbd1 then nbd0 after its contention", connects, device.name)
	}
}

// The send buffer must be widened on the socket the kernel sends requests
// through: the one held by the configured client serving that exact device.
func TestNBDClientSocketsFindsTheServingClient(t *testing.T) {
	proc := t.TempDir()
	process := func(pid string, args []string, fds map[string]string) {
		dir := filepath.Join(proc, pid)
		if err := os.MkdirAll(filepath.Join(dir, "fd"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(strings.Join(args, "\x00")+"\x00"), 0o644); err != nil {
			t.Fatal(err)
		}
		for fd, target := range fds {
			if err := os.Symlink(target, filepath.Join(dir, "fd", fd)); err != nil {
				t.Fatal(err)
			}
		}
	}
	process("10", []string{"nbd-client", "-unix", "/run/a.sock", "-N", "vol", "/dev/nbd10"}, map[string]string{"4": "socket:[100]"})
	process("11", []string{"nbd-client", "-d", "/dev/nbd1"}, map[string]string{"4": "socket:[101]"})
	process("12", []string{"mkfs.ext4", "/dev/nbd1"}, map[string]string{"4": "socket:[102]"})
	process("13", []string{"/usr/sbin/nbd-client", "-unix", "/run/b.sock", "-N", "vol", "/dev/nbd1"},
		map[string]string{"0": "/dev/null", "3": "/dev/nbd1", "4": "socket:[103]", "5": "pipe:[104]"})
	process("14", []string{"/opt/nbd/nbd-client-3.26", "-unix", "/run/c.sock", "-N", "vol", "/dev/nbd3"},
		map[string]string{"5": "socket:[105]"})

	pid, sockets, err := nbdClientSockets(proc, "nbd-client", "/dev/nbd1")
	if err != nil {
		t.Fatal(err)
	}
	if pid != 13 || len(sockets) != 1 || sockets[0] != 4 {
		t.Fatalf("found pid %d sockets %v, want pid 13 socket 4", pid, sockets)
	}
	if _, _, err := nbdClientSockets(proc, "nbd-client", "/dev/nbd2"); err == nil {
		t.Fatal("a device without a client must not resolve")
	}
	if pid, sockets, err := nbdClientSockets(proc, "nbd-client-3.26", "/dev/nbd3"); err != nil || pid != 14 || len(sockets) != 1 || sockets[0] != 5 {
		t.Fatalf("a configured client binary must resolve: pid %d sockets %v err %v", pid, sockets, err)
	}
}
