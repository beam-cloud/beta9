package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
				if err := os.WriteFile(filepath.Join(sysBlock, deviceName, "pid"), []byte("123\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				if deviceName == "nbd0" {
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
	if device.name != "nbd1" {
		t.Fatalf("acquired %s after nbd0 contention, want nbd1", device.name)
	}
}
