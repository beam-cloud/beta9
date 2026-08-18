package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// nbdDevice is an attached kernel NBD device. The flock is held for the whole
// attachment so concurrent workers and recovery sweeps never race on a device.
type nbdDevice struct {
	Path string // e.g. /dev/nbd3
	name string // e.g. nbd3
	lock *os.File
}

const (
	nbdSettleTimeout = 15 * time.Second
	// nbdBlockSize is the device block size requested from nbd-client.
	nbdBlockSize = 4096
	sectorSize   = 512
)

// acquireNBDDevice picks a free /dev/nbdN, locks it, and connects it to the
// daemon's NBD unix socket.
func (m *Manager) acquireNBDDevice(ctx context.Context, nbdSocket string, expectedSizeBytes int64) (*nbdDevice, error) {
	names, err := listNBDDeviceNames(m.sysBlockPath)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no nbd devices present; is the nbd kernel module loaded?")
	}
	for _, name := range names {
		device, ok := m.tryLockNBDDevice(name)
		if !ok {
			continue
		}
		if err := m.connectNBDDevice(ctx, device, nbdSocket, expectedSizeBytes); err != nil {
			device.release()
			return nil, err
		}
		return device, nil
	}
	return nil, fmt.Errorf("all %d nbd devices are busy", len(names))
}

// lockNBDDevice takes the exclusive flock for a device without caring whether
// the kernel currently has a server connected. Fresh attachments additionally
// require the device to be free (tryLockNBDDevice); adoption and crash
// cleanup expect it busy.
func (m *Manager) lockNBDDevice(name string) (*nbdDevice, bool) {
	if name == "" || name == "." {
		return nil, false
	}
	if err := os.MkdirAll(m.lockDir(), 0o755); err != nil {
		return nil, false
	}
	lockPath := filepath.Join(m.lockDir(), name+".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false
	}
	if err := flockNB(lock); err != nil {
		lock.Close()
		return nil, false
	}
	return &nbdDevice{Path: "/dev/" + name, name: name, lock: lock}, true
}

func (m *Manager) tryLockNBDDevice(name string) (*nbdDevice, bool) {
	device, ok := m.lockNBDDevice(name)
	if !ok {
		return nil, false
	}
	if m.nbdDeviceBusy(name) {
		device.release()
		return nil, false
	}
	return device, true
}

func (m *Manager) connectNBDDevice(ctx context.Context, device *nbdDevice, nbdSocket string, expectedSizeBytes int64) error {
	if _, err := m.run(ctx, m.binaries.NBDClient, "-unix", nbdSocket, "-N", qsdExportName, device.Path, "-b", strconv.Itoa(nbdBlockSize)); err != nil {
		return fmt.Errorf("connect %s: %w", device.Path, err)
	}
	// The device is usable once the kernel records a server pid and the
	// virtual size is visible.
	deadline := time.Now().Add(nbdSettleTimeout)
	expectedSectors := expectedSizeBytes / sectorSize
	for {
		if m.nbdDeviceBusy(device.name) {
			if sectors, err := m.nbdDeviceSectors(device.name); err == nil && sectors == expectedSectors {
				return nil
			}
		}
		if time.Now().After(deadline) {
			_ = m.disconnectNBDDevice(ctx, device)
			return fmt.Errorf("%s did not settle at %d bytes within %s", device.Path, expectedSizeBytes, nbdSettleTimeout)
		}
		select {
		case <-ctx.Done():
			_ = m.disconnectNBDDevice(ctx, device)
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (m *Manager) disconnectNBDDevice(ctx context.Context, device *nbdDevice) error {
	defer device.release()
	if _, err := m.run(ctx, m.binaries.NBDClient, "-d", device.Path); err != nil {
		return fmt.Errorf("disconnect %s: %w", device.Path, err)
	}
	deadline := time.Now().Add(nbdSettleTimeout)
	for m.nbdDeviceBusy(device.name) {
		if time.Now().After(deadline) {
			return fmt.Errorf("%s is still connected after disconnect", device.Path)
		}
		time.Sleep(pollInterval)
	}
	return nil
}

func flockNB(lock *os.File) error {
	return syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func (d *nbdDevice) release() {
	if d.lock != nil {
		_ = syscall.Flock(int(d.lock.Fd()), syscall.LOCK_UN)
		d.lock.Close()
		d.lock = nil
	}
}

func listNBDDeviceNames(sysBlockPath string) ([]string, error) {
	entries, err := os.ReadDir(sysBlockPath)
	if err != nil {
		return nil, fmt.Errorf("list block devices: %w", err)
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "nbd") && !strings.Contains(name, "p") {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		return len(names[i]) < len(names[j]) || (len(names[i]) == len(names[j]) && names[i] < names[j])
	})
	return names, nil
}

// nbdDeviceBusy reports whether the kernel has a server connected. The pid
// file only exists while a connection is live.
func (m *Manager) nbdDeviceBusy(name string) bool {
	_, err := os.Stat(filepath.Join(m.sysBlockPath, name, "pid"))
	return err == nil
}

func (m *Manager) nbdDeviceSectors(name string) (int64, error) {
	data, err := os.ReadFile(filepath.Join(m.sysBlockPath, name, "size"))
	if err != nil {
		return 0, err
	}
	var sectors int64
	_, err = fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &sectors)
	return sectors, err
}
