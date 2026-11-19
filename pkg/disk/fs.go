package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// directoryHashSeed is what ext4 hashes file names with to order the entries
// of a directory. mkfs draws one at random, so two disks list the same files
// in different orders, and a program that takes files in the order they are
// listed (a test runner that does not sort) behaves differently on each VM.
// GitHub's runners boot copies of a disk image, so those of one image share a
// seed. This is the seed most of its ubuntu-24.04 runners had when this was
// written (images 20261111.370 and 20261116.376): a checkout on a Beam runner
// lists in the order it does there, and in the same order on every VM.
// A disk belongs to one workspace, so nothing is lost by the seed being known.
const directoryHashSeed = "d6844188-c353-45ca-9eef-12a30c6aea47"

// formatExt4 formats a freshly created block device. Inode tables and the
// journal are left for the kernel to initialize lazily, and the device is not
// discarded first: it is a brand new qcow2 whose unallocated clusters already
// read as zeros, so eager zeroing and a whole-device trim over NBD (the bulk of
// mkfs time) accomplish nothing.
func (m *Manager) formatExt4(ctx context.Context, devicePath string) error {
	_, err := m.run(ctx, m.binaries.MkfsExt4, "-F", "-q", "-m", "0",
		"-E", "lazy_itable_init=1,lazy_journal_init=1,nodiscard,hash_seed="+directoryHashSeed, devicePath)
	if err != nil {
		return fmt.Errorf("format %s: %w", devicePath, err)
	}
	return nil
}

func (m *Manager) mountExt4(ctx context.Context, devicePath, mountpoint string, readOnly bool) error {
	if err := os.MkdirAll(mountpoint, 0o755); err != nil {
		return err
	}
	options := "noatime"
	if readOnly {
		// noload prevents journal replay, which would write to a device that
		// may be backed by shared immutable layers.
		options = "noatime,ro,noload"
	}
	if _, err := m.run(ctx, m.binaries.Mount, "-t", "ext4", "-o", options, devicePath, mountpoint); err != nil {
		return fmt.Errorf("mount %s at %s: %w", devicePath, mountpoint, err)
	}
	return nil
}

func (m *Manager) unmount(ctx context.Context, mountpoint string) error {
	if !isMountpoint(mountpoint) {
		return nil
	}
	if _, err := m.run(ctx, m.binaries.Umount, mountpoint); err != nil {
		return fmt.Errorf("unmount %s: %w", mountpoint, err)
	}
	return nil
}

// freezeFS blocks new writes and flushes dirty pages so the block device holds
// a consistent filesystem image. Returns a thaw function that must always run.
func (m *Manager) freezeFS(ctx context.Context, mountpoint string) (func(), error) {
	if _, err := m.run(ctx, m.binaries.Fsfreeze, "--freeze", mountpoint); err != nil {
		return nil, fmt.Errorf("freeze %s: %w", mountpoint, err)
	}
	return func() {
		// Thaw must not inherit a canceled snapshot context: an unfrozen
		// filesystem is strictly better than a wedged one.
		if _, err := m.run(context.Background(), m.binaries.Fsfreeze, "--unfreeze", mountpoint); err != nil {
			_, _ = m.run(context.Background(), m.binaries.Fsfreeze, "--unfreeze", mountpoint)
		}
	}, nil
}

func isMountpoint(path string) bool {
	target := filepath.Clean(path)
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 5 && unescapeMountPath(fields[4]) == target {
			return true
		}
	}
	return false
}

// unescapeMountPath decodes the octal escapes mountinfo uses for spaces etc.
func unescapeMountPath(path string) string {
	if !strings.Contains(path, `\`) {
		return path
	}
	var b strings.Builder
	for i := 0; i < len(path); i++ {
		if path[i] == '\\' && i+3 < len(path) {
			var c byte
			if _, err := fmt.Sscanf(path[i+1:i+4], "%03o", &c); err == nil {
				b.WriteByte(c)
				i += 3
				continue
			}
		}
		b.WriteByte(path[i])
	}
	return b.String()
}
