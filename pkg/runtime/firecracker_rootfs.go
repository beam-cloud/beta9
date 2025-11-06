package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// prepareRootfs prepares a block device for the rootfs
// Phase 1: Simple implementation using ext4 loop device
func (f *Firecracker) prepareRootfs(ctx context.Context, rootfsPath, vmDir string) (string, error) {
	// Check if rootfs directory exists
	if _, err := os.Stat(rootfsPath); err != nil {
		return "", fmt.Errorf("rootfs directory not found: %w", err)
	}

	// Calculate rootfs size
	size, err := f.calculateRootfsSize(rootfsPath)
	if err != nil {
		return "", fmt.Errorf("failed to calculate rootfs size: %w", err)
	}

	// Add 20% overhead for filesystem metadata and some working space
	size = size * 120 / 100
	if size < 64*1024*1024 {
		size = 64 * 1024 * 1024 // Minimum 64 MB
	}

	// Create sparse file for rootfs
	blockPath := filepath.Join(vmDir, "rootfs.ext4")
	if err := f.createSparseFile(blockPath, size); err != nil {
		return "", fmt.Errorf("failed to create sparse file: %w", err)
	}

	// Create ext4 filesystem
	if err := f.createExt4Filesystem(ctx, blockPath); err != nil {
		os.Remove(blockPath)
		return "", fmt.Errorf("failed to create ext4 filesystem: %w", err)
	}

	// Mount and copy rootfs
	if err := f.populateRootfs(ctx, blockPath, rootfsPath); err != nil {
		os.Remove(blockPath)
		return "", fmt.Errorf("failed to populate rootfs: %w", err)
	}

	return blockPath, nil
}

// calculateRootfsSize calculates the size of the rootfs directory
func (f *Firecracker) calculateRootfsSize(rootfsPath string) (int64, error) {
	cmd := exec.Command("du", "-sb", rootfsPath)
	output, err := cmd.Output()
	if err != nil {
		// Fallback to a reasonable default if du fails
		return 512 * 1024 * 1024, nil // 512 MB
	}

	var size int64
	_, err = fmt.Sscanf(string(output), "%d", &size)
	if err != nil {
		return 512 * 1024 * 1024, nil // 512 MB fallback
	}

	return size, nil
}

// createSparseFile creates a sparse file of the given size
func (f *Firecracker) createSparseFile(path string, size int64) error {
	// Use truncate to create sparse file
	cmd := exec.Command("truncate", "-s", fmt.Sprintf("%d", size), path)
	if err := cmd.Run(); err != nil {
		// Fallback: create file manually
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()

		if err := file.Truncate(size); err != nil {
			return err
		}
	}

	return nil
}

// createExt4Filesystem creates an ext4 filesystem on the given block device
func (f *Firecracker) createExt4Filesystem(ctx context.Context, blockPath string) error {
	// Use mkfs.ext4 to create filesystem
	// -F: force creation even on a file
	// -q: quiet mode
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-F", "-q", blockPath)
	
	if f.cfg.Debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mkfs.ext4 failed: %w", err)
	}

	return nil
}

// populateRootfs mounts the ext4 image and copies the rootfs contents
func (f *Firecracker) populateRootfs(ctx context.Context, blockPath, rootfsPath string) error {
	// Create temporary mount point
	mountPoint, err := os.MkdirTemp("", "beta9-rootfs-*")
	if err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)

	// Mount the ext4 image
	cmd := exec.CommandContext(ctx, "mount", "-o", "loop", blockPath, mountPoint)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to mount rootfs image: %w", err)
	}
	defer func() {
		// Unmount when done
		exec.Command("umount", mountPoint).Run()
	}()

	// Copy rootfs contents using cp or rsync
	// Using cp is simpler and more portable
	cpCmd := exec.CommandContext(ctx, "cp", "-a", rootfsPath+"/.", mountPoint+"/")
	if err := cpCmd.Run(); err != nil {
		// Try with rsync as fallback
		rsyncCmd := exec.CommandContext(ctx, "rsync", "-a", rootfsPath+"/", mountPoint+"/")
		if err := rsyncCmd.Run(); err != nil {
			return fmt.Errorf("failed to copy rootfs contents: %w", err)
		}
	}

	// Ensure beta9-vm-init is present and executable
	initPath := filepath.Join(mountPoint, "sbin", "beta9-vm-init")
	if _, err := os.Stat(initPath); err != nil {
		// Log warning but don't fail - the init might be elsewhere or missing
		if f.cfg.Debug {
			fmt.Fprintf(os.Stderr, "Warning: beta9-vm-init not found at %s\n", initPath)
		}
	} else {
		// Ensure it's executable
		if err := os.Chmod(initPath, 0755); err != nil {
			return fmt.Errorf("failed to make init executable: %w", err)
		}
	}

	return nil
}

// Phase 2 TODO: Lazy block device support
// This would integrate with a block device service that provides
// lazy loading from CAS/FUSE storage. For now, we use the simple
// ext4 image approach which is reliable and easy to debug.
