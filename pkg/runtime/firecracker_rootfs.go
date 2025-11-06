package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	
	"github.com/opencontainers/runtime-spec/specs-go"
)

// prepareRootfs prepares a block device for the rootfs
// Phase 1: Simple implementation using ext4 loop device
// It also handles copying mounts from the OCI spec into the rootfs
func (f *Firecracker) prepareRootfs(ctx context.Context, rootfsPath, vmDir string, spec *specs.Spec) (string, error) {
	// Check if rootfs directory exists
	if _, err := os.Stat(rootfsPath); err != nil {
		return "", fmt.Errorf("rootfs directory not found: %w", err)
	}

	// Calculate rootfs size (including mounts)
	size, err := f.calculateRootfsSize(rootfsPath)
	if err != nil {
		return "", fmt.Errorf("failed to calculate rootfs size: %w", err)
	}

	// Also calculate size of mounts that need to be copied in
	if spec != nil && spec.Mounts != nil {
		for _, mount := range spec.Mounts {
			// Skip virtual filesystems that don't need copying
			if mount.Type == "proc" || mount.Type == "sysfs" || 
			   mount.Type == "devpts" || mount.Type == "tmpfs" ||
			   mount.Type == "cgroup" || mount.Type == "cgroup2" {
				continue
			}
			
			// For bind mounts, calculate the source size
			if mount.Type == "bind" || mount.Type == "" {
				if mount.Source != "" {
					if info, err := os.Stat(mount.Source); err == nil {
						if info.IsDir() {
							if dirSize, err := f.calculateRootfsSize(mount.Source); err == nil {
								size += dirSize
							}
						} else {
							size += info.Size()
						}
					}
				}
			}
		}
	}

	// Add 30% overhead for filesystem metadata, VM init, and working space
	size = size * 130 / 100
	if size < 128*1024*1024 {
		size = 128 * 1024 * 1024 // Minimum 128 MB
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

	// Mount and copy rootfs (including mounts from spec)
	if err := f.populateRootfs(ctx, blockPath, rootfsPath, spec); err != nil {
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
// It also copies any bind mounts from the OCI spec into the appropriate locations
func (f *Firecracker) populateRootfs(ctx context.Context, blockPath, rootfsPath string, spec *specs.Spec) error {
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

	// Copy bind mounts from OCI spec into the rootfs
	// This is necessary because microVMs can't use bind mounts like containers
	if spec != nil && spec.Mounts != nil {
		for _, mount := range spec.Mounts {
			// Skip virtual filesystems - these will be handled by the guest init
			if mount.Type == "proc" || mount.Type == "sysfs" || 
			   mount.Type == "devpts" || mount.Type == "tmpfs" ||
			   mount.Type == "cgroup" || mount.Type == "cgroup2" {
				continue
			}
			
			// Handle bind mounts by copying source to destination
			if (mount.Type == "bind" || mount.Type == "") && mount.Source != "" {
				// Create destination directory in rootfs
				destPath := filepath.Join(mountPoint, mount.Destination)
				if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
					return fmt.Errorf("failed to create mount destination directory: %w", err)
				}
				
				// Check if source exists
				if _, err := os.Stat(mount.Source); err != nil {
					// Source doesn't exist - create empty directory
					if err := os.MkdirAll(destPath, 0755); err != nil {
						return fmt.Errorf("failed to create empty mount point: %w", err)
					}
					continue
				}
				
				// Copy source to destination
				// Use cp -a to preserve permissions and attributes
				cpMountCmd := exec.CommandContext(ctx, "cp", "-a", mount.Source, destPath)
				if err := cpMountCmd.Run(); err != nil {
					// If cp fails, try rsync
					rsyncMountCmd := exec.CommandContext(ctx, "rsync", "-a", mount.Source+"/", destPath+"/")
					if err := rsyncMountCmd.Run(); err != nil {
						return fmt.Errorf("failed to copy mount %s to %s: %w", mount.Source, destPath, err)
					}
				}
				
				if f.cfg.Debug {
					fmt.Fprintf(os.Stderr, "Copied mount: %s -> %s\n", mount.Source, mount.Destination)
				}
			}
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
