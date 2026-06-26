package worker

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	durableDiskDriverEnv    = "BETA9_DURABLE_DISK_DRIVER"
	durableDiskMarkerFile   = ".beta9-durable-disk"
	durableDiskReplicaDir   = ".beta9-durable-disk-replicas"
	defaultDevReplicaCopies = 3
)

func (s *Worker) prepareDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if mount == nil || mount.DurableDisk == nil {
		return fmt.Errorf("durable disk mount is missing metadata")
	}
	if mount.LocalPath == "" {
		return fmt.Errorf("durable disk %q has no local path", mount.DurableDisk.Name)
	}

	driver := durableDiskDriver(mount.DurableDisk.Driver)
	switch driver {
	case "", "dev":
		return prepareDevDurableDiskMount(mount)
	case "drbd":
		return s.prepareDRBDDurableDiskMount(request, mount)
	default:
		return fmt.Errorf("durable disk %q requested unsupported driver %q", mount.DurableDisk.Name, driver)
	}
}

func durableDiskDriver(configured string) string {
	if configured = strings.TrimSpace(configured); configured != "" {
		return strings.ToLower(configured)
	}
	return strings.ToLower(strings.TrimSpace(os.Getenv(durableDiskDriverEnv)))
}

func prepareDevDurableDiskMount(mount *types.Mount) error {
	if err := restoreDevDurableDiskPrimary(mount); err != nil {
		return err
	}
	if err := os.MkdirAll(mount.LocalPath, 0755); err != nil {
		return fmt.Errorf("create dev durable disk path %s: %w", mount.LocalPath, err)
	}
	if err := writeDevDurableDiskMarker(mount.LocalPath, mount); err != nil {
		return err
	}

	return ensureDevDurableDiskReplicas(mount)
}

func (s *Worker) syncDurableDiskMounts(request *types.ContainerRequest) {
	if request == nil {
		return
	}

	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount == nil || mount.DurableDisk == nil {
			continue
		}
		if durableDiskDriver(mount.DurableDisk.Driver) != "dev" {
			continue
		}
		if err := syncDevDurableDiskMount(mount); err != nil {
			log.Warn().Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).Err(err).Msg("failed to sync dev durable disk replicas")
		}
	}
}

func restoreDevDurableDiskPrimary(mount *types.Mount) error {
	if devDurableDiskHasPayload(mount.LocalPath) {
		return nil
	}

	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if !devDurableDiskHasPayload(replicaPath) {
			continue
		}

		if err := os.RemoveAll(mount.LocalPath); err != nil {
			return fmt.Errorf("clear dev durable disk primary %s: %w", mount.LocalPath, err)
		}
		if err := copyDurableDiskDir(replicaPath, mount.LocalPath); err != nil {
			return fmt.Errorf("restore dev durable disk primary %s from replica %s: %w", mount.LocalPath, replicaPath, err)
		}
		return writeDevDurableDiskMarker(mount.LocalPath, mount)
	}

	return nil
}

func ensureDevDurableDiskReplicas(mount *types.Mount) error {
	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if devDurableDiskHasPayload(mount.LocalPath) {
			if err := os.MkdirAll(replicaPath, 0755); err != nil {
				return fmt.Errorf("create dev durable disk replica %s: %w", replicaPath, err)
			}
			if err := writeDevDurableDiskMarker(replicaPath, mount); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(replicaPath, 0755); err != nil {
			return fmt.Errorf("create dev durable disk replica %s: %w", replicaPath, err)
		}
		if err := writeDevDurableDiskMarker(replicaPath, mount); err != nil {
			return err
		}
	}
	return nil
}

func syncDevDurableDiskMount(mount *types.Mount) error {
	if !devDurableDiskHasPayload(mount.LocalPath) {
		return nil
	}

	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if err := copyDurableDiskDirAtomic(mount.LocalPath, replicaPath); err != nil {
			return fmt.Errorf("sync dev durable disk replica %s: %w", replicaPath, err)
		}
	}
	return nil
}

func devDurableDiskReplicaPaths(mount *types.Mount) []string {
	if mount == nil || mount.DurableDisk == nil {
		return nil
	}

	replicas := int(mount.DurableDisk.Replicas)
	if replicas == 0 {
		replicas = defaultDevReplicaCopies
	}
	if replicas <= 1 {
		return nil
	}

	base := filepath.Base(filepath.Clean(mount.LocalPath))
	root := filepath.Join(filepath.Dir(filepath.Clean(mount.LocalPath)), durableDiskReplicaDir, base)
	paths := make([]string, 0, replicas-1)
	for replica := 1; replica < replicas; replica++ {
		paths = append(paths, filepath.Join(root, fmt.Sprintf("replica-%d", replica)))
	}
	return paths
}

func devDurableDiskHasPayload(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.Name() == durableDiskMarkerFile {
			continue
		}
		return true
	}
	return false
}

func writeDevDurableDiskMarker(path string, mount *types.Mount) error {
	replicas := uint32(defaultDevReplicaCopies)
	if mount != nil && mount.DurableDisk != nil && mount.DurableDisk.Replicas > 0 {
		replicas = mount.DurableDisk.Replicas
	}

	marker := fmt.Sprintf("driver=dev\nreplicas=%d\n", replicas)
	if err := os.WriteFile(filepath.Join(path, durableDiskMarkerFile), []byte(marker), 0644); err != nil {
		return fmt.Errorf("write dev durable disk marker %s: %w", path, err)
	}
	return nil
}

func copyDurableDiskDirAtomic(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	suffix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	tmp := fmt.Sprintf("%s.tmp.%s", dst, suffix)
	backup := fmt.Sprintf("%s.old.%s", dst, suffix)
	movedExisting := false
	defer os.RemoveAll(tmp)

	if err := copyDurableDiskDir(src, tmp); err != nil {
		return err
	}

	if _, err := os.Lstat(dst); err == nil {
		if err := os.Rename(dst, backup); err != nil {
			return err
		}
		movedExisting = true
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(tmp, dst); err != nil {
		if movedExisting {
			_ = os.Rename(backup, dst)
		}
		return err
	}

	if movedExisting {
		_ = os.RemoveAll(backup)
	}
	return nil
}

func copyDurableDiskDir(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", src)
	}
	if err := os.MkdirAll(dst, info.Mode().Perm()); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		info, err := os.Lstat(srcPath)
		if err != nil {
			return err
		}

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(srcPath)
			if err != nil {
				return err
			}
			if err := os.Symlink(target, dstPath); err != nil {
				return err
			}
		case info.IsDir():
			if err := copyDurableDiskDir(srcPath, dstPath); err != nil {
				return err
			}
		case info.Mode().IsRegular():
			if err := copyDurableDiskFile(srcPath, dstPath, info.Mode().Perm()); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyDurableDiskFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Worker) prepareDRBDDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if _, err := exec.LookPath("drbdadm"); err != nil {
		return fmt.Errorf("durable disk %q requires DRBD, but drbdadm is not available on this worker: %w", mount.DurableDisk.Name, err)
	}

	return fmt.Errorf("durable disk %q requested DRBD, but embedded DRBD orchestration is not enabled on this worker", mount.DurableDisk.Name)
}
