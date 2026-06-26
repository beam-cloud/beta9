package worker

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

const durableDiskDriverEnv = "BETA9_DURABLE_DISK_DRIVER"

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
	if err := os.MkdirAll(mount.LocalPath, 0755); err != nil {
		return fmt.Errorf("create dev durable disk path %s: %w", mount.LocalPath, err)
	}

	return os.WriteFile(
		filepath.Join(mount.LocalPath, ".beta9-durable-disk"),
		[]byte("driver=dev\n"),
		0644,
	)
}

func (s *Worker) prepareDRBDDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if _, err := exec.LookPath("drbdadm"); err != nil {
		return fmt.Errorf("durable disk %q requires DRBD, but drbdadm is not available on this worker: %w", mount.DurableDisk.Name, err)
	}

	return fmt.Errorf("durable disk %q requested DRBD, but embedded DRBD orchestration is not enabled on this worker", mount.DurableDisk.Name)
}
