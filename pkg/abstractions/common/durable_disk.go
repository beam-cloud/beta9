package abstractions

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
)

type DurableDiskPlacementRepos struct {
	BackendRepo    repository.BackendRepository
	ComputeRepo    repository.ComputeRepository
	WorkerRepo     repository.WorkerRepository
	WorkerPoolRepo repository.WorkerPoolRepository
}

func ConfigureDurableDiskPlacement(_ context.Context, _ DurableDiskPlacementRepos, _ *types.Workspace, stubConfig *types.StubConfigV1) error {
	if stubConfig == nil {
		return nil
	}
	if err := enforceDurableDiskSingleWriter(stubConfig); err != nil {
		return err
	}
	seenNames := make(map[string]struct{}, len(stubConfig.Disks))
	seenMounts := make(map[string]struct{}, len(stubConfig.Disks))
	for _, disk := range stubConfig.Disks {
		if disk == nil {
			continue
		}
		disk.Name = types.SafeDurableDiskName(disk.Name)
		if disk.Name == "root" {
			return fmt.Errorf("durable disk name root is reserved for persistent root state")
		}
		disk.Size = strings.TrimSpace(disk.Size)
		disk.MountPath = filepath.Clean(strings.TrimSpace(disk.MountPath))
		disk.SourceGenerationId = strings.TrimSpace(disk.SourceGenerationId)
		if disk.SourceGenerationId != "" {
			if _, err := uuid.Parse(disk.SourceGenerationId); err != nil {
				return fmt.Errorf("durable disk %q source_generation_id must be an RFC4122 UUID", disk.Name)
			}
		}
		if disk.Size == "" {
			return fmt.Errorf("durable disk %q size is required", disk.Name)
		}
		if !filepath.IsAbs(disk.MountPath) || disk.MountPath == "/" {
			return fmt.Errorf("durable disk %q mount_path must be an absolute directory below root", disk.Name)
		}
		if _, ok := seenNames[disk.Name]; ok {
			return fmt.Errorf("durable disk name %q is duplicated", disk.Name)
		}
		if _, ok := seenMounts[disk.MountPath]; ok {
			return fmt.Errorf("durable disk mount_path %q is duplicated", disk.MountPath)
		}
		seenNames[disk.Name] = struct{}{}
		seenMounts[disk.MountPath] = struct{}{}
	}
	return nil
}

func enforceDurableDiskSingleWriter(stubConfig *types.StubConfigV1) error {
	if durableDisksReadOnly(stubConfig.Disks) {
		return nil
	}
	maxContainers := uint(1)
	minContainers := uint(0)
	if stubConfig.Autoscaler != nil {
		minContainers = stubConfig.Autoscaler.MinContainers
		if stubConfig.Autoscaler.MaxContainers > 0 {
			maxContainers = stubConfig.Autoscaler.MaxContainers
		}
	}
	if minContainers > 1 || maxContainers > 1 {
		return fmt.Errorf("writable durable disks support one container; set max containers to 1 or mark every disk read_only")
	}
	return nil
}

func durableDisksReadOnly(disks []*pb.DurableDisk) bool {
	for _, disk := range disks {
		if disk != nil && !disk.ReadOnly {
			return false
		}
	}
	return true
}

// State volumes do not silently rewrite compute placement. A caller must
// explicitly choose another pool if the selected private pool is unavailable.
func ConfigureUnavailablePrivatePoolFallback(_ context.Context, _ DurableDiskPlacementRepos, _ *types.Workspace, _ *types.StubConfigV1) error {
	return nil
}
