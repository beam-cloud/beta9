package agent

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type workerDirs struct {
	Slot         string
	Images       string
	Tmp          string
	Data         string
	Workspace    string
	Cache        string
	CacheMount   string
	CacheEnabled bool
	Checkpoints  string
	DurableDisk  string
	Logs         string
}

func (d workerDirs) All() []string {
	dirs := []string{
		d.Slot,
		d.Images,
		d.Tmp,
		d.Data,
		d.Workspace,
		d.Checkpoints,
		d.DurableDisk,
		d.Logs,
	}
	if d.CacheEnabled {
		dirs = append(dirs, d.Cache)
	}
	return dirs
}

func agentWorkerDirs(stateDir, cacheDir, workerID string) workerDirs {
	slotName := sanitizeDockerName(workerID)
	return workerDirs{
		Slot:         filepath.Join(stateDir, "slots", slotName),
		Images:       filepath.Join(stateDir, "images"),
		Tmp:          filepath.Join(stateDir, "tmp", slotName),
		Data:         filepath.Join(stateDir, "data"),
		Workspace:    filepath.Join(stateDir, "workspace-data"),
		Cache:        agentCacheDir(stateDir, cacheDir),
		CacheMount:   types.AgentCachePath,
		CacheEnabled: true,
		Checkpoints:  filepath.Join(stateDir, "checkpoints"),
		DurableDisk:  filepath.Join(stateDir, "durable-disks"),
		Logs:         filepath.Join(stateDir, "logs", slotName),
	}
}

// agentWorkerDirsForSlot overlays managed-pool host paths onto the
// installer-level directories. Empty pool paths deliberately retain the
// installer defaults, so existing private pools and older gateways are
// backward compatible.
func agentWorkerDirsForSlot(stateDir, cacheDir string, slot *pb.AgentWorkerSlot) workerDirs {
	dirs := agentWorkerDirs(stateDir, cacheDir, slot.GetWorkerId())
	config := slot.GetPoolConfig()
	if config == nil {
		return dirs
	}

	if path := strings.TrimSpace(config.GetStoragePath()); path != "" {
		storagePath := filepath.Clean(path)
		dirs.Data = storagePath
		dirs.Workspace = filepath.Join(storagePath, "workspace-data")
		dirs.Checkpoints = filepath.Join(storagePath, "checkpoints")
		if strings.TrimSpace(config.GetDurableDisksPath()) == "" {
			dirs.DurableDisk = filepath.Join(storagePath, "durable-disks")
		}
	}
	if path := strings.TrimSpace(config.GetImagesPath()); path != "" {
		dirs.Images = filepath.Clean(path)
	}
	if path := strings.TrimSpace(config.GetDurableDisksPath()); path != "" {
		dirs.DurableDisk = filepath.Clean(path)
	}
	if cache := config.GetCache(); cache != nil {
		dirs.CacheEnabled = cache.GetEnabled() && cache.GetDisk().GetEnabled()
		if disk := cache.GetDisk(); disk != nil {
			if path := strings.TrimSpace(disk.GetHostPath()); path != "" {
				dirs.Cache = filepath.Clean(path)
			}
			if path := strings.TrimSpace(disk.GetMountPath()); path != "" {
				dirs.CacheMount = filepath.Clean(path)
			}
		}
	}
	return dirs
}

func agentCacheDir(stateDir, cacheDir string) string {
	if dir := strings.TrimSpace(cacheDir); dir != "" {
		return filepath.Clean(dir)
	}
	if dir := strings.TrimSpace(os.Getenv(types.AgentCacheDirEnv)); dir != "" {
		return filepath.Clean(dir)
	}
	return filepath.Join(stateDir, "cache")
}
