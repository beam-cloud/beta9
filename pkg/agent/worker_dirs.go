package agent

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

type workerDirs struct {
	Slot        string
	Images      string
	Tmp         string
	Data        string
	Workspace   string
	Cache       string
	Checkpoints string
	DurableDisk string
	Logs        string
}

func (d workerDirs) All() []string {
	return []string{
		d.Slot,
		d.Images,
		d.Tmp,
		d.Data,
		d.Workspace,
		d.Cache,
		d.Checkpoints,
		d.DurableDisk,
		d.Logs,
	}
}

func agentWorkerDirs(stateDir, cacheDir, workerID string) workerDirs {
	slotName := sanitizeDockerName(workerID)
	return workerDirs{
		Slot:        filepath.Join(stateDir, "slots", slotName),
		Images:      filepath.Join(stateDir, "images"),
		Tmp:         filepath.Join(stateDir, "tmp", slotName),
		Data:        filepath.Join(stateDir, "data"),
		Workspace:   filepath.Join(stateDir, "workspace-data"),
		Cache:       agentCacheDir(stateDir, cacheDir),
		Checkpoints: filepath.Join(stateDir, "checkpoints"),
		DurableDisk: filepath.Join(stateDir, "durable-disks"),
		Logs:        filepath.Join(stateDir, "logs", slotName),
	}
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
