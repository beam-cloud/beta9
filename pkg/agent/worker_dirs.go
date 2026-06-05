package agent

import "path/filepath"

type workerDirs struct {
	Slot        string
	Images      string
	Tmp         string
	Data        string
	Workspace   string
	Cache       string
	Checkpoints string
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
		d.Logs,
	}
}

func agentWorkerDirs(stateDir, workerID string) workerDirs {
	slotName := sanitizeDockerName(workerID)
	return workerDirs{
		Slot:        filepath.Join(stateDir, "slots", slotName),
		Images:      filepath.Join(stateDir, "images"),
		Tmp:         filepath.Join(stateDir, "tmp", slotName),
		Data:        filepath.Join(stateDir, "data"),
		Workspace:   filepath.Join(stateDir, "workspace-data"),
		Cache:       filepath.Join(stateDir, "cache"),
		Checkpoints: filepath.Join(stateDir, "checkpoints"),
		Logs:        filepath.Join(stateDir, "logs", slotName),
	}
}
