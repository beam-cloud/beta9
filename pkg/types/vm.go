package types

import "time"

// VMSpec is CPU-only. Unknown fields (including GPU options) are rejected by
// the API rather than silently changing the isolation or placement contract.
type VMSpec struct {
	ImageID          string   `json:"image_id"`
	CPU              int64    `json:"cpu"`    // millicores
	Memory           int64    `json:"memory"` // MiB
	DiskSize         string   `json:"disk_size"`
	Pool             string   `json:"pool,omitempty"`
	Env              []string `json:"env,omitempty"`
	Secrets          []string `json:"secrets,omitempty"`
	Ports            []uint32 `json:"ports,omitempty"`
	PrivatePorts     []uint32 `json:"private_ports,omitempty"`
	Desktop          bool     `json:"desktop"`
	DockerEnabled    bool     `json:"docker_enabled"`
	SSH              bool     `json:"ssh"`
	SSHPublicKey     string   `json:"ssh_public_key,omitempty"`
	IdleTimeout      int64    `json:"idle_timeout"`
	SourceSnapshotID string   `json:"source_snapshot_id,omitempty"`
}

type VM struct {
	ID                  string            `json:"id"`
	WorkspaceID         uint              `json:"-"`
	WorkspaceExternalID string            `json:"-"`
	TokenID             string            `json:"-"`
	Name                string            `json:"name"`
	Handle              string            `json:"handle"`
	Spec                VMSpec            `json:"spec"`
	StubID              string            `json:"stub_id,omitempty"`
	ContainerID         string            `json:"container_id,omitempty"`
	DesiredState        string            `json:"desired_state"`
	Status              string            `json:"status"`
	Error               string            `json:"error,omitempty"`
	Generation          int64             `json:"generation"`
	LaunchAttempts      int               `json:"launch_attempts"`
	EverRunning         bool              `json:"ever_running"`
	RootSnapshotID      string            `json:"root_snapshot_id,omitempty"`
	StopSnapshotID      string            `json:"stop_snapshot_id,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	LastActiveAt        time.Time         `json:"last_active_at"`
	TerminalURL         string            `json:"terminal_url,omitempty"`
	DesktopURL          string            `json:"desktop_url,omitempty"`
	URLs                map[uint32]string `json:"urls"`
}

// VMArtifact references existing immutable qcow snapshot data; it never owns
// the source VM's disk. Templates survive removal of the source VM.
type VMArtifact struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	VMID           string    `json:"vm_id"`
	Kind           string    `json:"kind"`
	RootSnapshotID string    `json:"root_snapshot_id"`
	Spec           VMSpec    `json:"spec"`
	CreatedAt      time.Time `json:"created_at"`
	Description    string    `json:"description,omitempty"`
}
