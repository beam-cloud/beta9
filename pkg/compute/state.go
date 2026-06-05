package compute

import (
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

type PoolState struct {
	Name                 string         `json:"name"`
	Selector             string         `json:"selector"`
	Config               *pb.PoolConfig `json:"config"`
	Reservations         []Reservation  `json:"reservations"`
	ReservedGPUs         uint32         `json:"reserved_gpus"`
	CommittedSpendMicros int64          `json:"committed_spend_micros"`
	Status               string         `json:"status"`
	Source               CapacitySource `json:"source"`
	Mode                 string         `json:"mode"`
	Transport            string         `json:"transport"`
	Fallback             string         `json:"fallback"`
	Priority             int32          `json:"priority"`
	CreatedByTokenID     string         `json:"created_by_token_id"`
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
	ExpiresAt            time.Time      `json:"expires_at"`
}

type JoinTokenState struct {
	TokenHash        string    `json:"token_hash"`
	WorkspaceID      string    `json:"workspace_id"`
	PoolName         string    `json:"pool_name"`
	CreatedByTokenID string    `json:"created_by_token_id"`
	CreatedAt        time.Time `json:"created_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	Revoked          bool      `json:"revoked"`
}

type AgentTokenState struct {
	TokenHash                 string                `json:"token_hash"`
	WorkspaceID               string                `json:"workspace_id"`
	PoolName                  string                `json:"pool_name"`
	MachineID                 string                `json:"machine_id"`
	MachineFingerprint        string                `json:"machine_fingerprint"`
	Hostname                  string                `json:"hostname"`
	OS                        string                `json:"os"`
	Arch                      string                `json:"arch"`
	CPUCount                  uint32                `json:"cpu_count"`
	CPUMillicores             int64                 `json:"cpu_millicores"`
	MemoryMB                  uint64                `json:"memory_mb"`
	GPUs                      []string              `json:"gpus"`
	GPUIDs                    []string              `json:"gpu_ids"`
	GPUCount                  uint32                `json:"gpu_count"`
	Executor                  string                `json:"executor"`
	NetworkSlotPoolSize       uint32                `json:"network_slot_pool_size"`
	ContainerStartConcurrency uint32                `json:"container_start_concurrency"`
	Schedulable               bool                  `json:"schedulable"`
	Preflight                 []PreflightCheckState `json:"preflight"`
	CreatedAt                 time.Time             `json:"created_at"`
	LastJoinAt                time.Time             `json:"last_join_at"`
}

type AgentWorkerSlotState struct {
	WorkerID                  string    `json:"worker_id"`
	WorkerTokenID             string    `json:"worker_token_id"`
	WorkerTokenHash           string    `json:"worker_token_hash"`
	WorkspaceID               string    `json:"workspace_id"`
	PoolName                  string    `json:"pool_name"`
	MachineID                 string    `json:"machine_id"`
	CPU                       int64     `json:"cpu"`
	Memory                    int64     `json:"memory"`
	GPU                       string    `json:"gpu"`
	GPUCount                  uint32    `json:"gpu_count"`
	GPUAssignment             string    `json:"gpu_assignment"`
	NetworkPrefix             string    `json:"network_prefix"`
	WorkerImage               string    `json:"worker_image"`
	NetworkSlotPoolSize       uint32    `json:"network_slot_pool_size"`
	ContainerStartConcurrency uint32    `json:"container_start_concurrency"`
	CreatedAt                 time.Time `json:"created_at"`
	UpdatedAt                 time.Time `json:"updated_at"`
}

type PreflightCheckState struct {
	Name     string `json:"name"`
	OK       bool   `json:"ok"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}
