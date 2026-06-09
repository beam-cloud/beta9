package compute

import (
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type PoolState struct {
	WorkspaceID          string         `json:"workspace_id,omitempty"`
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
	MachineID        string    `json:"machine_id,omitempty"`
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
	Metrics                   AgentMachineMetrics   `json:"metrics"`
	CreatedAt                 time.Time             `json:"created_at"`
	LastJoinAt                time.Time             `json:"last_join_at"`
	LastHeartbeatAt           time.Time             `json:"last_heartbeat_at"`
	LastDisconnectAt          time.Time             `json:"last_disconnect_at"`
}

type AgentMachineMetrics struct {
	Timestamp            time.Time `json:"timestamp"`
	CPUUtilizationPct    float32   `json:"cpu_utilization_pct"`
	MemoryUsedMB         uint64    `json:"memory_used_mb"`
	MemoryTotalMB        uint64    `json:"memory_total_mb"`
	MemoryUtilizationPct float32   `json:"memory_utilization_pct"`
	DiskUsedMB           uint64    `json:"disk_used_mb"`
	DiskTotalMB          uint64    `json:"disk_total_mb"`
	DiskUsagePct         float32   `json:"disk_usage_pct"`
	DiskPath             string    `json:"disk_path"`
	WorkerCount          uint32    `json:"worker_count"`
	ContainerCount       uint32    `json:"container_count"`
	FreeGPUCount         uint32    `json:"free_gpu_count"`
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

const AgentHeartbeatTimeout = 30 * time.Second

func AgentMachineStatus(state *AgentTokenState, now time.Time) string {
	if AgentMachineConnected(state, now) {
		return types.AgentMachineStatusSchedulable
	}
	if state != nil && !state.Schedulable && hasPreflightFailure(state.Preflight) {
		return types.AgentMachineStatusPreflightFail
	}
	return types.AgentMachineStatusDisconnected
}

func AgentMachineConnected(state *AgentTokenState, now time.Time) bool {
	if state == nil || !state.Schedulable {
		return false
	}
	lastSeen := AgentMachineLastSeen(state)
	if lastSeen.IsZero() {
		return false
	}
	if !state.LastDisconnectAt.IsZero() && !state.LastDisconnectAt.Before(lastSeen) {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if lastSeen.After(now) {
		return false
	}
	return now.Sub(lastSeen) <= AgentHeartbeatTimeout
}

func AgentMachineLastSeen(state *AgentTokenState) time.Time {
	if state == nil {
		return time.Time{}
	}
	if state.LastHeartbeatAt.After(state.LastJoinAt) {
		return state.LastHeartbeatAt
	}
	return state.LastJoinAt
}

func hasPreflightFailure(checks []PreflightCheckState) bool {
	for _, check := range checks {
		if check.Severity == "error" && !check.OK {
			return true
		}
	}
	return false
}
