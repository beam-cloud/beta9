package compute

import (
	"errors"
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
	ReservedNodes        uint32         `json:"reserved_nodes"`
	CommittedSpendMicros int64          `json:"committed_spend_micros"`
	Status               string         `json:"status"`
	Source               CapacitySource `json:"source"`
	Mode                 string         `json:"mode"`
	Transport            string         `json:"transport"`
	Fallback             string         `json:"fallback"`
	Priority             int32          `json:"priority"`
	Preemptible          bool           `json:"preemptible,omitempty"`
	MarketplaceListingID string         `json:"marketplace_listing_id,omitempty"`
	SellerWorkspaceID    string         `json:"seller_workspace_id,omitempty"`
	CreatedByTokenID     string         `json:"created_by_token_id"`
	CreatedAt            time.Time      `json:"created_at"`
	UpdatedAt            time.Time      `json:"updated_at"`
	ExpiresAt            time.Time      `json:"expires_at"`
	// BillingDegradedSince marks when balance checks started failing;
	// reservations terminate once the grace period is exceeded.
	BillingDegradedSince time.Time          `json:"billing_degraded_since,omitempty"`
	BYOC                 *BYOCProviderState `json:"byoc,omitempty"`
	// ManagementSource marks serverless inventory owned by the control plane
	// rather than a tenant. WorkerConfig preserves its complete configuration.
	ManagementSource types.WorkerPoolManagementSource `json:"management_source,omitempty"`
	// ManagedInstanceID changes whenever a deleted managed pool is
	// recreated, preventing an unexpired installer for the old pool from
	// authorizing a machine into the new one.
	ManagedInstanceID string                  `json:"managed_instance_id,omitempty"`
	WorkerConfig      *types.WorkerPoolConfig `json:"worker_config,omitempty"`
}

var (
	ErrManagedPermissionDenied  = errors.New("cluster admin permission required")
	ErrManagedPoolConflict      = errors.New("pool already exists")
	ErrManagedPoolNotFound      = errors.New("pool not found")
	ErrManagedPoolImmutable     = errors.New("config-managed pools are read-only")
	ErrManagedPoolInUse         = errors.New("pool must have no machines or workers")
	ErrInvalidManagedPoolConfig = errors.New("invalid pool configuration")
)

type ManagedPool struct {
	Name              string                           `json:"name"`
	Config            types.WorkerPoolConfig           `json:"config"`
	Source            types.WorkerPoolManagementSource `json:"source"`
	Controller        types.WorkerPoolController       `json:"controller"`
	State             *types.WorkerPoolState           `json:"-"` // Used by the admin CLI, not the HTTP API.
	MachineCount      int                              `json:"machine_count"`
	ReadyMachineCount int                              `json:"ready_machine_count"`
}

const (
	MarketplaceListingStatusActive   = "active"
	MarketplaceListingStatusInactive = "inactive"
)

// MarketplaceRentalState is a buyer's exclusive hold on GPUs of one seller
// machine. Rented GPUs are invisible to serverless marketplace scheduling;
// only the buyer's machine-pinned workloads consume them.
type MarketplaceRentalState struct {
	ID                string `json:"id"`
	BuyerWorkspaceID  string `json:"buyer_workspace_id"`
	SellerWorkspaceID string `json:"seller_workspace_id"`
	ListingID         string `json:"listing_id"`
	PoolName          string `json:"pool_name"`
	MachineID         string `json:"machine_id"`
	GPU               string `json:"gpu"`
	GPUCount          uint32 `json:"gpu_count"`
	// PricePerGPUHourCents is snapshotted from the listing at reserve time so
	// seller price changes never reprice a rental already held.
	PricePerGPUHourCents uint32    `json:"price_per_gpu_hour_cents,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
	// LastBilledAt is the end of the last rental usage interval the gateway
	// emitted; rentals bill wall-clock while held, including idle time.
	LastBilledAt time.Time `json:"last_billed_at,omitempty"`
}

type MarketplaceListingState struct {
	ID                string `json:"id"`
	SellerWorkspaceID string `json:"seller_workspace_id"`
	DisplayName       string `json:"display_name"`
	GPU               string `json:"gpu"`
	GPUCount          uint32 `json:"gpu_count"`
	Source            string `json:"source"`
	Preemptible       bool   `json:"preemptible"`
	Public            bool   `json:"public"`
	Status            string `json:"status"`
	PoolName          string `json:"pool_name"`
	Region            string `json:"region,omitempty"` // seller-declared, e.g. "us-east"
	// PricePerGPUHourCents is the seller-set on-demand rate, per GPU per hour.
	PricePerGPUHourCents uint32    `json:"price_per_gpu_hour_cents,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

type BYOCProviderState struct {
	Provider     string            `json:"provider,omitempty"`
	AccountID    string            `json:"account_id,omitempty"`
	Region       string            `json:"region,omitempty"`
	ResourceName string            `json:"resource_name,omitempty"`
	ResourceURL  string            `json:"resource_url,omitempty"`
	DestroyURL   string            `json:"destroy_url,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

type JoinTokenState struct {
	TokenHash            string    `json:"token_hash"`
	WorkspaceID          string    `json:"workspace_id"`
	PoolName             string    `json:"pool_name"`
	MachineID            string    `json:"machine_id,omitempty"`
	CreatedByTokenID     string    `json:"created_by_token_id"`
	CreatedAt            time.Time `json:"created_at"`
	Mode                 string    `json:"mode,omitempty"`
	MarketplaceListingID string    `json:"marketplace_listing_id,omitempty"`
	SellerWorkspaceID    string    `json:"seller_workspace_id,omitempty"`
	// A zero ExpiresAt is a persistent bootstrap token; it must be explicitly
	// revoked when the owning resource is deleted.
	ExpiresAt time.Time `json:"expires_at"`
	Revoked   bool      `json:"revoked"`
	// BoundFingerprint pins a machine-specific join token to the first
	// machine that used it.
	BoundFingerprint      string `json:"bound_fingerprint,omitempty"`
	ManagedPoolInstanceID string `json:"managed_pool_instance_id,omitempty"`
}

type AgentTokenState struct {
	TokenHash                 string                `json:"token_hash"`
	WorkspaceID               string                `json:"workspace_id"`
	PoolName                  string                `json:"pool_name"`
	Mode                      string                `json:"mode,omitempty"`
	MarketplaceListingID      string                `json:"marketplace_listing_id,omitempty"`
	SellerWorkspaceID         string                `json:"seller_workspace_id,omitempty"`
	ManagedPoolInstanceID     string                `json:"managed_pool_instance_id,omitempty"`
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
	WorkerImageOverride       string                `json:"worker_image_override,omitempty"`
	Cordoned                  bool                  `json:"cordoned,omitempty"`
	Schedulable               bool                  `json:"schedulable"`
	AvailabilityReason        string                `json:"availability_reason,omitempty"`
	AvailabilityUpdatedAt     time.Time             `json:"availability_updated_at,omitempty"`
	Preflight                 []PreflightCheckState `json:"preflight"`
	Metrics                   AgentMachineMetrics   `json:"metrics"`
	CreatedAt                 time.Time             `json:"created_at"`
	LastJoinAt                time.Time             `json:"last_join_at"`
	LastHeartbeatAt           time.Time             `json:"last_heartbeat_at"`
	LastDisconnectAt          time.Time             `json:"last_disconnect_at"`
	BillingCursorAt           time.Time             `json:"billing_cursor_at,omitempty"`
}

type AgentMachineMetrics struct {
	Timestamp            time.Time         `json:"timestamp"`
	CPUUtilizationPct    float32           `json:"cpu_utilization_pct"`
	MemoryUsedMB         uint64            `json:"memory_used_mb"`
	MemoryTotalMB        uint64            `json:"memory_total_mb"`
	MemoryUtilizationPct float32           `json:"memory_utilization_pct"`
	DiskUsedMB           uint64            `json:"disk_used_mb"`
	DiskTotalMB          uint64            `json:"disk_total_mb"`
	DiskUsagePct         float32           `json:"disk_usage_pct"`
	DiskPath             string            `json:"disk_path"`
	WorkerCount          uint32            `json:"worker_count"`
	ContainerCount       uint32            `json:"container_count"`
	FreeGPUCount         uint32            `json:"free_gpu_count"`
	PathMetrics          []AgentPathMetric `json:"path_metrics,omitempty"`
}

type AgentPathMetric struct {
	Label       string  `json:"label"`
	Path        string  `json:"path"`
	UsedMB      uint64  `json:"used_mb"`
	TotalMB     uint64  `json:"total_mb"`
	AvailableMB uint64  `json:"available_mb"`
	UsagePct    float32 `json:"usage_pct"`
}

type AgentWorkerSlotState struct {
	Generation                string              `json:"generation"`
	WorkerID                  string              `json:"worker_id"`
	WorkerTokenID             string              `json:"worker_token_id"`
	WorkerTokenHash           string              `json:"worker_token_hash"`
	WorkspaceID               string              `json:"workspace_id"`
	PoolName                  string              `json:"pool_name"`
	MachineID                 string              `json:"machine_id"`
	Mode                      string              `json:"mode,omitempty"`
	ContainerRuntime          string              `json:"container_runtime,omitempty"`
	ContainerRuntimeConfig    types.RuntimeConfig `json:"container_runtime_config,omitempty"`
	MarketplaceListingID      string              `json:"marketplace_listing_id,omitempty"`
	SellerWorkspaceID         string              `json:"seller_workspace_id,omitempty"`
	CPU                       int64               `json:"cpu"`
	Memory                    int64               `json:"memory"`
	GPU                       string              `json:"gpu"`
	GPUCount                  uint32              `json:"gpu_count"`
	GPUAssignment             string              `json:"gpu_assignment"`
	NetworkPrefix             string              `json:"network_prefix"`
	WorkerImage               string              `json:"worker_image"`
	NetworkSlotPoolSize       uint32              `json:"network_slot_pool_size"`
	ContainerStartConcurrency uint32              `json:"container_start_concurrency"`
	RequiresPoolSelector      bool                `json:"requires_pool_selector"`
	Priority                  int32               `json:"priority"`
	Preemptable               bool                `json:"preemptable"`
	CreatedAt                 time.Time           `json:"created_at"`
	UpdatedAt                 time.Time           `json:"updated_at"`
}

type PreflightCheckState struct {
	Name     string `json:"name"`
	OK       bool   `json:"ok"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}

// AgentHeartbeatTimeout is how long a machine stays "connected" after its
// last heartbeat. Heartbeats arrive every ~5s (telemetry) and ~10s
// (StreamAgent), so 60s tolerates brief dual-stream reconnects without
// disabling the machine's worker and flickering its status.
const AgentHeartbeatTimeout = 60 * time.Second

// agentHeartbeatFutureTolerance allows small clock differences between
// gateway pods writing heartbeats and gateway pods evaluating liveness.
const agentHeartbeatFutureTolerance = 5 * time.Second

func AgentMachineStatus(state *AgentTokenState, now time.Time) string {
	if AgentMachineConnected(state, now) {
		return types.AgentMachineStatusSchedulable
	}
	if state != nil && !state.Schedulable && AgentPreflightFailed(state.Preflight) {
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
		return lastSeen.Sub(now) <= agentHeartbeatFutureTolerance
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

func AgentPreflightFailed(checks []PreflightCheckState) bool {
	for _, check := range checks {
		if check.Severity == types.AgentPreflightSeverityError && !check.OK {
			return true
		}
	}
	return false
}
