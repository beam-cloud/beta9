package types

import "time"

const (
	MachineStatusRegistered          MachineStatus = "registered"
	MachineStatusPending             MachineStatus = "pending"
	MachinePendingExpirationS        int           = 3600 // 1 hour
	MachineKeepaliveExpirationS      int           = 300  // 5 minutes
	MachineEmptyConsolidationPeriodM time.Duration = 10 * time.Minute
)

type MachineStatus string

type ProviderComputeRequest struct {
	Cpu      int64
	Memory   int64
	Gpu      string
	GpuCount uint32
}

type ProviderMachine struct {
	State *ProviderMachineState
}

type ProviderMachineState struct {
	MachineId         string        `json:"machine_id" redis:"machine_id"`
	PoolName          string        `json:"pool_name" redis:"pool_name"`
	Status            MachineStatus `json:"status" redis:"status"`
	HostName          string        `json:"hostname" redis:"hostname"`
	Token             string        `json:"token" redis:"token"`
	Cpu               int64         `json:"cpu" redis:"cpu"`
	Memory            int64         `json:"memory" redis:"memory"`
	Gpu               string        `json:"gpu" redis:"gpu"`
	GpuCount          uint32        `json:"gpu_count" redis:"gpu_count"`
	RegistrationToken string        `json:"registration_token" redis:"registration_token"`
	Created           string        `json:"created" redis:"created"`
	LastWorkerSeen    string        `json:"last_worker_seen" redis:"last_worker_seen"`
	LastKeepalive     string        `json:"last_keepalive" redis:"last_keepalive"`
	AutoConsolidate   bool          `json:"auto_consolidate" redis:"auto_consolidate"`
}
