package types

const (
	MachineStatusRegistered     MachineStatus = "registered"
	MachineStatusPending        MachineStatus = "pending"
	MachinePendingExpirationS   int           = 3600 // 1 hour
	MachineKeepaliveExpirationS int           = 60   // 1 minute
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
}
