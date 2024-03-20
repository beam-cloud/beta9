package types

const (
	MachineStatusRegistered MachineStatus = "registered"
	MachineStatusPending    MachineStatus = "pending"
)

type MachineStatus string

type ProviderComputeRequest struct {
	Cpu      int64
	Memory   int64
	Gpu      string
	GpuCount uint32
}

type ProviderMachine struct {
	State      *ProviderMachineState
	WorkerKeys []string
}

type ProviderMachineState struct {
	MachineId string        `json:"machine_id" redis:"machine_id"`
	Status    MachineStatus `json:"status" redis:"status"`
	HostName  string        `json:"hostname" redis:"hostname"`
	Token     string        `json:"token" redis:"token"`
	Cpu       int64         `json:"cpu" redis:"cpu"`
	Memory    int64         `json:"memory" redis:"memory"`
	Gpu       string        `json:"gpu" redis:"gpu"`
	GpuCount  uint32        `json:"gpu_count" redis:"gpu_count"`
}
