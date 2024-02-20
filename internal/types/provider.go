package types

type ProviderComputeRequest struct {
	Cpu      int64
	Memory   int64
	Gpu      string
	GpuCount uint32
}

type ProviderMachineState struct {
	MachineId string `redis:"machine_id"`
	WorkerId  string `redis:"worker_id"`
	HostName  string `redis:"hostname"`
	Token     string `redis:"token"`
}
