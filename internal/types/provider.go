package types

type ProviderComputeRequest struct {
	Cpu      int64
	Memory   int64
	Gpu      string
	GpuCount uint32
}

type ProviderMachineState struct {
	MachineId string `json:"machine_id" redis:"machine_id"`
	HostName  string `json:"hostname" redis:"hostname"`
	Token     string `json:"token" redis:"token"`
}

/*
What do we need to do:

	- we need to reconcile existing beta9 machines, which are just machines that have been spun
		up with a particular label in a supported cloud provider

	- we need to know how much available compute there is in a particular machine, which means we need to know
	the initial compute capacity on the node

	- one approach is we could update state in redis, setting an initial capacity and then removing/adding capacity
	as workers are created and destroyed

	- is there a simpler approach??



*/
