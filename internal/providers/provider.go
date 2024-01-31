package providers

import (
	"context"
)

type ComputeRequest struct {
	Cpu    int64  `json:"cpu" redis:"cpu"`
	Memory int64  `json:"memory" redis:"memory"`
	Gpu    string `json:"gpu" redis:"gpu"`
}

type Provider interface {
	ListMachines() error
	TerminateMachine(ctx context.Context, id string) error
	ProvisionMachine(ctx context.Context, compute ComputeRequest, poolName string, machineId string) error
}
