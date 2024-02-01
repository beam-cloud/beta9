package providers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

type ComputeRequest struct {
	Cpu    int64
	Memory int64
	Gpu    string
}

type Provider interface {
	ListMachines(ctx context.Context, poolName string) ([]string, error)
	ProvisionMachine(ctx context.Context, poolName string, compute ComputeRequest) (string, error)
	TerminateMachine(ctx context.Context, poolName, machineId string) error
	Reconcile(ctx context.Context, poolName string)
}

func MachineId(id string) string {
	hasher := sha256.New()
	hasher.Write([]byte(id))
	hash := hasher.Sum(nil)
	machineId := hex.EncodeToString(hash[:8])
	return machineId
}
