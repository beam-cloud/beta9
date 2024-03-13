package providers

import (
	"context"

	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
)

type Provider interface {
	ProvisionMachine(ctx context.Context, poolName, token string, compute types.ProviderComputeRequest) (string, error)
	TerminateMachine(ctx context.Context, poolName, machineId string) error
	Reconcile(ctx context.Context, poolName string)
}

func MachineId() string {
	return uuid.New().String()[:8]
}
