package providers

import (
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type Provider interface {
	ListMachines() ([]types.Instance, error)
	StartMachine(id string) error
	StopMachine(id string) error
	// ProvisionMachine(config MachineConfig) (*types.Instance, error)
}
