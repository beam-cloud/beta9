package providers

import (
	"github.com/beam-cloud/beta9/internal/types"
)

type EC2Provider struct {
}

func NewEC2Provider(config types.AppConfig) (Provider, error) {
	return &EC2Provider{}, nil
}

func (p *EC2Provider) ListMachines() error {
	return nil
}
func (p *EC2Provider) TerminateMachine(id string) error {
	return nil
}
func (p *EC2Provider) ProvisionMachine() error {
	return nil
}
