package providers

import (
	"log"

	"github.com/beam-cloud/beta9/internal/types"
)

type EC2Provider struct {
}

func NewEC2Provider(config types.AppConfig) (Provider, error) {
	log.Println("creating ec2 provider")

	log.Printf("access key: %s\n", config.Providers.EC2Config.AWSAccessKeyID)
	log.Printf("secret key: %s\n", config.Providers.EC2Config.AWSSecretAccessKey)
	log.Printf("region: %s\n", config.Providers.EC2Config.AWSRegion)

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
