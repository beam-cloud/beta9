package providers

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// EC2Provider struct to handle EC2 operations.
type EC2Provider struct {
	client *ec2.Client
}

// MachineConfig struct to define the configuration for a new machine.
type MachineConfig struct {
	InstanceType types.InstanceType
	ImageID      string
	KeyName      string
}

// NewEC2Provider initializes a new EC2 provider with AWS configuration.
func NewEC2Provider() (*EC2Provider, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	client := ec2.NewFromConfig(cfg)
	return &EC2Provider{client: client}, nil
}

// ListMachines lists all EC2 instances.
func (e *EC2Provider) ListMachines() ([]types.Instance, error) {
	input := &ec2.DescribeInstancesInput{}
	result, err := e.client.DescribeInstances(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	var instances []types.Instance
	for _, reservation := range result.Reservations {
		instances = append(instances, reservation.Instances...)
	}

	return instances, nil
}

// StartMachine starts an EC2 instance with the given ID.
func (e *EC2Provider) StartMachine(id string) error {
	input := &ec2.StartInstancesInput{
		InstanceIds: []string{id},
	}

	_, err := e.client.StartInstances(context.TODO(), input)
	return err
}

// StopMachine stops an EC2 instance with the given ID.
func (e *EC2Provider) StopMachine(id string) error {
	input := &ec2.StopInstancesInput{
		InstanceIds: []string{id},
	}

	_, err := e.client.StopInstances(context.TODO(), input)
	return err
}

// ProvisionMachine creates a new EC2 instance with the specified configuration.
func (e *EC2Provider) ProvisionMachine(config MachineConfig) (*types.Instance, error) {
	input := &ec2.RunInstancesInput{
		ImageId:      aws.String(config.ImageID),
		InstanceType: config.InstanceType,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		KeyName:      aws.String(config.KeyName),
	}

	result, err := e.client.RunInstances(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	if len(result.Instances) == 0 {
		return nil, fmt.Errorf("no instance was created")
	}

	return &result.Instances[0], nil
}
