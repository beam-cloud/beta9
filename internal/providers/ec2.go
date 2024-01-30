package providers

import (
	"context"
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/beam-cloud/beta9/internal/types"
)

type EC2Provider struct {
	client *ec2.Client
	config types.EC2ProviderConfig
}

func NewEC2Provider(appConfig types.AppConfig) (*EC2Provider, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(appConfig.Providers.EC2Config.AWSRegion),
	)
	if err != nil {
		log.Printf("Error loading AWS configuration: %v", err)
		return nil, err
	}

	return &EC2Provider{
		client: ec2.NewFromConfig(cfg),
		config: appConfig.Providers.EC2Config,
	}, nil
}

func (p *EC2Provider) ProvisionMachine(ctx context.Context) error {
	instanceType := "bababab" // TODO: map compute request to instance type
	// based on GPU/compute config

	userData := "#!/bin/bash"

	input := &ec2.RunInstancesInput{
		ImageId:      aws.String(p.config.AMI),
		InstanceType: awsTypes.InstanceType(instanceType),
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		UserData:     aws.String(userData),
	}

	result, err := p.client.RunInstances(ctx, input)
	if err != nil {
		return err
	}

	if len(result.Instances) == 0 {
		return errors.New("instance not created")
	}

	instanceID := *result.Instances[0].InstanceId
	log.Printf("provisioned instance with ID: %s", instanceID)

	return nil
}

func (p *EC2Provider) TerminateMachine(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("invalid instance ID")
	}

	input := &ec2.TerminateInstancesInput{
		InstanceIds: []string{id},
	}

	_, err := p.client.TerminateInstances(ctx, input)
	if err != nil {
		log.Printf("Error terminating EC2 instance %s: %v", id, err)
		return err
	}

	return nil
}

func (p *EC2Provider) ListMachines() error {
	return nil
}
