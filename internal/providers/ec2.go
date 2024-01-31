package providers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	awsTypes "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/beam-cloud/beta9/internal/types"
)

type EC2Provider struct {
	client *ec2.Client
	config types.EC2ProviderConfig
}

const instanceNamePrefix = "beta9"

func NewEC2Provider(appConfig types.AppConfig) (*EC2Provider, error) {
	credentials := credentials.NewStaticCredentialsProvider(appConfig.Providers.EC2Config.AWSAccessKeyID, appConfig.Providers.EC2Config.AWSSecretAccessKey, "")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(appConfig.Providers.EC2Config.AWSRegion),
		config.WithCredentialsProvider(credentials),
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

func (p *EC2Provider) ProvisionMachine(ctx context.Context, poolName string, machineId string) error {
	instanceType := "g4dn.xlarge" // TODO: map compute request to instance type

	encodedUserData := base64.StdEncoding.EncodeToString([]byte(userData))
	input := &ec2.RunInstancesInput{
		ImageId:      aws.String(p.config.AMI),
		InstanceType: awsTypes.InstanceType(instanceType),
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		UserData:     aws.String(encodedUserData),
		SubnetId:     p.config.SubnetId,
	}

	result, err := p.client.RunInstances(ctx, input)
	if err != nil {
		return err
	}

	if len(result.Instances) == 0 {
		return errors.New("instance not created")
	}

	instanceID := *result.Instances[0].InstanceId
	instanceName := fmt.Sprintf("%s-%s-%s", instanceNamePrefix, poolName, machineId)

	_, err = p.client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{instanceID},
		Tags: []awsTypes.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(instanceName),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("failed to tag the instance: %w", err)
	}

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

var userData string = `
#!/bin/bash

INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl http://169.254.169.254/latest/meta-data/placement/region)
PROVIDER_ID="aws:///$REGION/$INSTANCE_ID"
INSTALL_K3S_VERSION="v1.28.5+k3s1"

# Install K3s with the Elastic IP in the certificate SAN
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION=$INSTALL_K3S_VERSION INSTALL_K3S_EXEC=" --disable traefik --kubelet-arg=provider-id=$PROVIDER_ID" sh -    

# Wait for K3s to start, create the kubeconfig file, and the node token
while [ ! -f /etc/rancher/k3s/k3s.yaml ] || [ ! -f /var/lib/rancher/k3s/server/node-token ]
do
  sleep 1
done

KUBECONFIG=/etc/rancher/k3s/k3s.yaml
`
