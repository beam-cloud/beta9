package compute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	AWSBYOCTemplatePath = "/api/v1/gateway/pools/byoc/aws/template.yaml"

	awsBYOCDefaultRegion       = "us-east-1"
	awsBYOCDefaultInstanceType = "i4i.xlarge"
	awsBYOCDefaultDesiredNodes = uint32(1)
	awsBYOCMaxNodesLimit       = uint32(100)
	awsBYOCDefaultNetworkSlots = uint32(128)
	awsBYOCDefaultStartLimit   = uint32(16)
	awsBYOCDefaultRootVolumeGB = uint32(200)
)

var (
	awsRegionPattern       = regexp.MustCompile(`^[a-z]{2}(-gov)?-[a-z0-9-]+-\d$`)
	awsInstanceTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,63}$`)
	awsAccountIDPattern    = regexp.MustCompile(`^\d{12}$`)
	cloudFormationPartRe   = regexp.MustCompile(`[^a-z0-9-]+`)
	cloudFormationDashRe   = regexp.MustCompile(`-+`)
	awsBYOCInstanceTypes   = map[string]struct{}{
		"i4i.large":   {},
		"i4i.xlarge":  {},
		"i4i.2xlarge": {},
		"i4i.4xlarge": {},
	}
)

func (s *Service) CreateBYOCAWSPoolOnboarding(ctx context.Context, in *pb.CreateBYOCAWSPoolOnboardingRequest) (*pb.CreateBYOCAWSPoolOnboardingResponse, error) {
	if in == nil {
		return &pb.CreateBYOCAWSPoolOnboardingResponse{Ok: false, ErrMsg: "request is required"}, nil
	}
	result, err := s.createBYOCPoolOnboarding(ctx, byocAWSProvider{}, byocRawOnboardingRequest{
		PoolName:     in.PoolName,
		Region:       in.Region,
		InstanceType: in.InstanceType,
		DesiredNodes: in.DesiredNodes,
		MaxNodes:     in.MaxNodes,
		AccountID:    in.AwsAccountId,
	})
	if err != nil {
		return &pb.CreateBYOCAWSPoolOnboardingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreateBYOCAWSPoolOnboardingResponse{
		Ok:        true,
		Pool:      s.privatePoolStateToProto(result.Pool),
		SetupUrl:  result.SetupURL,
		StackName: result.ResourceName,
		StackUrl:  result.ResourceURL,
	}, nil
}

func (s *Service) GetBYOCAWSStack(ctx context.Context, in *pb.GetBYOCAWSStackRequest) (*pb.GetBYOCAWSStackResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	stack, err := s.getBYOCProviderResource(ctx, authInfo, byocAWSProvider{}, in.GetPoolName())
	if err != nil {
		return &pb.GetBYOCAWSStackResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.GetBYOCAWSStackResponse{
		Ok:         true,
		Provider:   stack.Provider,
		AccountId:  stack.AccountID,
		Region:     stack.Region,
		StackName:  stack.ResourceName,
		StackUrl:   stack.ResourceURL,
		DestroyUrl: stack.DestroyURL,
	}, nil
}

type byocAWSProvider struct{}

func (byocAWSProvider) Source() model.CapacitySource {
	return model.SourceAWS
}

func (byocAWSProvider) NormalizeOnboarding(raw byocRawOnboardingRequest) (byocPoolOnboardingRequest, error) {
	req := byocPoolOnboardingRequest{
		poolName:     strings.TrimSpace(raw.PoolName),
		region:       strings.TrimSpace(raw.Region),
		instanceType: strings.TrimSpace(raw.InstanceType),
		desiredNodes: raw.DesiredNodes,
		maxNodes:     raw.MaxNodes,
		accountID:    strings.TrimSpace(raw.AccountID),
	}
	if req.poolName == "" {
		return byocPoolOnboardingRequest{}, fmt.Errorf("pool name is required")
	}
	if err := model.ValidatePoolName(req.poolName); err != nil {
		return byocPoolOnboardingRequest{}, err
	}
	if req.region == "" {
		req.region = awsBYOCDefaultRegion
	}
	if !awsRegionPattern.MatchString(req.region) {
		return byocPoolOnboardingRequest{}, fmt.Errorf("invalid AWS region %q", req.region)
	}
	if req.instanceType == "" {
		req.instanceType = awsBYOCDefaultInstanceType
	}
	if !strings.Contains(req.instanceType, ".") || !awsInstanceTypePattern.MatchString(req.instanceType) {
		return byocPoolOnboardingRequest{}, fmt.Errorf("invalid AWS instance type %q", req.instanceType)
	}
	if _, ok := awsBYOCInstanceTypes[req.instanceType]; !ok {
		return byocPoolOnboardingRequest{}, fmt.Errorf("unsupported AWS instance type %q", req.instanceType)
	}
	if req.desiredNodes == 0 {
		req.desiredNodes = awsBYOCDefaultDesiredNodes
	}
	if req.desiredNodes > awsBYOCMaxNodesLimit {
		return byocPoolOnboardingRequest{}, fmt.Errorf("desired_nodes must be <= %d", awsBYOCMaxNodesLimit)
	}
	if req.maxNodes == 0 {
		req.maxNodes = req.desiredNodes
	}
	if req.maxNodes < req.desiredNodes {
		return byocPoolOnboardingRequest{}, fmt.Errorf("max_nodes must be >= desired_nodes")
	}
	if req.maxNodes > awsBYOCMaxNodesLimit {
		return byocPoolOnboardingRequest{}, fmt.Errorf("max_nodes must be <= %d", awsBYOCMaxNodesLimit)
	}
	if req.accountID != "" && !awsAccountIDPattern.MatchString(req.accountID) {
		return byocPoolOnboardingRequest{}, fmt.Errorf("invalid AWS account ID")
	}
	return req, nil
}

func (byocAWSProvider) PoolState(workspaceID string, req byocPoolOnboardingRequest) *model.BYOCProviderState {
	stackName := awsCloudFormationStackName(workspaceID, req.poolName)
	stackURL := awsCloudFormationStackURL(req.region, stackName)
	return &model.BYOCProviderState{
		Provider:     string(model.SourceAWS),
		AccountID:    req.accountID,
		Region:       req.region,
		ResourceName: stackName,
		ResourceURL:  stackURL,
		DestroyURL:   stackURL,
	}
}

func (byocAWSProvider) Setup(_ context.Context, input byocProviderSetupInput) (*byocProviderSetupResult, error) {
	stackName := ""
	if input.ProviderData != nil {
		stackName = input.ProviderData.ResourceName
	}
	if stackName == "" {
		stackName = awsCloudFormationStackName(input.WorkspaceID, input.Request.poolName)
	}
	stackURL := awsCloudFormationStackURL(input.Request.region, stackName)
	consoleURL := awsCloudFormationConsoleURL(input.Request.region, stackName, awsBYOCTemplateURL(input.GatewayURL), map[string]string{
		"BeamGatewayURL":            input.GatewayURL,
		"BeamJoinToken":             input.JoinToken,
		"BeamPoolName":              input.Request.poolName,
		"BeamWorkerImage":           input.WorkerImage,
		"ContainerStartConcurrency": strconv.FormatUint(uint64(awsBYOCDefaultStartLimit), 10),
		"DesiredCapacity":           strconv.FormatUint(uint64(input.Request.desiredNodes), 10),
		"MaxSize":                   strconv.FormatUint(uint64(input.Request.maxNodes), 10),
		"MinSize":                   strconv.FormatUint(uint64(input.Request.desiredNodes), 10),
		"NetworkSlots":              strconv.FormatUint(uint64(awsBYOCDefaultNetworkSlots), 10),
		"NodeInstanceType":          input.Request.instanceType,
		"RootVolumeSizeGB":          strconv.FormatUint(uint64(awsBYOCDefaultRootVolumeGB), 10),
	})
	return &byocProviderSetupResult{
		SetupURL:     consoleURL,
		ResourceName: stackName,
		ResourceURL:  stackURL,
		EventAttrs: map[string]string{
			"cloud":         string(model.SourceAWS),
			"region":        input.Request.region,
			"instance_type": input.Request.instanceType,
		},
	}, nil
}

func (byocAWSProvider) Resource(workspaceID string, state *model.PoolState) (*byocProviderResource, error) {
	stack := awsBYOCStackState(workspaceID, state)
	stackURL := stack.ResourceURL
	if stackURL == "" {
		stackURL = awsCloudFormationStackURL(stack.Region, stack.ResourceName)
	}
	destroyURL := stack.DestroyURL
	if destroyURL == "" {
		destroyURL = stackURL
	}
	return &byocProviderResource{
		Provider:     string(model.SourceAWS),
		AccountID:    stack.AccountID,
		Region:       stack.Region,
		ResourceName: stack.ResourceName,
		ResourceURL:  stackURL,
		DestroyURL:   destroyURL,
	}, nil
}

func (byocAWSProvider) ValidateExistingPool(existing *model.PoolState) error {
	if existing != nil && configuredPoolGPU(existing) != "" {
		return fmt.Errorf("AWS BYOC currently supports CPU pools only")
	}
	return nil
}

func awsBYOCTemplateURL(gatewayURL string) string {
	return strings.TrimRight(gatewayURL, "/") + AWSBYOCTemplatePath
}

func awsCloudFormationConsoleURL(region, stackName, templateURL string, params map[string]string) string {
	values := url.Values{}
	values.Set("templateURL", templateURL)
	values.Set("stackName", stackName)
	for key, value := range params {
		values.Set("param_"+key, value)
	}
	return fmt.Sprintf("https://console.aws.amazon.com/cloudformation/home?region=%s#/stacks/create/review?%s", url.QueryEscape(region), values.Encode())
}

func awsCloudFormationStackURL(region, stackName string) string {
	return fmt.Sprintf("https://console.aws.amazon.com/cloudformation/home?region=%s#/stacks/stackinfo?stackId=%s", url.QueryEscape(region), url.QueryEscape(stackName))
}

func awsCloudFormationStackName(workspaceID, poolName string) string {
	part := cloudFormationNamePart(poolName)
	if part == "" {
		part = "pool"
	}
	sum := sha256.Sum256([]byte(workspaceID + "\x00" + poolName))
	suffix := hex.EncodeToString(sum[:4])
	maxPart := 128 - len("beam--") - len(suffix)
	if len(part) > maxPart {
		part = strings.Trim(part[:maxPart], "-")
	}
	return fmt.Sprintf("beam-%s-%s", part, suffix)
}

func awsBYOCStackState(workspaceID string, state *model.PoolState) *model.BYOCProviderState {
	if state == nil {
		return &model.BYOCProviderState{Provider: string(model.SourceAWS), Region: awsBYOCDefaultRegion}
	}
	if state.BYOC != nil {
		stack := *state.BYOC
		if stack.Provider == "" {
			stack.Provider = string(model.SourceAWS)
		}
		if stack.Region == "" {
			stack.Region = awsBYOCStateRegion(state)
		}
		if stack.ResourceName == "" {
			stack.ResourceName = awsCloudFormationStackName(workspaceID, state.Name)
		}
		if stack.ResourceURL == "" {
			stack.ResourceURL = awsCloudFormationStackURL(stack.Region, stack.ResourceName)
		}
		if stack.DestroyURL == "" {
			stack.DestroyURL = stack.ResourceURL
		}
		return &stack
	}
	region := awsBYOCStateRegion(state)
	stackName := awsCloudFormationStackName(workspaceID, state.Name)
	stackURL := awsCloudFormationStackURL(region, stackName)
	return &model.BYOCProviderState{
		Provider:     string(model.SourceAWS),
		Region:       region,
		ResourceName: stackName,
		ResourceURL:  stackURL,
		DestroyURL:   stackURL,
	}
}

func awsBYOCStateRegion(state *model.PoolState) string {
	if state != nil && state.Config != nil && len(state.Config.Regions) > 0 && strings.TrimSpace(state.Config.Regions[0]) != "" {
		return strings.TrimSpace(state.Config.Regions[0])
	}
	return awsBYOCDefaultRegion
}

func cloudFormationNamePart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = cloudFormationPartRe.ReplaceAllString(value, "-")
	value = cloudFormationDashRe.ReplaceAllString(value, "-")
	return strings.Trim(value, "-")
}

func AWSBYOCTemplate() string {
	return awsCloudFormationTemplate
}

const awsCloudFormationTemplate = `AWSTemplateFormatVersion: "2010-09-09"
Description: Beam AWS BYOC CPU pool. Creates an EC2 Auto Scaling Group that joins a Beam private pool.

Parameters:
  BeamGatewayURL:
    Type: String
    Description: Public Beam gateway HTTP URL.
  BeamJoinToken:
    Type: String
    NoEcho: true
    Description: Beam private pool join token.
  BeamPoolName:
    Type: String
    Description: Beam pool name used for AWS resource tags.
  BeamWorkerImage:
    Type: String
    Default: public.ecr.aws/n4e0e1y0/beta9-worker:latest
    AllowedPattern: '^[A-Za-z0-9./:_@-]+$'
    Description: Beam worker image to run on each BYOC node.
  NodeInstanceType:
    Type: String
    Default: i4i.xlarge
    AllowedValues:
      - i4i.large
      - i4i.xlarge
      - i4i.2xlarge
      - i4i.4xlarge
    Description: EC2 storage-optimized instance type for Beam CPU nodes with local NVMe.
  RootVolumeSizeGB:
    Type: Number
    Default: 200
    MinValue: 50
    MaxValue: 2048
    Description: Root EBS volume size in GiB for image cache, logs, and sandbox working data.
  NetworkSlots:
    Type: Number
    Default: 128
    MinValue: 16
    MaxValue: 512
    Description: Preallocated container network slots per node.
  ContainerStartConcurrency:
    Type: Number
    Default: 16
    MinValue: 1
    MaxValue: 128
    Description: Maximum concurrent sandbox starts per node.
  DesiredCapacity:
    Type: Number
    Default: 1
    MinValue: 1
    MaxValue: 100
  MinSize:
    Type: Number
    Default: 1
    MinValue: 0
    MaxValue: 100
  MaxSize:
    Type: Number
    Default: 1
    MinValue: 1
    MaxValue: 100
  VpcCidr:
    Type: String
    Default: 10.86.0.0/16
  PublicSubnetACidr:
    Type: String
    Default: 10.86.1.0/24
  PublicSubnetBCidr:
    Type: String
    Default: 10.86.2.0/24
  LatestAmiId:
    Type: AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>
    Default: /aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id

Resources:
  VPC:
    Type: AWS::EC2::VPC
    Properties:
      CidrBlock: !Ref VpcCidr
      EnableDnsHostnames: true
      EnableDnsSupport: true
      Tags:
        - Key: Name
          Value: !Sub beam-${BeamPoolName}
        - Key: beam:pool
          Value: !Ref BeamPoolName

  InternetGateway:
    Type: AWS::EC2::InternetGateway
    Properties:
      Tags:
        - Key: Name
          Value: !Sub beam-${BeamPoolName}

  VPCGatewayAttachment:
    Type: AWS::EC2::VPCGatewayAttachment
    Properties:
      VpcId: !Ref VPC
      InternetGatewayId: !Ref InternetGateway

  PublicRouteTable:
    Type: AWS::EC2::RouteTable
    Properties:
      VpcId: !Ref VPC
      Tags:
        - Key: Name
          Value: !Sub beam-${BeamPoolName}-public

  PublicRoute:
    Type: AWS::EC2::Route
    DependsOn: VPCGatewayAttachment
    Properties:
      RouteTableId: !Ref PublicRouteTable
      DestinationCidrBlock: 0.0.0.0/0
      GatewayId: !Ref InternetGateway

  PublicSubnetA:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref VPC
      CidrBlock: !Ref PublicSubnetACidr
      AvailabilityZone: !Select [0, !GetAZs ""]
      MapPublicIpOnLaunch: true
      Tags:
        - Key: Name
          Value: !Sub beam-${BeamPoolName}-a

  PublicSubnetB:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref VPC
      CidrBlock: !Ref PublicSubnetBCidr
      AvailabilityZone: !Select [1, !GetAZs ""]
      MapPublicIpOnLaunch: true
      Tags:
        - Key: Name
          Value: !Sub beam-${BeamPoolName}-b

  PublicSubnetARouteTableAssociation:
    Type: AWS::EC2::SubnetRouteTableAssociation
    Properties:
      SubnetId: !Ref PublicSubnetA
      RouteTableId: !Ref PublicRouteTable

  PublicSubnetBRouteTableAssociation:
    Type: AWS::EC2::SubnetRouteTableAssociation
    Properties:
      SubnetId: !Ref PublicSubnetB
      RouteTableId: !Ref PublicRouteTable

  NodeSecurityGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: Beam BYOC node security group
      VpcId: !Ref VPC
      SecurityGroupIngress:
        - IpProtocol: udp
          FromPort: 41641
          ToPort: 41641
          CidrIp: 0.0.0.0/0
          Description: Tailscale direct WireGuard traffic
      SecurityGroupEgress:
        - IpProtocol: -1
          CidrIp: 0.0.0.0/0
      Tags:
        - Key: Name
          Value: !Sub beam-${BeamPoolName}-nodes

  NodeSecurityGroupSelfIngress:
    Type: AWS::EC2::SecurityGroupIngress
    Properties:
      GroupId: !Ref NodeSecurityGroup
      IpProtocol: -1
      SourceSecurityGroupId: !Ref NodeSecurityGroup
      Description: Beam BYOC node-to-node private traffic

  NodeSecurityGroupVpcIngress:
    Type: AWS::EC2::SecurityGroupIngress
    Properties:
      GroupId: !Ref NodeSecurityGroup
      IpProtocol: -1
      CidrIp: !Ref VpcCidr
      Description: Beam BYOC private VPC traffic

  NodeRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: ec2.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
      Tags:
        - Key: beam:pool
          Value: !Ref BeamPoolName

  NodeInstanceProfile:
    Type: AWS::IAM::InstanceProfile
    Properties:
      Roles:
        - !Ref NodeRole

  NodeLaunchTemplate:
    Type: AWS::EC2::LaunchTemplate
    Properties:
      LaunchTemplateData:
        ImageId: !Ref LatestAmiId
        InstanceType: !Ref NodeInstanceType
        BlockDeviceMappings:
          - DeviceName: /dev/sda1
            Ebs:
              VolumeSize: !Ref RootVolumeSizeGB
              VolumeType: gp3
              Encrypted: true
              DeleteOnTermination: true
        IamInstanceProfile:
          Arn: !GetAtt NodeInstanceProfile.Arn
        SecurityGroupIds:
          - !Ref NodeSecurityGroup
        MetadataOptions:
          HttpEndpoint: enabled
          HttpTokens: required
        TagSpecifications:
          - ResourceType: instance
            Tags:
              - Key: Name
                Value: !Sub beam-${BeamPoolName}-node
              - Key: beam:pool
                Value: !Ref BeamPoolName
          - ResourceType: volume
            Tags:
              - Key: beam:pool
                Value: !Ref BeamPoolName
        UserData:
          Fn::Base64: !Sub |
            #!/bin/bash
            set -euxo pipefail
            BEAM_STATE_DIR=/var/lib/beam/agent
            BEAM_NVME_ROOT=/mnt/beam-nvme
            NVME_DEVICE=""
            for link in /dev/disk/by-id/nvme-Amazon_EC2_NVMe_Instance_Storage*; do
              if [ -e "$link" ]; then
                NVME_DEVICE="$(readlink -f "$link")"
                break
              fi
            done
            if [ -n "$NVME_DEVICE" ]; then
              if ! blkid "$NVME_DEVICE" >/dev/null 2>&1; then
                mkfs.ext4 -F "$NVME_DEVICE"
              fi
              mkdir -p "$BEAM_NVME_ROOT"
              if ! mountpoint -q "$BEAM_NVME_ROOT"; then
                mount "$NVME_DEVICE" "$BEAM_NVME_ROOT"
              fi
              NVME_UUID="$(blkid -s UUID -o value "$NVME_DEVICE")"
              if [ -n "$NVME_UUID" ] && ! grep -q "$NVME_UUID" /etc/fstab; then
                echo "UUID=$NVME_UUID $BEAM_NVME_ROOT ext4 defaults,nofail,noatime 0 2" >> /etc/fstab
              fi
              BEAM_STATE_DIR="$BEAM_NVME_ROOT/beam/agent"
              mkdir -p "$BEAM_NVME_ROOT/docker" "$BEAM_STATE_DIR"
              mkdir -p /etc/docker
              printf '{"data-root":"%s"}\n' "$BEAM_NVME_ROOT/docker" > /etc/docker/daemon.json
            fi
            mkdir -p "$BEAM_STATE_DIR"
            export BEAM_AGENT_INSTALL_DOCKER=1
            curl -fsSL '${BeamGatewayURL}/install/agent' | sh -s -- \
              --gateway '${BeamGatewayURL}' \
              --join-token '${BeamJoinToken}' \
              --background \
              --state-dir "$BEAM_STATE_DIR" \
              --transport tsnet_restricted \
              --executor worker-container \
              --worker-image '${BeamWorkerImage}' \
              --network-slots '${NetworkSlots}' \
              --container-start-concurrency '${ContainerStartConcurrency}'

  NodeAutoScalingGroup:
    Type: AWS::AutoScaling::AutoScalingGroup
    Properties:
      MinSize: !Ref MinSize
      MaxSize: !Ref MaxSize
      DesiredCapacity: !Ref DesiredCapacity
      VPCZoneIdentifier:
        - !Ref PublicSubnetA
        - !Ref PublicSubnetB
      HealthCheckType: EC2
      LaunchTemplate:
        LaunchTemplateId: !Ref NodeLaunchTemplate
        Version: !GetAtt NodeLaunchTemplate.LatestVersionNumber
      Tags:
        - Key: Name
          Value: !Sub beam-${BeamPoolName}
          PropagateAtLaunch: true
        - Key: beam:pool
          Value: !Ref BeamPoolName
          PropagateAtLaunch: true

Outputs:
  PoolName:
    Description: Beam private pool name.
    Value: !Ref BeamPoolName
  AutoScalingGroupName:
    Description: EC2 Auto Scaling Group backing the Beam pool.
    Value: !Ref NodeAutoScalingGroup
  VpcId:
    Description: VPC created for Beam BYOC nodes.
    Value: !Ref VPC
`
