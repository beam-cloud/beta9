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
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	AWSCloudFormationTemplatePath = "/api/v1/gateway/pools/cloud/aws/template.yaml"

	awsCloudDefaultRegion       = "us-east-1"
	awsCloudDefaultInstanceType = "t3.large"
	awsCloudDefaultDesiredNodes = uint32(1)
	awsCloudMaxNodesLimit       = uint32(100)
	awsCloudJoinTokenTTL        = 30 * 24 * time.Hour
	awsCloudDefaultNetworkSlots = uint32(128)
	awsCloudDefaultStartLimit   = uint32(16)
	awsCloudDefaultRootVolumeGB = uint32(200)
)

var (
	awsRegionPattern       = regexp.MustCompile(`^[a-z]{2}(-gov)?-[a-z0-9-]+-\d$`)
	awsInstanceTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,63}$`)
	cloudFormationPartRe   = regexp.MustCompile(`[^a-z0-9-]+`)
	cloudFormationDashRe   = regexp.MustCompile(`-+`)
)

func (s *Service) CreateAWSCloudPoolOnboarding(ctx context.Context, in *pb.CreateAWSCloudPoolOnboardingRequest) (*pb.CreateAWSCloudPoolOnboardingResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	if workspaceID == "" || ownerTokenID == "" {
		return &pb.CreateAWSCloudPoolOnboardingResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}

	req, err := normalizeAWSCloudPoolOnboardingRequest(in)
	if err != nil {
		return &pb.CreateAWSCloudPoolOnboardingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	var state *model.PoolState
	lockErr := s.withPoolStateLock(ctx, workspaceID, req.poolName, func() error {
		next, err := s.createOrUpdateAWSCloudPool(ctx, authInfo, req)
		if err != nil {
			return err
		}
		state = next
		return nil
	})
	if lockErr != nil {
		return &pb.CreateAWSCloudPoolOnboardingResponse{Ok: false, ErrMsg: lockErr.Error()}, nil
	}

	joinToken, _, err := s.createPrivatePoolJoinTokenForOwner(
		ctx,
		workspaceID,
		ownerTokenID,
		req.poolName,
		awsCloudJoinTokenTTL.String(),
		"",
	)
	if err != nil {
		return &pb.CreateAWSCloudPoolOnboardingResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	gatewayURL := strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	templateURL := awsCloudFormationTemplateURL(gatewayURL)
	stackName := awsCloudFormationStackName(workspaceID, req.poolName)
	consoleURL := awsCloudFormationConsoleURL(req.region, stackName, templateURL, map[string]string{
		"BeamGatewayURL":            gatewayURL,
		"BeamJoinToken":             joinToken,
		"BeamPoolName":              req.poolName,
		"BeamWorkerImage":           agentWorkerImage(s.appConfig),
		"ContainerStartConcurrency": strconv.FormatUint(uint64(awsCloudDefaultStartLimit), 10),
		"DesiredCapacity":           strconv.FormatUint(uint64(req.desiredNodes), 10),
		"MaxSize":                   strconv.FormatUint(uint64(req.maxNodes), 10),
		"MinSize":                   strconv.FormatUint(uint64(req.desiredNodes), 10),
		"NetworkSlots":              strconv.FormatUint(uint64(awsCloudDefaultNetworkSlots), 10),
		"NodeInstanceType":          req.instanceType,
		"RootVolumeSizeGB":          strconv.FormatUint(uint64(awsCloudDefaultRootVolumeGB), 10),
	})

	s.emitComputeEvent(types.EventComputePool, types.EventComputeSchema{
		WorkspaceID: workspaceID,
		PoolName:    state.Name,
		Action:      types.EventComputeActionPoolCreated,
		Status:      "cloud_onboarding",
		Source:      string(model.SourceAWSCloudFormation),
		Transport:   state.Transport,
		Fallback:    state.Fallback,
		NodeCount:   req.desiredNodes,
		Attrs: map[string]string{
			"cloud":         "aws",
			"region":        req.region,
			"instance_type": req.instanceType,
			"stack_name":    stackName,
		},
	})

	return &pb.CreateAWSCloudPoolOnboardingResponse{
		Ok:            true,
		Pool:          s.privatePoolStateToProto(state),
		StackName:     stackName,
		TemplateUrl:   templateURL,
		AwsConsoleUrl: consoleURL,
	}, nil
}

func (s *Service) GetCloudPoolOnboardingStatus(ctx context.Context, in *pb.GetCloudPoolOnboardingStatusRequest) (*pb.GetCloudPoolOnboardingStatusResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.GetCloudPoolOnboardingStatusResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}

	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, strings.TrimSpace(in.PoolName))
	if err != nil {
		return &pb.GetCloudPoolOnboardingStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.GetCloudPoolOnboardingStatusResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	machines, err := s.computeRepo.ListAgentTokenStates(ctx, workspaceID, state.Name)
	if err != nil {
		return &pb.GetCloudPoolOnboardingStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	pool := s.privatePoolStateToProtoWithMachines(state, machines)
	return &pb.GetCloudPoolOnboardingStatusResponse{
		Ok:                true,
		Pool:              pool,
		Ready:             pool.ReadyMachineCount > 0,
		ReadyMachineCount: pool.ReadyMachineCount,
		MachineCount:      pool.MachineCount,
	}, nil
}

type awsCloudPoolOnboardingRequest struct {
	poolName     string
	region       string
	instanceType string
	desiredNodes uint32
	maxNodes     uint32
}

func normalizeAWSCloudPoolOnboardingRequest(in *pb.CreateAWSCloudPoolOnboardingRequest) (awsCloudPoolOnboardingRequest, error) {
	if in == nil {
		return awsCloudPoolOnboardingRequest{}, fmt.Errorf("request is required")
	}
	req := awsCloudPoolOnboardingRequest{
		poolName:     strings.TrimSpace(in.PoolName),
		region:       strings.TrimSpace(in.Region),
		instanceType: strings.TrimSpace(in.InstanceType),
		desiredNodes: in.DesiredNodes,
		maxNodes:     in.MaxNodes,
	}
	if req.poolName == "" {
		return awsCloudPoolOnboardingRequest{}, fmt.Errorf("pool name is required")
	}
	if err := model.ValidatePoolName(req.poolName); err != nil {
		return awsCloudPoolOnboardingRequest{}, err
	}
	if req.region == "" {
		req.region = awsCloudDefaultRegion
	}
	if !awsRegionPattern.MatchString(req.region) {
		return awsCloudPoolOnboardingRequest{}, fmt.Errorf("invalid AWS region %q", req.region)
	}
	if req.instanceType == "" {
		req.instanceType = awsCloudDefaultInstanceType
	}
	if !strings.Contains(req.instanceType, ".") || !awsInstanceTypePattern.MatchString(req.instanceType) {
		return awsCloudPoolOnboardingRequest{}, fmt.Errorf("invalid AWS instance type %q", req.instanceType)
	}
	if req.desiredNodes == 0 {
		req.desiredNodes = awsCloudDefaultDesiredNodes
	}
	if req.desiredNodes > awsCloudMaxNodesLimit {
		return awsCloudPoolOnboardingRequest{}, fmt.Errorf("desired_nodes must be <= %d", awsCloudMaxNodesLimit)
	}
	if req.maxNodes == 0 {
		req.maxNodes = req.desiredNodes
	}
	if req.maxNodes < req.desiredNodes {
		return awsCloudPoolOnboardingRequest{}, fmt.Errorf("max_nodes must be >= desired_nodes")
	}
	if req.maxNodes > awsCloudMaxNodesLimit {
		return awsCloudPoolOnboardingRequest{}, fmt.Errorf("max_nodes must be <= %d", awsCloudMaxNodesLimit)
	}
	return req, nil
}

func (s *Service) createOrUpdateAWSCloudPool(ctx context.Context, authInfo *auth.AuthInfo, req awsCloudPoolOnboardingRequest) (*model.PoolState, error) {
	workspaceID := computeWorkspaceID(authInfo)
	ownerTokenID := computeOwnerTokenID(authInfo)
	existing, err := s.getPrivatePoolState(ctx, workspaceID, req.poolName)
	if err != nil {
		return nil, err
	}
	if existing != nil && !computePoolCreatedByAuth(existing, authInfo) {
		return nil, fmt.Errorf("pool already exists in this workspace")
	}
	if existing != nil && configuredPoolGPU(existing) != "" {
		return nil, fmt.Errorf("AWS cloud onboarding currently supports CPU pools only")
	}

	config := normalizePoolConfig(&pb.PoolConfig{
		Name:      req.poolName,
		Regions:   []string{req.region},
		Mode:      string(types.PoolModePrivate),
		Transport: defaultPrivateTransport,
		Fallback:  defaultPrivateFallback,
		Priority:  defaultPrivatePriority,
	})
	if _, err := computePoolFromProto(config, 0, false); err != nil {
		return nil, err
	}

	now := time.Now()
	state := &model.PoolState{
		Name:             config.Name,
		Selector:         config.Selector,
		Config:           config,
		Status:           "active",
		Source:           model.SourceAWSCloudFormation,
		Mode:             config.Mode,
		Transport:        config.Transport,
		Fallback:         config.Fallback,
		Priority:         config.Priority,
		CreatedByTokenID: ownerTokenID,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if existing != nil {
		state.Reservations = existing.Reservations
		state.ReservedNodes = existing.ReservedNodes
		state.CommittedSpendMicros = existing.CommittedSpendMicros
		state.CreatedByTokenID = existing.CreatedByTokenID
		state.CreatedAt = existing.CreatedAt
		state.ExpiresAt = existing.ExpiresAt
	}
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		return nil, err
	}
	if s.scheduler != nil {
		if err := s.scheduler.RegisterAgentPool(workspaceID, state); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func awsCloudFormationTemplateURL(gatewayURL string) string {
	return strings.TrimRight(gatewayURL, "/") + AWSCloudFormationTemplatePath
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

func cloudFormationNamePart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = cloudFormationPartRe.ReplaceAllString(value, "-")
	value = cloudFormationDashRe.ReplaceAllString(value, "-")
	return strings.Trim(value, "-")
}

func AWSCloudFormationTemplate() string {
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
    Default: t3.large
    Description: EC2 instance type for Beam CPU nodes.
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
            curl -fsSL '${BeamGatewayURL}/install/agent' | sh -s -- \
              --gateway '${BeamGatewayURL}' \
              --join-token '${BeamJoinToken}' \
              --background \
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
