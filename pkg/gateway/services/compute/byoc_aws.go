package compute

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	awsBYOCDefaultRegion       = "us-east-1"
	awsBYOCDefaultInstanceType = "i4i.xlarge"
	awsBYOCDefaultDesiredNodes = uint32(1)
	awsBYOCMaxNodesLimit       = uint32(100)
	awsBYOCDefaultNetworkSlots = uint32(128)
	awsBYOCDefaultStartLimit   = uint32(16)
	awsBYOCDefaultRootVolumeGB = uint32(200)
	awsBYOCSandboxesPerNode    = uint32(20)

	awsBYOCAutoScalingGroupNameLabel  = "autoscaling_group_name"
	awsBYOCControlRoleArnLabel        = "control_role_arn"
	awsBYOCControlRoleExternalIDLabel = "control_role_external_id"
	awsBYOCControlRoleNameLabel       = "control_role_name"
)

var (
	awsRegionPattern       = regexp.MustCompile(`^[a-z]{2}(-gov)?-[a-z0-9-]+-\d$`)
	awsInstanceTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,63}$`)
	awsAccountIDPattern    = regexp.MustCompile(`^\d{12}$`)
	awsPrincipalARNPattern = regexp.MustCompile(`^arn:[^:]+:iam::\d{12}:(role/.+|root)$`)
	awsS3TemplateHostRe    = regexp.MustCompile(`^(?:s3[.-][a-z0-9-]+|[a-z0-9][a-z0-9.-]*\.s3[.-][a-z0-9-]+)\.amazonaws\.com$`)
	awsEC2InstanceIDRe     = regexp.MustCompile(`^i-[a-f0-9]{8,17}$`)
	cloudFormationPartRe   = regexp.MustCompile(`[^a-z0-9-]+`)
	cloudFormationDashRe   = regexp.MustCompile(`-+`)
	awsBYOCInstanceCatalog = map[string]awsBYOCInstanceType{
		// Storage optimized instances with local NVMe for cache performance.
		// Prices are us-east-1 Linux On-Demand USD/hr, in micros.
		"i4i.large":   {HourlyCostMicros: 172_000},
		"i4i.xlarge":  {HourlyCostMicros: 343_000},
		"i4i.2xlarge": {HourlyCostMicros: 686_000},
		"i4i.4xlarge": {HourlyCostMicros: 1_373_000},
	}
)

var errBYOCDirectScaleUnavailable = errors.New("direct BYOC scaling is unavailable for this pool; recreate the AWS stack from Beam to enable one-click resizing")
var errAWSBYOCResourceNotFound = errors.New("AWS BYOC resource not found")

type byocAWSProvider struct{}

type awsBYOCInstanceType struct {
	HourlyCostMicros int64
}

func (byocAWSProvider) Source() model.CapacitySource {
	return model.SourceAWS
}

func (byocAWSProvider) NormalizePoolRequest(raw byocRawPoolRequest) (byocPoolRequest, error) {
	req := byocPoolRequest{
		poolName:     strings.TrimSpace(raw.PoolName),
		region:       strings.TrimSpace(raw.Region),
		instanceType: strings.TrimSpace(raw.InstanceType),
		desiredNodes: raw.DesiredNodes,
		maxNodes:     raw.MaxNodes,
		accountID:    strings.TrimSpace(raw.AccountID),
	}
	if req.poolName == "" {
		return byocPoolRequest{}, fmt.Errorf("pool name is required")
	}
	if err := model.ValidatePoolName(req.poolName); err != nil {
		return byocPoolRequest{}, err
	}
	if req.region == "" {
		req.region = awsBYOCDefaultRegion
	}
	if !awsRegionPattern.MatchString(req.region) {
		return byocPoolRequest{}, fmt.Errorf("invalid AWS region %q", req.region)
	}
	if req.instanceType == "" {
		req.instanceType = awsBYOCDefaultInstanceType
	}
	if !strings.Contains(req.instanceType, ".") || !awsInstanceTypePattern.MatchString(req.instanceType) {
		return byocPoolRequest{}, fmt.Errorf("invalid AWS instance type %q", req.instanceType)
	}
	if _, ok := awsBYOCInstanceCatalog[req.instanceType]; !ok {
		return byocPoolRequest{}, fmt.Errorf("unsupported AWS instance type %q", req.instanceType)
	}
	if req.desiredNodes == 0 {
		req.desiredNodes = awsBYOCDefaultDesiredNodes
	}
	if req.desiredNodes > awsBYOCMaxNodesLimit {
		return byocPoolRequest{}, fmt.Errorf("desired_nodes must be <= %d", awsBYOCMaxNodesLimit)
	}
	if req.maxNodes == 0 {
		req.maxNodes = req.desiredNodes
	}
	if req.maxNodes < req.desiredNodes {
		return byocPoolRequest{}, fmt.Errorf("max_nodes must be >= desired_nodes")
	}
	if req.maxNodes > awsBYOCMaxNodesLimit {
		return byocPoolRequest{}, fmt.Errorf("max_nodes must be <= %d", awsBYOCMaxNodesLimit)
	}
	if req.accountID == "" {
		return byocPoolRequest{}, fmt.Errorf("AWS account ID is required")
	}
	if !awsAccountIDPattern.MatchString(req.accountID) {
		return byocPoolRequest{}, fmt.Errorf("invalid AWS account ID")
	}
	return req, nil
}

func (byocAWSProvider) PoolState(workspaceID string, req byocPoolRequest, config types.ManagedComputeConfig) *model.BYOCProviderState {
	stackName := awsCloudFormationStackName(workspaceID, req.poolName)
	stackURL := awsCloudFormationStackURL(req.region, stackName)
	labels := awsBYOCCapacityLabels(req)
	labels[awsBYOCAutoScalingGroupNameLabel] = awsBYOCAutoScalingGroupName(workspaceID, req.poolName)
	if strings.TrimSpace(config.BYOC.AWS.ControlRolePrincipalARN) != "" {
		roleName := awsBYOCControlRoleName(workspaceID, req.poolName)
		labels[awsBYOCControlRoleNameLabel] = roleName
		labels[awsBYOCControlRoleArnLabel] = awsBYOCControlRoleARN(req.region, req.accountID, roleName)
		labels[awsBYOCControlRoleExternalIDLabel] = awsBYOCControlExternalID(workspaceID, req.poolName)
	}
	return &model.BYOCProviderState{
		Provider:     string(model.SourceAWS),
		AccountID:    req.accountID,
		Region:       req.region,
		ResourceName: stackName,
		ResourceURL:  stackURL,
		DestroyURL:   stackURL,
		Labels:       labels,
	}
}

func (byocAWSProvider) ValidateSetupConfig(config types.ManagedComputeConfig) error {
	if _, err := awsBYOCTemplateURL(config.BYOC.AWS.TemplateURL); err != nil {
		return err
	}
	principal := strings.TrimSpace(config.BYOC.AWS.ControlRolePrincipalARN)
	if principal != "" && !awsPrincipalARNPattern.MatchString(principal) {
		return fmt.Errorf("AWS BYOC control role principal ARN is invalid")
	}
	return nil
}

func (byocAWSProvider) Setup(_ context.Context, input byocProviderSetupInput) (*byocProviderSetupResult, error) {
	if input.ProviderData == nil || input.ProviderData.ResourceName == "" {
		return nil, fmt.Errorf("missing AWS BYOC resource metadata")
	}
	stackName := input.ProviderData.ResourceName
	stackURL := awsCloudFormationStackURL(input.Request.region, stackName)
	templateURL, err := awsBYOCTemplateURL(input.Config.BYOC.AWS.TemplateURL)
	if err != nil {
		return nil, err
	}
	consoleURL := awsCloudFormationConsoleURL(input.Request.region, stackName, templateURL, map[string]string{
		"BeamAutoScalingGroupName":    awsBYOCLabelString(input.ProviderData.Labels, awsBYOCAutoScalingGroupNameLabel, awsBYOCAutoScalingGroupName(input.WorkspaceID, input.Request.poolName)),
		"BeamControlExternalId":       awsBYOCLabelString(input.ProviderData.Labels, awsBYOCControlRoleExternalIDLabel, ""),
		"BeamControlRoleName":         awsBYOCLabelString(input.ProviderData.Labels, awsBYOCControlRoleNameLabel, ""),
		"BeamControlRolePrincipalArn": strings.TrimSpace(input.Config.BYOC.AWS.ControlRolePrincipalARN),
		"BeamGatewayURL":              input.GatewayURL,
		"BeamJoinToken":               input.JoinToken,
		"BeamPoolName":                input.Request.poolName,
		"BeamWorkerImage":             input.WorkerImage,
		"ContainerStartConcurrency":   strconv.FormatUint(uint64(awsBYOCDefaultStartLimit), 10),
		"DesiredCapacity":             strconv.FormatUint(uint64(input.Request.desiredNodes), 10),
		"MaxSize":                     strconv.FormatUint(uint64(input.Request.maxNodes), 10),
		"MinSize":                     strconv.FormatUint(uint64(input.Request.desiredNodes), 10),
		"NetworkSlots":                strconv.FormatUint(uint64(awsBYOCDefaultNetworkSlots), 10),
		"NodeInstanceType":            input.Request.instanceType,
		"RootVolumeSizeGB":            strconv.FormatUint(uint64(awsBYOCDefaultRootVolumeGB), 10),
		"TargetAWSAccountID":          input.Request.accountID,
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

func (byocAWSProvider) Resource(_ string, state *model.PoolState) (*byocProviderResource, error) {
	if state == nil || state.BYOC == nil {
		return nil, fmt.Errorf("missing AWS BYOC resource metadata")
	}
	resource := state.BYOC
	if resource.Provider != string(model.SourceAWS) {
		return nil, fmt.Errorf("pool is not an AWS BYOC pool")
	}
	if resource.Region == "" || resource.ResourceName == "" || resource.ResourceURL == "" || resource.DestroyURL == "" {
		return nil, fmt.Errorf("incomplete AWS BYOC resource metadata")
	}
	instanceType := awsBYOCLabelString(resource.Labels, "instance_type", awsBYOCDefaultInstanceType)
	desiredNodes := awsBYOCLabelUint32(resource.Labels, "desired_nodes", awsBYOCDefaultDesiredNodes)
	hourlyCostMicros := awsBYOCLabelInt64(resource.Labels, "hourly_cost_micros", awsBYOCInstanceHourlyCostMicros(instanceType))
	totalHourlyMicros := awsBYOCLabelInt64(resource.Labels, "total_hourly_cost_micros", hourlyCostMicros*int64(desiredNodes))
	return &byocProviderResource{
		Provider:          string(model.SourceAWS),
		AccountID:         resource.AccountID,
		Region:            resource.Region,
		ResourceName:      resource.ResourceName,
		ResourceURL:       resource.ResourceURL,
		DestroyURL:        resource.DestroyURL,
		InstanceType:      instanceType,
		DesiredNodes:      desiredNodes,
		MaxNodes:          awsBYOCLabelUint32(resource.Labels, "max_nodes", awsBYOCDefaultDesiredNodes),
		TargetSandboxes:   awsBYOCLabelUint32(resource.Labels, "target_sandboxes", awsBYOCDefaultDesiredNodes*awsBYOCSandboxesPerNode),
		SandboxesPerNode:  awsBYOCLabelUint32(resource.Labels, "sandboxes_per_node", awsBYOCSandboxesPerNode),
		HourlyCostMicros:  hourlyCostMicros,
		TotalHourlyMicros: totalHourlyMicros,
		DirectScale:       awsBYOCDirectScaleEnabled(resource.Labels),
	}, nil
}

func (byocAWSProvider) Scale(ctx context.Context, input byocProviderScaleInput) error {
	if input.ProviderData == nil {
		return fmt.Errorf("missing AWS BYOC resource metadata")
	}
	roleARN, externalID, asgName := awsBYOCControlMetadata(input.ProviderData.Labels)
	if roleARN == "" || externalID == "" || asgName == "" {
		return errBYOCDirectScaleUnavailable
	}
	return awsBYOCScaleAutoScalingGroup(ctx, awsBYOCScaleAutoScalingGroupInput{
		Region:               input.ProviderData.Region,
		RoleARN:              roleARN,
		ExternalID:           externalID,
		AutoScalingGroupName: asgName,
		DesiredNodes:         input.Request.desiredNodes,
		MaxNodes:             input.Request.maxNodes,
	})
}

func (byocAWSProvider) ResourceDeleted(ctx context.Context, input byocProviderResourceInput) (bool, error) {
	if input.ProviderData == nil {
		return false, fmt.Errorf("missing AWS BYOC resource metadata")
	}
	roleARN, externalID, _ := awsBYOCControlMetadata(input.ProviderData.Labels)
	if roleARN == "" || externalID == "" || input.ProviderData.ResourceName == "" {
		return false, errBYOCDirectScaleUnavailable
	}
	return awsBYOCResourceDeleted(ctx, awsBYOCResourceDeletedInput{
		Region:     input.ProviderData.Region,
		RoleARN:    roleARN,
		ExternalID: externalID,
		StackName:  input.ProviderData.ResourceName,
	})
}

func (byocAWSProvider) ReleaseMachine(ctx context.Context, input byocProviderReleaseMachineInput) error {
	if input.ProviderData == nil {
		return fmt.Errorf("missing AWS BYOC resource metadata")
	}
	roleARN, externalID, asgName := awsBYOCControlMetadata(input.ProviderData.Labels)
	if roleARN == "" || externalID == "" || asgName == "" {
		return errBYOCDirectScaleUnavailable
	}
	return awsBYOCReleaseMachine(ctx, awsBYOCReleaseMachineInput{
		Region:               input.ProviderData.Region,
		RoleARN:              roleARN,
		ExternalID:           externalID,
		AutoScalingGroupName: asgName,
		Machine:              input.Machine,
	})
}

func (byocAWSProvider) UpdateScaleState(state *model.PoolState, req byocPoolRequest) error {
	if state == nil || state.BYOC == nil {
		return fmt.Errorf("missing AWS BYOC resource metadata")
	}
	if state.BYOC.Labels == nil {
		state.BYOC.Labels = map[string]string{}
	}
	for key, value := range awsBYOCCapacityLabels(req) {
		state.BYOC.Labels[key] = value
	}
	if state.Config != nil {
		state.Config.Regions = []string{req.region}
	}
	return nil
}

func (byocAWSProvider) ValidateExistingPool(existing *model.PoolState) error {
	if existing != nil && configuredPoolGPU(existing) != "" {
		return fmt.Errorf("AWS BYOC currently supports CPU pools only")
	}
	return nil
}

func awsBYOCTemplateURL(raw string) (string, error) {
	templateURL := strings.TrimSpace(raw)
	if templateURL == "" {
		return "", fmt.Errorf("AWS BYOC template URL is not configured")
	}
	parsed, err := url.Parse(templateURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path == "" {
		return "", fmt.Errorf("AWS BYOC template URL must be an HTTPS S3 object URL")
	}
	if !awsS3TemplateHostRe.MatchString(strings.ToLower(parsed.Host)) {
		return "", fmt.Errorf("AWS BYOC template URL must use a CloudFormation-supported S3 URL")
	}
	return templateURL, nil
}

func awsBYOCCapacityLabels(req byocPoolRequest) map[string]string {
	targetSandboxes := req.desiredNodes * awsBYOCSandboxesPerNode
	hourlyCostMicros := awsBYOCInstanceHourlyCostMicros(req.instanceType)
	totalHourlyMicros := hourlyCostMicros * int64(req.desiredNodes)
	return map[string]string{
		"instance_type":            req.instanceType,
		"desired_nodes":            strconv.FormatUint(uint64(req.desiredNodes), 10),
		"max_nodes":                strconv.FormatUint(uint64(req.maxNodes), 10),
		"target_sandboxes":         strconv.FormatUint(uint64(targetSandboxes), 10),
		"sandboxes_per_node":       strconv.FormatUint(uint64(awsBYOCSandboxesPerNode), 10),
		"hourly_cost_micros":       strconv.FormatInt(hourlyCostMicros, 10),
		"total_hourly_cost_micros": strconv.FormatInt(totalHourlyMicros, 10),
		"capacity_unit":            "concurrent_sandbox",
		"capacity_unit_label":      "concurrent sandboxes",
	}
}

func awsBYOCInstanceHourlyCostMicros(instanceType string) int64 {
	instance, ok := awsBYOCInstanceCatalog[instanceType]
	if !ok {
		return 0
	}
	return instance.HourlyCostMicros
}

func awsBYOCLabelString(labels map[string]string, key, fallback string) string {
	if labels == nil {
		return fallback
	}
	value := strings.TrimSpace(labels[key])
	if value == "" {
		return fallback
	}
	return value
}

func awsBYOCLabelUint32(labels map[string]string, key string, fallback uint32) uint32 {
	if labels == nil {
		return fallback
	}
	value, err := strconv.ParseUint(strings.TrimSpace(labels[key]), 10, 32)
	if err != nil || value == 0 {
		return fallback
	}
	return uint32(value)
}

func awsBYOCLabelInt64(labels map[string]string, key string, fallback int64) int64 {
	if labels == nil {
		return fallback
	}
	value, err := strconv.ParseInt(strings.TrimSpace(labels[key]), 10, 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func awsBYOCDirectScaleEnabled(labels map[string]string) bool {
	return awsBYOCLabelString(labels, awsBYOCControlRoleArnLabel, "") != "" &&
		awsBYOCLabelString(labels, awsBYOCControlRoleExternalIDLabel, "") != "" &&
		awsBYOCLabelString(labels, awsBYOCAutoScalingGroupNameLabel, "") != ""
}

func awsBYOCControlMetadata(labels map[string]string) (roleARN, externalID, asgName string) {
	return awsBYOCLabelString(labels, awsBYOCControlRoleArnLabel, ""),
		awsBYOCLabelString(labels, awsBYOCControlRoleExternalIDLabel, ""),
		awsBYOCLabelString(labels, awsBYOCAutoScalingGroupNameLabel, "")
}

type awsBYOCScaleAutoScalingGroupInput struct {
	Region               string
	RoleARN              string
	ExternalID           string
	AutoScalingGroupName string
	DesiredNodes         uint32
	MaxNodes             uint32
}

var awsBYOCScaleAutoScalingGroup = realAWSBYOCScaleAutoScalingGroup
var awsBYOCResourceDeleted = realAWSBYOCResourceDeleted
var awsBYOCReleaseMachine = realAWSBYOCReleaseMachine

type awsBYOCResourceDeletedInput struct {
	Region     string
	RoleARN    string
	ExternalID string
	StackName  string
}

type awsBYOCReleaseMachineInput struct {
	Region               string
	RoleARN              string
	ExternalID           string
	AutoScalingGroupName string
	Machine              *model.AgentTokenState
}

func realAWSBYOCScaleAutoScalingGroup(ctx context.Context, input awsBYOCScaleAutoScalingGroupInput) error {
	if input.Region == "" || input.RoleARN == "" || input.ExternalID == "" || input.AutoScalingGroupName == "" {
		return errBYOCDirectScaleUnavailable
	}
	cfg, err := awsBYOCControlConfig(ctx, input.Region, input.RoleARN, input.ExternalID, "beam-byoc-scale")
	if err != nil {
		return err
	}
	client := autoscaling.NewFromConfig(cfg)
	if input.MaxNodes < input.DesiredNodes {
		input.MaxNodes = input.DesiredNodes
	}
	_, err = client.UpdateAutoScalingGroup(ctx, &autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(input.AutoScalingGroupName),
		DesiredCapacity:      aws.Int32(int32(input.DesiredNodes)),
		MaxSize:              aws.Int32(int32(input.MaxNodes)),
		MinSize:              aws.Int32(int32(input.DesiredNodes)),
	})
	if err != nil {
		return fmt.Errorf("update AWS BYOC node group: %w", err)
	}
	return nil
}

func realAWSBYOCResourceDeleted(ctx context.Context, input awsBYOCResourceDeletedInput) (bool, error) {
	if input.Region == "" || input.RoleARN == "" || input.ExternalID == "" || input.StackName == "" {
		return false, errBYOCDirectScaleUnavailable
	}
	cfg, err := awsBYOCControlConfig(ctx, input.Region, input.RoleARN, input.ExternalID, "beam-byoc-stack-check")
	if err != nil {
		return false, err
	}
	client := cloudformation.NewFromConfig(cfg)
	out, err := client.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{
		StackName: aws.String(input.StackName),
	})
	if err != nil {
		if awsBYOCCloudFormationStackNotFound(err) {
			return false, errBYOCProviderResourceNotFound
		}
		if awsBYOCControlRoleUnavailable(err) {
			return false, errBYOCProviderControlUnavailable
		}
		return false, fmt.Errorf("describe AWS BYOC stack: %w", err)
	}
	if len(out.Stacks) == 0 {
		return false, errBYOCProviderResourceNotFound
	}
	status := out.Stacks[0].StackStatus
	if awsBYOCStackStatusCreating(status) {
		return false, nil
	}
	if awsBYOCStackStatusDeleted(status) {
		return true, nil
	}
	return false, nil
}

func awsBYOCStackStatusCreating(status cftypes.StackStatus) bool {
	switch status {
	case cftypes.StackStatusReviewInProgress,
		cftypes.StackStatusCreateInProgress,
		cftypes.StackStatusImportInProgress,
		cftypes.StackStatusUpdateInProgress,
		cftypes.StackStatusUpdateCompleteCleanupInProgress:
		return true
	default:
		return false
	}
}

func awsBYOCStackStatusDeleted(status cftypes.StackStatus) bool {
	switch status {
	case cftypes.StackStatusDeleteInProgress, cftypes.StackStatusDeleteComplete:
		return true
	default:
		return false
	}
}

func realAWSBYOCReleaseMachine(ctx context.Context, input awsBYOCReleaseMachineInput) error {
	if input.Region == "" || input.RoleARN == "" || input.ExternalID == "" || input.AutoScalingGroupName == "" {
		return errBYOCDirectScaleUnavailable
	}
	cfg, err := awsBYOCControlConfig(ctx, input.Region, input.RoleARN, input.ExternalID, "beam-byoc-release")
	if err != nil {
		return err
	}
	instanceID, err := awsBYOCResolveMachineInstanceID(ctx, cfg, input)
	if err != nil {
		if errors.Is(err, errAWSBYOCResourceNotFound) {
			return nil
		}
		return err
	}
	_, err = autoscaling.NewFromConfig(cfg).TerminateInstanceInAutoScalingGroup(ctx, &autoscaling.TerminateInstanceInAutoScalingGroupInput{
		InstanceId:                     aws.String(instanceID),
		ShouldDecrementDesiredCapacity: aws.Bool(false),
	})
	if err != nil {
		return fmt.Errorf("terminate AWS BYOC node %s: %w", instanceID, err)
	}
	return nil
}

func awsBYOCControlConfig(ctx context.Context, region, roleARN, externalID, sessionName string) (aws.Config, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return aws.Config{}, fmt.Errorf("load AWS config: %w", err)
	}
	provider := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), roleARN, func(options *stscreds.AssumeRoleOptions) {
		options.ExternalID = aws.String(externalID)
		options.RoleSessionName = sessionName
	})
	cfg.Credentials = aws.NewCredentialsCache(provider)
	return cfg, nil
}

func awsBYOCResolveMachineInstanceID(ctx context.Context, cfg aws.Config, input awsBYOCReleaseMachineInput) (string, error) {
	instanceIDs, err := awsBYOCAutoScalingGroupInstanceIDs(ctx, cfg, input.AutoScalingGroupName)
	if err != nil {
		return "", err
	}
	if len(instanceIDs) == 0 {
		return "", errAWSBYOCResourceNotFound
	}

	candidateIDs := awsBYOCMachineInstanceIDCandidates(input.Machine)
	for _, instanceID := range instanceIDs {
		if candidateIDs[instanceID] {
			return instanceID, nil
		}
	}
	if len(candidateIDs) > 0 {
		return "", errAWSBYOCResourceNotFound
	}

	out, err := ec2.NewFromConfig(cfg).DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: instanceIDs,
	})
	if err != nil {
		return "", fmt.Errorf("describe AWS BYOC nodes: %w", err)
	}
	for _, reservation := range out.Reservations {
		for _, instance := range reservation.Instances {
			if awsBYOCMachineMatchesInstance(input.Machine, instance) && instance.InstanceId != nil {
				return *instance.InstanceId, nil
			}
		}
	}
	machineID := ""
	if input.Machine != nil {
		machineID = input.Machine.MachineID
	}
	return "", fmt.Errorf("unable to find AWS BYOC instance for machine %q in Auto Scaling Group %q", machineID, input.AutoScalingGroupName)
}

func awsBYOCAutoScalingGroupInstanceIDs(ctx context.Context, cfg aws.Config, asgName string) ([]string, error) {
	out, err := autoscaling.NewFromConfig(cfg).DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{asgName},
	})
	if err != nil {
		return nil, fmt.Errorf("describe AWS BYOC Auto Scaling Group: %w", err)
	}
	if len(out.AutoScalingGroups) == 0 {
		return nil, errAWSBYOCResourceNotFound
	}
	instanceIDs := make([]string, 0, len(out.AutoScalingGroups[0].Instances))
	for _, instance := range out.AutoScalingGroups[0].Instances {
		if instance.InstanceId == nil || *instance.InstanceId == "" {
			continue
		}
		instanceIDs = append(instanceIDs, *instance.InstanceId)
	}
	return instanceIDs, nil
}

func awsBYOCMachineInstanceIDCandidates(machine *model.AgentTokenState) map[string]bool {
	candidates := map[string]bool{}
	if machine == nil {
		return candidates
	}
	for _, value := range []string{machine.MachineFingerprint, machine.Hostname, machine.MachineID} {
		value = strings.TrimSpace(value)
		if awsEC2InstanceIDRe.MatchString(value) {
			candidates[value] = true
		}
	}
	return candidates
}

func awsBYOCMachineMatchesInstance(machine *model.AgentTokenState, instance ec2types.Instance) bool {
	if machine == nil {
		return false
	}
	candidates := awsBYOCMachineHostCandidates(machine)
	for _, value := range []string{
		aws.ToString(instance.PrivateDnsName),
		aws.ToString(instance.PublicDnsName),
		aws.ToString(instance.PrivateIpAddress),
		aws.ToString(instance.InstanceId),
	} {
		for _, candidate := range normalizedAWSBYOCIdentityValues(value) {
			if candidates[candidate] {
				return true
			}
		}
	}
	return false
}

func awsBYOCMachineHostCandidates(machine *model.AgentTokenState) map[string]bool {
	candidates := map[string]bool{}
	if machine == nil {
		return candidates
	}
	for _, value := range []string{machine.Hostname, machine.MachineFingerprint} {
		for _, candidate := range normalizedAWSBYOCIdentityValues(value) {
			candidates[candidate] = true
		}
	}
	return candidates
}

func normalizedAWSBYOCIdentityValues(value string) []string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	values := []string{value}
	if host, _, ok := strings.Cut(value, "."); ok && host != "" {
		values = append(values, host)
	}
	if ip := awsBYOCPrivateDNSHostIP(value); ip != "" {
		values = append(values, ip)
	}
	return values
}

func awsBYOCPrivateDNSHostIP(hostname string) string {
	host, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(hostname)), ".")
	if !strings.HasPrefix(host, "ip-") {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(host, "ip-"), "-")
	if len(parts) != 4 {
		return ""
	}
	for _, part := range parts {
		if _, err := strconv.Atoi(part); err != nil {
			return ""
		}
	}
	return strings.Join(parts, ".")
}

func awsBYOCCloudFormationStackNotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return strings.EqualFold(apiErr.ErrorCode(), "ValidationError") && strings.Contains(strings.ToLower(apiErr.ErrorMessage()), "does not exist")
}

func awsBYOCControlRoleUnavailable(err error) bool {
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "sts:assumerole") && !strings.Contains(msg, "assume role") && !strings.Contains(msg, "assumerole") {
		return false
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := strings.ToLower(apiErr.ErrorCode())
		switch code {
		case "accessdenied", "accessdeniedexception", "nosuchentity", "validationerror":
			return true
		}
	}

	return strings.Contains(msg, "not authorized") ||
		strings.Contains(msg, "cannot be assumed") ||
		strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "not found")
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

func awsBYOCAutoScalingGroupName(workspaceID, poolName string) string {
	return awsBYOCNamedResource("asg", workspaceID, poolName, 255)
}

func awsBYOCControlRoleName(workspaceID, poolName string) string {
	return awsBYOCNamedResource("control", workspaceID, poolName, 64)
}

func awsBYOCControlExternalID(workspaceID, poolName string) string {
	sum := sha256.Sum256([]byte("beam-byoc-external-id\x00" + workspaceID + "\x00" + poolName))
	return "beam-byoc-" + hex.EncodeToString(sum[:16])
}

func awsBYOCControlRoleARN(region, accountID, roleName string) string {
	return fmt.Sprintf("arn:%s:iam::%s:role/%s", awsPartitionForRegion(region), accountID, roleName)
}

func awsBYOCNamedResource(kind, workspaceID, poolName string, maxLen int) string {
	part := cloudFormationNamePart(poolName)
	if part == "" {
		part = "pool"
	}
	sum := sha256.Sum256([]byte(kind + "\x00" + workspaceID + "\x00" + poolName))
	suffix := hex.EncodeToString(sum[:4])
	prefix := "beam-" + kind + "-"
	maxPart := maxLen - len(prefix) - len("-") - len(suffix)
	if maxPart < 1 {
		maxPart = 1
	}
	if len(part) > maxPart {
		part = strings.Trim(part[:maxPart], "-")
	}
	if part == "" {
		part = "pool"
	}
	return fmt.Sprintf("%s%s-%s", prefix, part, suffix)
}

func awsPartitionForRegion(region string) string {
	switch {
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	default:
		return "aws"
	}
}

func cloudFormationNamePart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = cloudFormationPartRe.ReplaceAllString(value, "-")
	value = cloudFormationDashRe.ReplaceAllString(value, "-")
	return strings.Trim(value, "-")
}

//go:embed templates/aws/byoc.yaml
var awsBYOCTemplate string

func AWSBYOCTemplate() string {
	return awsBYOCTemplate
}
