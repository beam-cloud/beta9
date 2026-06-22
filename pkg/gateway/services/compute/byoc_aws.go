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
	"github.com/aws/aws-sdk-go-v2/service/sts"
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
	labels := input.ProviderData.Labels
	roleARN := awsBYOCLabelString(labels, awsBYOCControlRoleArnLabel, "")
	externalID := awsBYOCLabelString(labels, awsBYOCControlRoleExternalIDLabel, "")
	asgName := awsBYOCLabelString(labels, awsBYOCAutoScalingGroupNameLabel, "")
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
		"capacity_unit":            "normal_sandbox",
		"capacity_unit_label":      "normal sandboxes",
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

type awsBYOCScaleAutoScalingGroupInput struct {
	Region               string
	RoleARN              string
	ExternalID           string
	AutoScalingGroupName string
	DesiredNodes         uint32
	MaxNodes             uint32
}

var awsBYOCScaleAutoScalingGroup = realAWSBYOCScaleAutoScalingGroup

func realAWSBYOCScaleAutoScalingGroup(ctx context.Context, input awsBYOCScaleAutoScalingGroupInput) error {
	if input.Region == "" || input.RoleARN == "" || input.ExternalID == "" || input.AutoScalingGroupName == "" {
		return errBYOCDirectScaleUnavailable
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(input.Region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	provider := stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), input.RoleARN, func(options *stscreds.AssumeRoleOptions) {
		options.ExternalID = aws.String(input.ExternalID)
		options.RoleSessionName = "beam-byoc-scale"
	})
	cfg.Credentials = aws.NewCredentialsCache(provider)
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
