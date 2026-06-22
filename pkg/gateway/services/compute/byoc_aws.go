package compute

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

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
)

var (
	awsRegionPattern       = regexp.MustCompile(`^[a-z]{2}(-gov)?-[a-z0-9-]+-\d$`)
	awsInstanceTypePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,63}$`)
	awsAccountIDPattern    = regexp.MustCompile(`^\d{12}$`)
	awsS3TemplateHostRe    = regexp.MustCompile(`^(?:s3[.-][a-z0-9-]+|[a-z0-9][a-z0-9.-]*\.s3[.-][a-z0-9-]+)\.amazonaws\.com$`)
	cloudFormationPartRe   = regexp.MustCompile(`[^a-z0-9-]+`)
	cloudFormationDashRe   = regexp.MustCompile(`-+`)
	awsBYOCInstanceTypes   = map[string]struct{}{
		// Storage optimized instances with local NVMe for cache performance.
		"i4i.large":   {},
		"i4i.xlarge":  {},
		"i4i.2xlarge": {},
		"i4i.4xlarge": {},
	}
)

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
	if req.accountID == "" {
		return byocPoolOnboardingRequest{}, fmt.Errorf("AWS account ID is required")
	}
	if !awsAccountIDPattern.MatchString(req.accountID) {
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
		Labels:       awsBYOCCapacityLabels(req),
	}
}

func (byocAWSProvider) ValidateSetupConfig(config types.ManagedComputeConfig) error {
	_, err := awsBYOCTemplateURL(config.BYOC.AWS.TemplateURL)
	return err
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
		"TargetAWSAccountID":        input.Request.accountID,
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
	return &byocProviderResource{
		Provider:         string(model.SourceAWS),
		AccountID:        resource.AccountID,
		Region:           resource.Region,
		ResourceName:     resource.ResourceName,
		ResourceURL:      resource.ResourceURL,
		DestroyURL:       resource.DestroyURL,
		InstanceType:     awsBYOCLabelString(resource.Labels, "instance_type", awsBYOCDefaultInstanceType),
		DesiredNodes:     awsBYOCLabelUint32(resource.Labels, "desired_nodes", awsBYOCDefaultDesiredNodes),
		MaxNodes:         awsBYOCLabelUint32(resource.Labels, "max_nodes", awsBYOCDefaultDesiredNodes),
		TargetSandboxes:  awsBYOCLabelUint32(resource.Labels, "target_sandboxes", awsBYOCDefaultDesiredNodes*awsBYOCSandboxesPerNode),
		SandboxesPerNode: awsBYOCLabelUint32(resource.Labels, "sandboxes_per_node", awsBYOCSandboxesPerNode),
	}, nil
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

func awsBYOCCapacityLabels(req byocPoolOnboardingRequest) map[string]string {
	targetSandboxes := req.desiredNodes * awsBYOCSandboxesPerNode
	return map[string]string{
		"instance_type":       req.instanceType,
		"desired_nodes":       strconv.FormatUint(uint64(req.desiredNodes), 10),
		"max_nodes":           strconv.FormatUint(uint64(req.maxNodes), 10),
		"target_sandboxes":    strconv.FormatUint(uint64(targetSandboxes), 10),
		"sandboxes_per_node":  strconv.FormatUint(uint64(awsBYOCSandboxesPerNode), 10),
		"capacity_unit":       "normal_sandbox",
		"capacity_unit_label": "normal sandboxes",
	}
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
