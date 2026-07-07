package gatewayservices

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	rolloutModeAuto      = "auto"
	rolloutModeBlueGreen = "blue-green"
	rolloutModeReplace   = "replace"

	rolloutActionBlueGreen = "blue-green"
	rolloutActionReplace   = "replace"
)

type deploymentRolloutPlan struct {
	action     string
	warning    string
	stopLatest bool
}

type deploymentRolloutInput struct {
	workspace *types.Workspace
	stub      *types.StubWithRelated
	config    *types.StubConfigV1
	latest    *types.DeploymentWithRelated
	mode      string
}

func (gws *GatewayService) planDeploymentRollout(in deploymentRolloutInput) (deploymentRolloutPlan, error) {
	mode := normalizeRolloutMode(in.mode)
	if !validRolloutMode(mode) {
		return deploymentRolloutPlan{}, fmt.Errorf("unsupported rollout mode; expected auto, blue-green, or replace")
	}
	if in.latest != nil && in.stub != nil && in.stub.Type == types.StubType(types.StubTypeScheduledJobDeployment) {
		return deploymentRolloutPlan{stopLatest: true}, nil
	}
	if !deploymentRolloutTarget(in) {
		return deploymentRolloutPlan{}, nil
	}
	if mode == rolloutModeAuto {
		return deploymentRolloutPlan{}, nil
	}

	request := rolloutContainerRequest(in)
	replicas := rolloutReplicas(in.config)
	if mode == rolloutModeBlueGreen {
		if gws.rolloutCapacityAvailable(request, replicas, "", nil) {
			return deploymentRolloutPlan{action: rolloutActionBlueGreen}, nil
		}
		return deploymentRolloutPlan{}, fmt.Errorf("blue-green rollout requires enough spare capacity to start the new revision before stopping the current revision")
	}

	activeContainers, err := gws.containerRepo.GetActiveContainersByStubId(in.latest.Stub.ExternalId)
	if err != nil {
		return deploymentRolloutPlan{}, fmt.Errorf("failed to check active containers before rollout: %w", err)
	}
	if !gws.rolloutCapacityAvailable(request, replicas, in.latest.Stub.ExternalId, activeContainers) {
		return deploymentRolloutPlan{}, fmt.Errorf("rollout blocked: the new revision is not predicted to fit even after replacing the current always-on revision")
	}

	return deploymentRolloutPlan{
		action:     rolloutActionReplace,
		stopLatest: true,
		warning:    "Rollout will replace the current always-on revision before starting the new revision. Expected downtime is possible; the new revision is predicted to fit after replacement.",
	}, nil
}

func (gws *GatewayService) activeDeploymentsForRollout(ctx context.Context, workspaceID uint, name, stubType string) ([]types.DeploymentWithRelated, error) {
	active := true
	deployments, err := gws.backendRepo.ListDeploymentsWithRelated(ctx, types.DeploymentFilter{
		WorkspaceID: workspaceID,
		StubType:    types.StringSlice{stubType},
		Name:        name,
		Active:      &active,
	})
	if err != nil {
		return nil, err
	}

	activeDeployments := make([]types.DeploymentWithRelated, 0, len(deployments))
	for _, deployment := range deployments {
		if deployment.Active && deployment.Deployment.WorkspaceId == workspaceID && deployment.Name == name && deployment.StubType == stubType {
			activeDeployments = append(activeDeployments, deployment)
		}
	}
	return activeDeployments, nil
}

func latestActiveDeployment(deployments []types.DeploymentWithRelated) *types.DeploymentWithRelated {
	var latest *types.DeploymentWithRelated
	for i := range deployments {
		deployment := deployments[i]
		if latest == nil || deployment.Version > latest.Version {
			latest = &deployment
		}
	}
	return latest
}

func normalizeRolloutMode(mode string) string {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		return rolloutModeAuto
	}
	return mode
}

func validRolloutMode(mode string) bool {
	switch mode {
	case rolloutModeAuto, rolloutModeBlueGreen, rolloutModeReplace:
		return true
	default:
		return false
	}
}

func deploymentRolloutApplies(in deploymentRolloutInput) bool {
	if !deploymentRolloutTarget(in) {
		return false
	}
	return alwaysOnDeploymentConfig(in.config) && latestAlwaysOnDeployment(in.latest)
}

func deploymentRolloutTarget(in deploymentRolloutInput) bool {
	if in.stub == nil || in.config == nil || in.latest == nil || !in.latest.Active {
		return false
	}
	if !in.stub.Type.IsDeployment() || !in.latest.Stub.Type.IsDeployment() {
		return false
	}
	return true
}

func latestAlwaysOnDeployment(deployment *types.DeploymentWithRelated) bool {
	var latestConfig types.StubConfigV1
	if deployment == nil || json.Unmarshal([]byte(deployment.Stub.Config), &latestConfig) != nil {
		return false
	}
	return alwaysOnDeploymentConfig(&latestConfig)
}

func alwaysOnDeploymentConfig(config *types.StubConfigV1) bool {
	return rolloutReplicas(config) > 0
}

func rolloutReplicas(config *types.StubConfigV1) uint {
	if config == nil {
		return 0
	}
	if config.Autoscaler != nil && config.Autoscaler.MinContainers > 0 {
		return config.Autoscaler.MinContainers
	}
	if config.KeepWarmSeconds < 0 {
		return 1
	}
	return 0
}

func rolloutContainerRequest(in deploymentRolloutInput) *types.ContainerRequest {
	workspaceID := ""
	if in.workspace != nil {
		workspaceID = in.workspace.ExternalId
	}
	return &types.ContainerRequest{
		Cpu:          in.config.Runtime.Cpu,
		Memory:       in.config.Runtime.Memory,
		Gpu:          string(in.config.Runtime.Gpu),
		GpuRequest:   types.GpuTypesToStrings(in.config.Runtime.Gpus),
		GpuCount:     in.config.Runtime.GpuCount,
		ImageId:      in.config.Runtime.ImageId,
		StubId:       in.stub.ExternalId,
		WorkspaceId:  workspaceID,
		Stub:         *in.stub,
		PoolSelector: in.config.PoolSelector(),
		MachineId:    in.config.MachineID,
	}
}

func (gws *GatewayService) rolloutCapacityAvailable(request *types.ContainerRequest, replicas uint, replacedStubID string, activeContainers []types.ContainerState) bool {
	return gws != nil && gws.scheduler != nil &&
		gws.scheduler.CheckCapacity(request, replicas, replacedStubID, activeContainers).CanSchedule
}
