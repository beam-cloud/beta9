package gatewayservices

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	rolloutModeAuto      = "auto"
	rolloutModeBlueGreen = "blue-green"
	rolloutModeReplace   = "replace"

	rolloutActionBlueGreen = "blue-green"
	rolloutActionReplace   = "replace"
	rolloutActionBlocked   = "blocked"
)

type privatePodRolloutDecision struct {
	action       string
	warning      string
	errorMessage string
}

func (gws *GatewayService) privatePodRolloutDecision(ctx context.Context, workspace *types.Workspace, stub *types.StubWithRelated, config *types.StubConfigV1, latest *types.DeploymentWithRelated, mode string) privatePodRolloutDecision {
	mode = normalizeRolloutMode(mode)
	if mode == "" {
		return privatePodRolloutDecision{errorMessage: "unsupported rollout mode"}
	}
	if !gws.shouldCheckPrivatePodRollout(ctx, workspace, stub, config, latest) {
		return privatePodRolloutDecision{}
	}

	request := containerRequestForRolloutCapacity(workspace, stub, config)
	replicas := config.Autoscaler.MinContainers

	if mode != rolloutModeReplace {
		spare := gws.checkRolloutCapacity(request, replicas, "", nil)
		if spare.CanSchedule {
			return privatePodRolloutDecision{action: rolloutActionBlueGreen}
		}
		if mode == rolloutModeBlueGreen {
			return privatePodRolloutDecision{
				action:       rolloutActionBlocked,
				errorMessage: "private pool rollout blocked: blue-green rollout requires enough spare capacity to start the new revision before stopping the current revision",
			}
		}
	}

	activeContainers, err := gws.containerRepo.GetActiveContainersByStubId(latest.Stub.ExternalId)
	if err != nil {
		return privatePodRolloutDecision{action: rolloutActionBlocked, errorMessage: "failed to check active private pool containers before rollout"}
	}
	replacement := gws.checkRolloutCapacity(request, replicas, latest.Stub.ExternalId, activeContainers)
	if !replacement.CanSchedule {
		return privatePodRolloutDecision{
			action:       rolloutActionBlocked,
			errorMessage: "private pool rollout blocked: the new revision is not predicted to fit even after replacing the current always-on revision",
		}
	}

	return privatePodRolloutDecision{
		action:  rolloutActionReplace,
		warning: "Private pool rollout will replace the current always-on revision before starting the new revision. Expected downtime is possible; spare capacity is insufficient for blue-green, but the new revision is predicted to fit after replacement.",
	}
}

func normalizeRolloutMode(mode string) string {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		return rolloutModeAuto
	}
	switch mode {
	case rolloutModeAuto, rolloutModeBlueGreen, rolloutModeReplace:
		return mode
	default:
		return ""
	}
}

func (gws *GatewayService) shouldCheckPrivatePodRollout(ctx context.Context, workspace *types.Workspace, stub *types.StubWithRelated, config *types.StubConfigV1, latest *types.DeploymentWithRelated) bool {
	if workspace == nil || stub == nil || config == nil || latest == nil || !latest.Active {
		return false
	}
	if stub.Type != types.StubType(types.StubTypePodDeployment) || latest.Stub.Type != types.StubType(types.StubTypePodDeployment) {
		return false
	}
	if !podDeploymentAlwaysOn(config) {
		return false
	}
	var latestConfig types.StubConfigV1
	if err := json.Unmarshal([]byte(latest.Stub.Config), &latestConfig); err != nil {
		return false
	}
	return podDeploymentAlwaysOn(&latestConfig) && gws.usesPrivatePool(ctx, workspace.ExternalId, config)
}

func podDeploymentAlwaysOn(config *types.StubConfigV1) bool {
	return config != nil && config.Autoscaler != nil && config.Autoscaler.MinContainers > 0
}

func containerRequestForRolloutCapacity(workspace *types.Workspace, stub *types.StubWithRelated, config *types.StubConfigV1) *types.ContainerRequest {
	workspaceID := ""
	if workspace != nil {
		workspaceID = workspace.ExternalId
	}
	return &types.ContainerRequest{
		Cpu:          config.Runtime.Cpu,
		Memory:       config.Runtime.Memory,
		Gpu:          string(config.Runtime.Gpu),
		GpuRequest:   types.GpuTypesToStrings(config.Runtime.Gpus),
		GpuCount:     config.Runtime.GpuCount,
		ImageId:      config.Runtime.ImageId,
		StubId:       stub.ExternalId,
		WorkspaceId:  workspaceID,
		Stub:         *stub,
		PoolSelector: config.PoolSelector(),
		MachineId:    config.MachineID,
	}
}

func (gws *GatewayService) checkRolloutCapacity(request *types.ContainerRequest, replicas uint, ignoreStubID string, activeContainers []types.ContainerState) scheduler.CapacityCheckResult {
	if gws == nil || gws.scheduler == nil {
		return scheduler.CapacityCheckResult{Err: fmt.Errorf("scheduler unavailable")}
	}
	return gws.scheduler.CheckCapacity(request, replicas, ignoreStubID, activeContainers)
}
