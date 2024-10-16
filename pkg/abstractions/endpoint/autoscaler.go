package endpoint

import (
	"fmt"
	"math"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"go.opentelemetry.io/otel/attribute"
)

type endpointAutoscalerSample struct {
	TotalRequests     int64
	CurrentContainers int64
}

func endpointSampleFunc(i *endpointInstance) (*endpointAutoscalerSample, error) {
	trace := common.TraceFunc(i.Ctx, "pkg/abstractions/endpoint", "endpointSampleFunc",
		attribute.String("stub.id", i.Stub.ExternalId))
	defer trace.End()

	totalRequests, err := i.TaskRepo.TasksInFlight(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		totalRequests = -1
	}
	trace.Span.AddEvent(fmt.Sprintf("Total requests: %d", totalRequests))

	currentContainers := 0
	state, err := i.State()
	if err != nil {
		currentContainers = -1
	}
	trace.Span.AddEvent(fmt.Sprintf("Current containers: %d", currentContainers))

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &endpointAutoscalerSample{
		TotalRequests:     int64(totalRequests),
		CurrentContainers: int64(currentContainers),
	}

	return sample, nil
}

func endpointDeploymentScaleFunc(i *endpointInstance, s *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if s.TotalRequests == 0 {
		desiredContainers = 0
	} else {
		if s.TotalRequests == -1 {
			return &abstractions.AutoscalerResult{
				ResultValid: false,
			}
		}

		desiredContainers = int(s.TotalRequests / int64(i.StubConfig.Autoscaler.TasksPerContainer))
		if s.TotalRequests%int64(i.StubConfig.Autoscaler.TasksPerContainer) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(i.StubConfig.Autoscaler.MaxContainers), float64(abstractions.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func endpointServeScaleFunc(i *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 1

	timeoutKey := Keys.endpointServeLock(i.Workspace.Name, i.Stub.ExternalId)
	exists, err := i.Rdb.Exists(i.Ctx, timeoutKey).Result()
	if err != nil {
		return &abstractions.AutoscalerResult{
			ResultValid: false,
		}
	}

	if exists == 0 {
		desiredContainers = 0
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
