package taskqueue

import (
	"math"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
)

type taskQueueAutoscalerSample struct {
	QueueLength       int64
	RunningTasks      int64
	CurrentContainers int
	TaskDuration      float64
}

// taskQueueAutoscalerSampleFunc retrieves an autoscaling sample from the task queue instance
func taskQueueAutoscalerSampleFunc(i *taskQueueInstance) (*taskQueueAutoscalerSample, error) {
	runningTasks, err := i.client.TasksRunning(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		runningTasks = -1
	}

	taskDuration, err := i.client.GetTaskDuration(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		taskDuration = -1
	}

	queueLength, err := i.TaskRepo.TasksInFlight(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		queueLength = -1
	}

	currentContainers := 0
	state, err := i.State()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &taskQueueAutoscalerSample{
		QueueLength:       int64(queueLength),
		RunningTasks:      int64(runningTasks),
		CurrentContainers: currentContainers,
		TaskDuration:      taskDuration,
	}

	return sample, nil
}

// taskQueueScaleFunc scales based on the number of items in the queue
func taskQueueScaleFunc(i *taskQueueInstance, s *taskQueueAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if s.QueueLength == 0 {
		desiredContainers = 0
	} else {
		if s.QueueLength == -1 {
			return &abstractions.AutoscalerResult{
				ResultValid: false,
			}
		}

		desiredContainers = int(s.QueueLength / int64(i.StubConfig.Autoscaler.TasksPerContainer))
		if s.QueueLength%int64(i.StubConfig.Autoscaler.TasksPerContainer) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or the limit specified on the gateway config (whichever is lower)
		maxReplicas := math.Min(float64(i.StubConfig.Autoscaler.MaxContainers), float64(i.AppConfig.GatewayService.StubLimits.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

// taskQueueScaleFunc scales up and down based on the server container timeout
func taskQueueServeScaleFunc(i *taskQueueInstance, sample *taskQueueAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 1

	lockKey := common.RedisKeys.SchedulerServeLock(i.Workspace.Name, i.Stub.ExternalId)
	exists, err := i.Rdb.Exists(i.Ctx, lockKey).Result()
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
