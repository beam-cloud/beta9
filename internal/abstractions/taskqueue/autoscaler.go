package taskqueue

import (
	"math"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
)

type taskQueueAutoscalerSample struct {
	QueueLength       int64
	RunningTasks      int64
	CurrentContainers int
	TaskDuration      float64
}

// taskQueueAutoscalerSampleFunc retrieves an autoscaling sample from the task queue instance
func taskQueueAutoscalerSampleFunc(i *taskQueueInstance) (*taskQueueAutoscalerSample, error) {
	queueLength, err := i.client.QueueLength(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		queueLength = -1
	}

	runningTasks, err := i.client.TasksRunning(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		runningTasks = -1
	}

	taskDuration, err := i.client.GetTaskDuration(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		taskDuration = -1
	}

	currentContainers := 0
	state, err := i.State()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &taskQueueAutoscalerSample{
		QueueLength:       queueLength,
		RunningTasks:      int64(runningTasks),
		CurrentContainers: currentContainers,
		TaskDuration:      taskDuration,
	}

	if sample.RunningTasks >= 0 {
		sample.QueueLength = sample.QueueLength + int64(runningTasks)
	}

	return sample, nil
}

// taskQueueScaleFunc scales based on the number of items in the queue
func taskQueueDeploymentScaleFunc(i *taskQueueInstance, s *taskQueueAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if s.QueueLength == 0 {
		desiredContainers = 0
	} else {
		if s.QueueLength == -1 {
			return &abstractions.AutoscalerResult{
				DesiredContainers: 0,
				ResultValid:       false,
			}
		}

		desiredContainers = int(s.QueueLength / int64(i.StubConfig.Concurrency))
		if s.QueueLength%int64(i.StubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(i.StubConfig.MaxContainers), float64(abstractions.MaxReplicas))
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

	timeoutKey := Keys.taskQueueServeLock(i.Workspace.Name, i.Stub.ExternalId)
	exists, err := i.Rdb.Exists(i.Ctx, timeoutKey).Result()
	if err != nil {
		return &abstractions.AutoscalerResult{
			ResultValid: false,
		}
	}

	if exists == 0 && sample.RunningTasks == 0 {
		desiredContainers = 0
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
