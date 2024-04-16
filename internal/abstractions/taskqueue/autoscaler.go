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

// taskQueueSampleFunc retrieves an autoscaling sample from the task queue instance
func taskQueueSampleFunc(i *taskQueueInstance) (*taskQueueAutoscalerSample, error) {
	queueLength, err := i.client.QueueLength(i.ctx, i.workspace.Name, i.stub.ExternalId)
	if err != nil {
		queueLength = -1
	}

	runningTasks, err := i.client.TasksRunning(i.ctx, i.workspace.Name, i.stub.ExternalId)
	if err != nil {
		runningTasks = -1
	}

	taskDuration, err := i.client.GetTaskDuration(i.ctx, i.workspace.Name, i.stub.ExternalId)
	if err != nil {
		taskDuration = -1
	}

	currentContainers := 0
	state, err := i.state()
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
func taskQueueScaleFunc(i *taskQueueInstance, s *taskQueueAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if s.QueueLength == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(s.QueueLength / int64(i.stubConfig.Concurrency))
		if s.QueueLength%int64(i.stubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(i.stubConfig.MaxContainers), float64(abstractions.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
