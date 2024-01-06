package taskqueue

import (
	"context"
	"log"
	"math"
	"time"

	rolling "github.com/asecurityteam/rolling"
)

const (
	AutoScalingModeQueueDepth int = 0
	AutoScalingModeDefault    int = 1
)

type autoscaler struct {
	instance         *taskQueueInstance
	autoscalingMode  int
	samples          *autoscalingWindows
	mostRecentSample *autoscalerSample
}

type autoscalingWindows struct {
	QueueLength       *rolling.PointPolicy
	RunningTasks      *rolling.PointPolicy
	CurrentContainers *rolling.PointPolicy
	TaskDuration      *rolling.PointPolicy
}

type autoscalerSample struct {
	QueueLength       int64
	RunningTasks      int64
	CurrentContainers int
	TaskDuration      float64
}

type AutoscaleResult struct {
	DesiredContainers int
	ResultValid       bool
}

const MaxReplicas uint = 5                                              // Maximum number of desired replicas that can be returned
const WindowSize int = 60                                               // Number of samples in the sampling window
const SampleRate time.Duration = time.Duration(1000) * time.Millisecond // Time between samples

// Create a new autoscaler
func newAutoscaler(i *taskQueueInstance) *autoscaler {
	var autoscalingMode = AutoScalingModeDefault

	return &autoscaler{
		instance:        i,
		autoscalingMode: autoscalingMode,
		samples: &autoscalingWindows{
			QueueLength:       rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
			RunningTasks:      rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
			CurrentContainers: rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
			TaskDuration:      rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
		},
		mostRecentSample: nil,
	}
}

// Retrieve a datapoint from the request bucket
func (as *autoscaler) sample() (*autoscalerSample, error) {
	instance := as.instance

	queueLength, err := instance.client.QueueLength(instance.workspace.Name, instance.stub.ExternalId)
	if err != nil {
		queueLength = -1
	}

	runningTasks, err := instance.client.TasksRunning(instance.workspace.Name, instance.stub.ExternalId)
	if err != nil {
		runningTasks = -1
	}

	taskDuration, err := instance.client.GetTaskDuration(instance.workspace.Name, instance.stub.ExternalId)
	if err != nil {
		taskDuration = -1
	}

	currentContainers := 0
	state, err := instance.state()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &autoscalerSample{
		QueueLength:       queueLength,
		RunningTasks:      int64(runningTasks),
		CurrentContainers: currentContainers,
		TaskDuration:      taskDuration,
	}

	if sample.RunningTasks >= 0 {
		sample.QueueLength = sample.QueueLength + int64(runningTasks)
	}

	log.Printf("autoscaler sample: %+v\n", sample)

	// Cache most recent autoscaler sample so RequestBucket can access without hitting redis
	as.mostRecentSample = sample

	return sample, nil
}

// Start the autoscaler
func (as *autoscaler) start(ctx context.Context) {
	if as.autoscalingMode == -1 {
		return
	}

	// Fill windows with -1 so we can avoid using those values in the scaling logic
	for i := 0; i < WindowSize; i += 1 {
		as.samples.QueueLength.Append(-1)
		as.samples.RunningTasks.Append(-1)
		as.samples.CurrentContainers.Append(-1)
		as.samples.TaskDuration.Append(-1)
	}

	ticker := time.NewTicker(SampleRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample, err := as.sample()
			if err != nil {
				continue
			}

			// Append samples to moving windows
			as.samples.QueueLength.Append(float64(sample.QueueLength))
			as.samples.CurrentContainers.Append(float64(sample.CurrentContainers))
			as.samples.RunningTasks.Append(float64(sample.RunningTasks))
			as.samples.TaskDuration.Append(float64(sample.TaskDuration))

			var scaleResult *AutoscaleResult = nil
			switch as.autoscalingMode {
			case AutoScalingModeQueueDepth:
				scaleResult = as.scaleByQueueDepth(sample)
			case AutoScalingModeDefault:
				scaleResult = as.scaleToOne(sample)
			default:
			}

			// If there is a gateway:taskqueue_containers:<app_id> key in the store, use this value instead
			// This basically override any autoscaling result calculated above

			// containerCountOverride, err := as.stateStore.MinContainerCount(as.requestBucket.AppId)
			// if err == nil && scaleResult.DesiredContainers != 0 {
			// 	scaleResult.DesiredContainers = containerCountOverride
			// }

			if scaleResult != nil && scaleResult.ResultValid {
				as.instance.scaleEventChan <- scaleResult.DesiredContainers // Send autoscaling result to request bucket
			}
		}
	}
}

// Scale up to 1 if the queue has items in it - not really autoscaling, just spinning the container up and down
func (as *autoscaler) scaleToOne(sample *autoscalerSample) *AutoscaleResult {
	desiredContainers := 0

	if sample.QueueLength > 0 || sample.RunningTasks > 0 {
		desiredContainers = 1
	}

	return &AutoscaleResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}

}

// Scale based on the number of items in the queue
func (as *autoscaler) scaleByQueueDepth(sample *autoscalerSample) *AutoscaleResult {
	desiredContainers := 0

	if sample.QueueLength == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(sample.QueueLength / int64(as.instance.stubConfig.Concurrency))
		if sample.QueueLength%int64(as.instance.stubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(as.instance.stubConfig.MaxContainers), float64(MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &AutoscaleResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
