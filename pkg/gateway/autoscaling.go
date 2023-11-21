package gateway

import (
	"math"
	"time"

	rolling "github.com/asecurityteam/rolling"

	"github.com/beam-cloud/beam/pkg/common"
)

const (
	AutoScalingModeMaxRequestLatency int = 0
	AutoScalingModeQueueDepth        int = 1
	AutoScalingModeDefault           int = 2
)

type AutoScaler struct {
	requestBucket    *DeploymentRequestBucket
	stateStore       *common.StateStore
	autoscalingMode  int
	samples          *AutoscalingWindows
	MostRecentSample *AutoscalerSample
}

type AutoscalingWindows struct {
	QueueLength       *rolling.PointPolicy
	RunningTasks      *rolling.PointPolicy
	CurrentContainers *rolling.PointPolicy
	TaskDuration      *rolling.PointPolicy
}

type AutoscalerSample struct {
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
func NewAutoscaler(rb *DeploymentRequestBucket, stateStore *common.StateStore) *AutoScaler {
	var autoscalingMode = AutoScalingModeDefault

	// TODO: Remove "AutoscalingConfig". Keep for backwards compatibility for now.
	if rb.AutoscalingConfig.AutoScalingType == "max_request_latency" {
		autoscalingMode = AutoScalingModeMaxRequestLatency
	}

	// We are making the assumption that only one of these autoscaler structs will be defined
	if rb.AutoscalerConfig.RequestLatency != nil {
		autoscalingMode = AutoScalingModeMaxRequestLatency
	} else if rb.AutoscalerConfig.QueueDepth != nil {
		autoscalingMode = AutoScalingModeQueueDepth
	}

	return &AutoScaler{
		stateStore:      stateStore,
		requestBucket:   rb,
		autoscalingMode: autoscalingMode,
		samples: &AutoscalingWindows{
			QueueLength:       rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
			RunningTasks:      rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
			CurrentContainers: rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
			TaskDuration:      rolling.NewPointPolicy(rolling.NewWindow(WindowSize)),
		},
		MostRecentSample: nil,
	}
}

// Retrieve a datapoint from the request bucket
func (as *AutoScaler) sample() (*AutoscalerSample, error) {
	requestBucket := as.requestBucket

	queueLength, err := requestBucket.queueClient.QueueLength(requestBucket.IdentityId, requestBucket.Name)
	if err != nil {
		queueLength = -1
	}

	runningTasks, err := requestBucket.queueClient.TasksRunning(requestBucket.IdentityId, requestBucket.Name)
	if err != nil {
		runningTasks = -1
	}

	taskDuration, err := requestBucket.queueClient.GetTaskDuration(requestBucket.IdentityId, requestBucket.Name)
	if err != nil {
		taskDuration = -1
	}

	tasksInFlight, err := requestBucket.taskRepo.GetTasksInFlight(requestBucket.Name, requestBucket.IdentityId)
	if err != nil {
		tasksInFlight = -1
	}

	currentContainers := 0
	state, err := requestBucket.bucketRepo.GetRequestBucketState()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &AutoscalerSample{
		QueueLength:       queueLength,
		RunningTasks:      int64(runningTasks),
		CurrentContainers: currentContainers,
		TaskDuration:      taskDuration,
	}

	if sample.RunningTasks >= 0 {
		sample.QueueLength = sample.QueueLength + int64(runningTasks)
	}

	if tasksInFlight > 0 {
		sample.QueueLength += int64(tasksInFlight)
	}

	// Cache most recent autoscaler sample so RequestBucket can access without hitting redis
	as.MostRecentSample = sample

	return sample, nil
}

// Start the autoscaler
func (as *AutoScaler) Start() {
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

	for range ticker.C {
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
		case AutoScalingModeMaxRequestLatency:
			scaleResult = as.scaleByMaxRequestLatency(sample)
		case AutoScalingModeQueueDepth:
			scaleResult = as.scaleByQueueDepth(sample)
		case AutoScalingModeDefault:
			scaleResult = as.scaleToOne(sample)
		default:
		}

		// If there is a activator:deployment_containers:<app_id> key in the store, use this value instead
		// This basically override any autoscaling result calculated above
		containerCountOverride, err := as.stateStore.MinContainerCount(as.requestBucket.AppId)
		if err == nil && scaleResult.DesiredContainers != 0 {
			scaleResult.DesiredContainers = containerCountOverride
		}

		if scaleResult != nil && scaleResult.ResultValid {
			as.requestBucket.ScaleEventChan <- scaleResult.DesiredContainers // Send autoscaling result to request bucket
		}
	}
}

// Scale up to 1 if the queue has items in it - not really autoscaling, just spinning the container up and down
func (as *AutoScaler) scaleToOne(sample *AutoscalerSample) *AutoscaleResult {
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
func (as *AutoScaler) scaleByQueueDepth(sample *AutoscalerSample) *AutoscaleResult {
	desiredContainers := 0

	if sample.QueueLength == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(sample.QueueLength / int64(as.requestBucket.AutoscalerConfig.QueueDepth.MaxTasksPerReplica))
		if sample.QueueLength%int64(as.requestBucket.AutoscalerConfig.QueueDepth.MaxTasksPerReplica) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(as.requestBucket.AutoscalerConfig.QueueDepth.MaxReplicas), float64(MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &AutoscaleResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

// Scale by max request latency (for tasks)
func (as *AutoScaler) scaleByMaxRequestLatency(sample *AutoscalerSample) *AutoscaleResult {
	averageQueueDepth := as.samples.QueueLength.Reduce(exponentialAvg)
	averageTaskDuration := as.samples.TaskDuration.Reduce(averageTaskDuration)

	if averageTaskDuration == -1 {
		averageTaskDuration, _ = as.requestBucket.queueClient.GetAverageTaskDuration(as.requestBucket.IdentityId, as.requestBucket.Name)
	} else {
		// Store average task duration for later use
		as.requestBucket.queueClient.SetAverageTaskDuration(as.requestBucket.IdentityId, as.requestBucket.Name, averageTaskDuration)
	}

	taskConsumptionRate := -float64(sample.CurrentContainers) / averageTaskDuration

	if sample.CurrentContainers == 0 {
		taskConsumptionRate = 0
	}

	// Scale to zero if there are no tasks left to process
	if sample.QueueLength == 0 && sample.RunningTasks == 0 {
		return &AutoscaleResult{
			DesiredContainers: 0,
			ResultValid:       true,
		}
	}

	// TOOD: Remove "Autoscaling" in the future
	desiredLatency := as.requestBucket.AutoscalingConfig.DesiredLatency
	if as.requestBucket.AutoscalerConfig.RequestLatency != nil {
		desiredLatency = as.requestBucket.AutoscalerConfig.RequestLatency.DesiredLatency
	}

	if averageQueueDepth < 0 {
		return &AutoscaleResult{
			ResultValid: false,
		}
	}

	desiredTaskConsumptionRate := -averageQueueDepth / desiredLatency

	// How far off the mark we are -- meaning what's the difference between
	// how faster we're processing tasks / unit time vs. what we'd like to be processing
	offset := 1 / (taskConsumptionRate / desiredTaskConsumptionRate)

	desiredContainers := math.Ceil(float64(sample.CurrentContainers) * offset)
	desiredContainers = math.Min(float64(MaxReplicas), desiredContainers)

	// Don't run more containers than there are tasks
	if int64(desiredContainers) > sample.QueueLength {
		desiredContainers = float64(sample.QueueLength)
	}

	// If we have a NaN here, it's an invalid scaling result, assume the calculation is wrong
	if math.IsNaN(desiredContainers) {
		desiredContainers = 0
	}

	// Don't allow scaling to zero if anything is running or should be running
	if desiredContainers <= 0 && (sample.QueueLength > 0) {
		return &AutoscaleResult{
			DesiredContainers: 1,
			ResultValid:       true,
		}
	}

	return &AutoscaleResult{
		DesiredContainers: int(desiredContainers),
		ResultValid:       true,
	}
}

// Computes the exponential moving average of a window
func exponentialAvg(w rolling.Window) float64 {
	/*
		effectiveValue is the approximate average over the past:

		1 / (1-smoothingFactor) samples

		Since our window is 60 samples taken over thirty seconds
		For now choose a smoothing factor that averages over the previous ~15 samples:
		1 / (1-0.934) = 15.15
	*/
	smoothingFactor := 0.934
	previousValue := w[0][0]
	effectiveValue := 0.0

	for _, bucket := range w {
		for _, value := range bucket {
			if value < 0 {
				continue
			}

			effectiveValue = smoothingFactor*previousValue + (1-smoothingFactor)*value
		}

		previousValue = effectiveValue
	}

	return effectiveValue
}

// Get the average time it takes to complete a task
func averageTaskDuration(w rolling.Window) float64 {
	sum := 0.0
	samplesToAverage := 0

	for _, bucket := range w {
		for _, value := range bucket {

			if value < 0 {
				continue
			}

			samplesToAverage += 1
			sum += value
		}
	}

	if samplesToAverage == 0 {
		return -1
	}

	average := sum / float64(samplesToAverage)

	return average
}
