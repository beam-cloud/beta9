package webserver

import (
	"context"
	"math"
	"time"

	rolling "github.com/asecurityteam/rolling"
)

const (
	autoScalingModeQueueDepth int = -1
)

type autoscaler struct {
	instance         *webserverInstance
	autoscalingMode  int
	samples          *autoscalingWindows
	mostRecentSample *autoscalerSample
}

type autoscalingWindows struct {
	QueueLength       *rolling.PointPolicy
	RunningTasks      *rolling.PointPolicy
	CurrentContainers *rolling.PointPolicy
}

type autoscalerSample struct {
	QueueLength       int64
	RunningTasks      int64
	CurrentContainers int
	TaskDuration      float64
}

type autoscaleResult struct {
	DesiredContainers int
	ResultValid       bool
}

const (
	maxReplicas uint          = 5                                      // Maximum number of desired replicas that can be returned
	windowSize  int           = 60                                     // Number of samples in the sampling window
	sampleRate  time.Duration = time.Duration(1000) * time.Millisecond // Time between samples
)

// Create a new autoscaler
func newAutoscaler(i *webserverInstance) *autoscaler {
	var autoscalingMode = autoScalingModeQueueDepth

	return &autoscaler{
		instance:        i,
		autoscalingMode: autoscalingMode,
		samples: &autoscalingWindows{
			QueueLength:       rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
			CurrentContainers: rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
		},
		mostRecentSample: nil,
	}
}

// Retrieve a datapoint from the request bucket
func (as *autoscaler) sample() (*autoscalerSample, error) {
	instance := as.instance

	queueLength := instance.buffer.Len()

	currentContainers := 0
	state, err := instance.state()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &autoscalerSample{
		QueueLength:       int64(queueLength),
		CurrentContainers: currentContainers,
	}
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
	for i := 0; i < windowSize; i += 1 {
		as.samples.QueueLength.Append(-1)
		as.samples.CurrentContainers.Append(-1)
	}

	ticker := time.NewTicker(sampleRate)
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

			var scaleResult *autoscaleResult = nil
			switch as.autoscalingMode {
			case autoScalingModeQueueDepth:
				scaleResult = as.scaleByQueueDepth(sample)
			default:
			}

			if scaleResult != nil && scaleResult.ResultValid {
				as.instance.scaleEventChan <- scaleResult.DesiredContainers // Send autoscaling result to request bucket
			}
		}
	}
}

// Scale based on the number of items in the queue
func (as *autoscaler) scaleByQueueDepth(sample *autoscalerSample) *autoscaleResult {
	desiredContainers := 0

	if sample.QueueLength == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(sample.QueueLength / int64(as.instance.stubConfig.Concurrency))
		if sample.QueueLength%int64(as.instance.stubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(as.instance.stubConfig.MaxContainers), float64(maxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &autoscaleResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
