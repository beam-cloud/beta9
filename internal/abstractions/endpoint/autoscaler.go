package endpoint

import (
	"context"
	"math"
	"time"

	rolling "github.com/asecurityteam/rolling"
	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
)

type autoscaler struct {
	instance         *endpointInstance
	samples          *autoscalingWindows
	mostRecentSample *autoscalerSample
}

type autoscalingWindows struct {
	TotalRequests     *rolling.PointPolicy
	CurrentContainers *rolling.PointPolicy
}

type autoscalerSample struct {
	TotalRequests     int64
	CurrentContainers int64
}

const (
	maxReplicas uint          = 5                                      // Maximum number of desired replicas that can be returned
	windowSize  int           = 60                                     // Number of samples in the sampling window
	sampleRate  time.Duration = time.Duration(1000) * time.Millisecond // Time between samples
)

// Create a new autoscaler
func newDeploymentAutoscaler(i *endpointInstance) abstractions.AutoScaler {
	return &autoscaler{
		instance:         i,
		mostRecentSample: nil,
		samples: &autoscalingWindows{
			TotalRequests:     rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
			CurrentContainers: rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
		},
	}
}

// Retrieve a datapoint from the request bucket
func (as *autoscaler) sample() (*autoscalerSample, error) {
	instance := as.instance

	totalRequests := instance.buffer.Length()

	currentContainers := 0
	state, err := instance.state()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &autoscalerSample{
		TotalRequests:     int64(totalRequests),
		CurrentContainers: int64(currentContainers),
	}

	// Cache most recent autoscaler sample so RequestBucket can access without hitting redis
	as.mostRecentSample = sample

	return sample, nil
}

func (as *autoscaler) scaleByTotalRequests(sample *autoscalerSample) *abstractions.AutoscaleResult {
	desiredContainers := 0

	if sample.TotalRequests == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(sample.TotalRequests / int64(as.instance.stubConfig.Concurrency))
		if sample.TotalRequests%int64(as.instance.stubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(as.instance.stubConfig.MaxContainers), float64(maxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscaleResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

// Start the autoscaler
func (as *autoscaler) Start(ctx context.Context) {
	ticker := time.NewTicker(sampleRate)
	defer ticker.Stop()

	for {
		select {

		case <-ctx.Done():
			return
		case <-ticker.C:
			if as.instance.buffer.Length() > 0 {
				as.instance.scaleEventChan <- 1
			} else {
				as.instance.scaleEventChan <- 1
			}
		}
	}
}
