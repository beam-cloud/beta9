package endpoint

import (
	"context"
	"math"
	"time"

	rolling "github.com/asecurityteam/rolling"
)

type autoscaler struct {
	instance         *endpointInstance
	autoscalingMode  int
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
func newAutoscaler(i *endpointInstance) *autoscaler {
	var autoscalingMode = -1

	return &autoscaler{
		instance:         i,
		autoscalingMode:  autoscalingMode,
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

func (as *autoscaler) scaleByTotalRequests(sample *autoscalerSample) *autoscaleResult {
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

	return &autoscaleResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

// Start the autoscaler
func (as *autoscaler) start(ctx context.Context) {
	ticker := time.NewTicker(sampleRate)
	defer ticker.Stop()

	// TODO: For the time being, we want to aggresively try to scale down

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if as.instance.buffer.Length() > 0 {
				as.instance.scaleEventChan <- 1
			} else {
				as.instance.scaleEventChan <- 0
			}

			// sample, err := as.sample()
			// if err != nil {
			// 	continue
			// }

			// // Append samples to moving windows
			// as.samples.TotalRequests.Append(float64(sample.TotalRequests))
			// as.samples.CurrentContainers.Append(float64(sample.CurrentContainers))

			// var scaleResult *autoscaleResult = nil
			// scaleResult = as.scaleByTotalRequests(sample)

			// if scaleResult != nil && scaleResult.ResultValid {
			// 	as.instance.scaleEventChan <- scaleResult.DesiredContainers // Send autoscaling result to request bucket
			// }
		}
	}
}
