package abstractions

import (
	"context"
	"time"

	rolling "github.com/asecurityteam/rolling"
)

type AutoscalerResult struct {
	DesiredContainers int
	ResultValid       bool
}

type AutoScaler[I AbstractionInstance, S AutoscalerSample] struct {
	instance         I
	mostRecentSample S
	rollingWindows   map[string]*rolling.PointPolicy
	sampleFunc       func(I) (S, error)
	scaleFunc        func(I, S) *AutoscalerResult
}

const (
	maxReplicas uint          = 5                                      // Maximum number of desired replicas that can be returned
	windowSize  int           = 60                                     // Number of samples in the sampling window
	sampleRate  time.Duration = time.Duration(1000) * time.Millisecond // Time between samples
)

type AbstractionInstance interface {
	ConsumeScaleResult(*AutoscalerResult)
}
type AutoscalerSample interface{}

func NewAutoscaler[I AbstractionInstance, S AutoscalerSample](instance I, sampleFunc func(I) (S, error), scaleFunc func(I, S) *AutoscalerResult) *AutoScaler[I, S] {
	windows := map[string]*rolling.PointPolicy{
		"QueueLength":       rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
		"RunningTasks":      rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
		"CurrentContainers": rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
		"TaskDuration":      rolling.NewPointPolicy(rolling.NewWindow(windowSize)),
	}
	return &AutoScaler[I, S]{
		instance:       instance,
		rollingWindows: windows,
		sampleFunc:     sampleFunc,
		scaleFunc:      scaleFunc,
	}
}

// Start the autoscaler
func (as *AutoScaler[I, S]) Start(ctx context.Context) {
	ticker := time.NewTicker(sampleRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample, err := as.sampleFunc(as.instance)
			if err != nil {
				continue
			}

			scaleResult := as.scaleFunc(as.instance, sample)
			if scaleResult != nil && scaleResult.ResultValid {
				as.instance.ConsumeScaleResult(scaleResult) // Send autoscaling result back to instance
			}
		}
	}
}
