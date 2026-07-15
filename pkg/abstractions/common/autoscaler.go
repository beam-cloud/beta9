package abstractions

import (
	"context"
	"time"
)

type AutoscalerResult struct {
	DesiredContainers int
	ResultValid       bool
}

type Autoscaler[I IAutoscaledInstance, S AutoscalerSample] struct {
	instance         I
	mostRecentSample S
	sampleFunc       func(I) (S, error)
	scaleFunc        func(I, S) *AutoscalerResult
	trigger          chan struct{}
}

type IAutoscaler interface {
	Start(ctx context.Context)
	Trigger()
}

const (
	windowSize int           = 60                                     // Number of samples in the sampling window
	sampleRate time.Duration = time.Duration(1000) * time.Millisecond // Time between samples
)

type AutoscalerSample interface{}

func NewAutoscaler[I IAutoscaledInstance, S AutoscalerSample](instance I, sampleFunc func(I) (S, error), scaleFunc func(I, S) *AutoscalerResult) *Autoscaler[I, S] {
	return &Autoscaler[I, S]{
		instance:   instance,
		sampleFunc: sampleFunc,
		scaleFunc:  scaleFunc,
		trigger:    make(chan struct{}, 1),
	}
}

func (as *Autoscaler[I, S]) Trigger() {
	select {
	case as.trigger <- struct{}{}:
	default:
	}
}

// Start the autoscaler
func (as *Autoscaler[I, S]) Start(ctx context.Context) {
	ticker := time.NewTicker(sampleRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-as.trigger:
		}

		sample, err := as.sampleFunc(as.instance)
		if err != nil {
			continue
		}

		as.mostRecentSample = sample

		scaleResult := as.scaleFunc(as.instance, sample)
		if scaleResult != nil && scaleResult.ResultValid {
			as.instance.ConsumeScaleResult(scaleResult)
		}
	}
}
