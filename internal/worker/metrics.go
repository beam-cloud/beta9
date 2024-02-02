package worker

import (
	"context"
	"log"
	"time"

	"github.com/beam-cloud/beta9/internal/repository"
	repo "github.com/beam-cloud/beta9/internal/repository"
	types "github.com/beam-cloud/beta9/internal/types"
	"github.com/prometheus/client_golang/prometheus"
)

type WorkerMetrics struct {
	workerId    string
	metricsRepo repo.PrometheusRepository
	workerRepo  repo.WorkerRepository
	ctx         context.Context
}

func NewWorkerMetrics(
	ctx context.Context,
	workerId string,
	workerRepo repo.WorkerRepository,
	config types.PrometheusConfig,
) *WorkerMetrics {
	metricsRepo := repository.NewMetricsPrometheusRepository(config)
	metricsRepo.RegisterCounterVec(
		prometheus.CounterOpts{
			Name: types.MetricsWorkerContainerDurationSeconds,
		},
		[]string{"container_id", "worker_id"},
	)

	go func() {
		if err := metricsRepo.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	workerMetrics := &WorkerMetrics{
		ctx:         ctx,
		workerId:    workerId,
		metricsRepo: metricsRepo,
		workerRepo:  workerRepo,
	}

	return workerMetrics
}

func (wm *WorkerMetrics) metricsContainerDuration(containerId string, workerId string, duration time.Duration) {
	if handler := wm.metricsRepo.GetCounterVecHandler(types.MetricsWorkerContainerDurationSeconds); handler != nil {
		handler.WithLabelValues(containerId, workerId).Add(duration.Seconds())
	}
}

// Periodically send metrics to track container duration
func (wm *WorkerMetrics) EmitContainerUsage(request *types.ContainerRequest, done chan bool) {
	cursorTime := time.Now()
	ticker := time.NewTicker(types.ContainerDurationEmissionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go wm.metricsContainerDuration(request.ContainerId, wm.workerId, time.Since(cursorTime))
			cursorTime = time.Now()
		case <-done:
			// Consolidate any remaining time
			go wm.metricsContainerDuration(request.ContainerId, wm.workerId, time.Since(cursorTime))
			return
		}
	}
}
