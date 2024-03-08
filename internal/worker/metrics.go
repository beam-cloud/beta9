package worker

import (
	"context"
	"time"

	repo "github.com/beam-cloud/beta9/internal/repository"
	types "github.com/beam-cloud/beta9/internal/types"
)

type WorkerMetrics struct {
	workerId    string
	metricsRepo repo.MetricsRepository
	workerRepo  repo.WorkerRepository
	ctx         context.Context
}

func NewWorkerMetrics(
	ctx context.Context,
	workerId string,
	workerRepo repo.WorkerRepository,
	config types.PrometheusConfig,
) *WorkerMetrics {
	metricsRepo := repo.MetricsRepository(config)
	// metricsRepo.Init()
	// metricsRepo.RegisterCounterVec(
	// 	prometheus.CounterOpts{
	// 		Name: types.MetricsWorkerContainerDurationSeconds,
	// 	},
	// 	[]string{"container_id", "worker_id"},
	// )

	workerMetrics := &WorkerMetrics{
		ctx:         ctx,
		workerId:    workerId,
		metricsRepo: metricsRepo,
		workerRepo:  workerRepo,
	}

	return workerMetrics
}

func (wm *WorkerMetrics) metricsContainerDuration(containerId string, workerId string, duration time.Duration) {
	// if handler := wm.metricsRepo.GetCounterVecHandler(types.MetricsWorkerContainerDurationSeconds); handler != nil {
	// 	handler.WithLabelValues(containerId, workerId).Add(duration.Seconds())
	// }
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
