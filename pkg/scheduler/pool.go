package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	Beta9WorkerLabelKey         string  = "run.beam.cloud/role"
	Beta9WorkerLabelValue       string  = "worker"
	Beta9WorkerJobPrefix        string  = "worker"
	Beta9WorkerLabelIDKey       string  = "run.beam.cloud/worker-id"
	Beta9WorkerLabelPoolNameKey string  = "run.beam.cloud/worker-pool-name"
	PrometheusPortKey           string  = "prometheus.io/port"
	PrometheusScrapeKey         string  = "prometheus.io/scrape"
	tmpVolumeName               string  = "beta9-tmp"
	logVolumeName               string  = "beta9-logs"
	imagesVolumeName            string  = "beta9-images"
	defaultContainerName        string  = "worker"
	defaultWorkerEntrypoint     string  = "/usr/local/bin/worker"
	defaultWorkerLogPath        string  = "/var/log/worker"
	defaultImagesPath           string  = "/images"
	defaultSharedMemoryPct      float32 = 0.5
	poolMonitoringInterval              = 1 * time.Second
	poolHealthCheckInterval             = 1 * time.Second
)

type WorkerPoolController interface {
	AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error)
	AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineId string) (*types.Worker, error)
	Name() string
	FreeCapacity() (*WorkerPoolCapacity, error)
	Context() context.Context
	IsPreemptable() bool
	State() (*types.WorkerPoolState, error)
	RequiresPoolSelector() bool
}

type WorkerPoolConfig struct {
	DefaultWorkerCpuRequest    int64
	DefaultWorkerMemoryRequest int64
}

type WorkerPoolCapacity struct {
	FreeCpu    int64
	FreeMemory int64
	FreeGpu    uint
}

type WorkerPoolControllerOptions struct {
	Name           string
	Context        context.Context
	Config         types.AppConfig
	BackendRepo    repository.BackendRepository
	WorkerRepo     repository.WorkerRepository
	WorkerPoolRepo repository.WorkerPoolRepository
	ContainerRepo  repository.ContainerRepository
	ProviderName   *types.MachineProvider
	ProviderRepo   repository.ProviderRepository
	Tailscale      *network.Tailscale
}

func GenerateWorkerId() string {
	return uuid.New().String()[:8]
}

func MonitorPoolSize(wpc WorkerPoolController,
	workerPoolConfig *types.WorkerPoolConfig,
	workerRepo repository.WorkerRepository,
	workerPoolRepo repository.WorkerPoolRepository,
	providerRepo repository.ProviderRepository) error {
	poolSizer, err := NewWorkerPoolSizer(wpc, workerPoolConfig, workerRepo, workerPoolRepo, providerRepo)
	if err != nil {
		return err
	}

	go poolSizer.Start()
	return nil
}

func MonitorPoolHealth(wpc WorkerPoolController,
	workerPoolConfig *types.WorkerPoolConfig,
	workerConfig *types.WorkerConfig,
	workerRepo repository.WorkerRepository,
	providerRepo repository.ProviderRepository,
	workerPoolRepo repository.WorkerPoolRepository,
	containerRepo repository.ContainerRepository) error {

	poolHealthMonitor := NewPoolHealthMonitor(PoolHealthMonitorOptions{
		Controller:       wpc,
		WorkerPoolConfig: workerPoolConfig,
		WorkerConfig:     workerConfig,
		WorkerRepo:       workerRepo,
		ProviderRepo:     providerRepo,
		WorkerPoolRepo:   workerPoolRepo,
		ContainerRepo:    containerRepo,
	})

	go poolHealthMonitor.Start()

	return nil
}

// // DeleteStalePendingWorkerJobs ensures that worker jobs are deleted if they don't
// // start a pod after a certain amount of time.
// func DeleteStalePendingWorkerJobs(wpc WorkerPoolController,
// 	workerPoolConfig *types.WorkerPoolConfig,
// 	workerConfig *types.WorkerConfig,
// 	workerRepo repository.WorkerRepository,
// 	providerRepo repository.ProviderRepository) {
// 	ctx := wpc.Context()
// 	maxAge := workerConfig.AddWorkerTimeout
// 	namespace := workerConfig.Namespace

// 	ticker := time.NewTicker(time.Minute)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		select {
// 		case <-ctx.Done():
// 			return // Context has been cancelled
// 		default: // Continue processing requests
// 		}

// 		jobSelector := strings.Join([]string{
// 			fmt.Sprintf("%s=%s", Beta9WorkerLabelKey, Beta9WorkerLabelValue),
// 			fmt.Sprintf("%s=%s", Beta9WorkerLabelPoolNameKey, wpc.Name()),
// 		}, ",")

// 		jobs, err := wpc.kubeClient.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{LabelSelector: jobSelector})
// 		if err != nil {
// 			log.Error().Str("pool_name", wpc.Name()).Err(err).Msg("failed to list jobs for controller")
// 			continue
// 		}

// 		for _, job := range jobs.Items {
// 			podSelector := fmt.Sprintf("job-name=%s", job.Name)

// 			pods, err := wpc.kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: podSelector})
// 			if err != nil {
// 				log.Error().Str("job_name", job.Name).Err(err).Msg("failed to list pods for job")
// 				continue
// 			}

// 			for _, pod := range pods.Items {
// 				// Skip the pod if its scheduled/not pending
// 				if pod.Status.Phase != corev1.PodPending {
// 					continue
// 				}

// 				duration := time.Since(pod.CreationTimestamp.Time)
// 				if duration >= maxAge {
// 					// Remove worker from repository
// 					if workerId, ok := pod.Labels[Beta9WorkerLabelIDKey]; ok {
// 						if err := workerRepo.RemoveWorker(workerId); err != nil {
// 							log.Error().Str("worker_id", workerId).Err(err).Msg("failed to delete pending worker")
// 						}
// 					}

// 					// Remove worker job from kubernetes
// 					if err := wpc.kubeClient.BatchV1().Jobs(namespace).Delete(ctx, job.Name, metav1.DeleteOptions{
// 						PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
// 					}); err != nil {
// 						log.Error().Str("job_name", job.Name).Err(err).Msg("failed to delete pending worker job")
// 					}

// 					log.Info().Str("job_name", job.Name).Str("duration", maxAge.String()).Msg("deleted worker due to exceeding age limit")
// 				}
// 			}
// 		}
// 	}
// }

func freePoolCapacity(workerRepo repository.WorkerRepository, wpc WorkerPoolController) (*WorkerPoolCapacity, error) {
	workers, err := workerRepo.GetAllWorkersInPool(wpc.Name())
	if err != nil {
		return nil, err
	}

	capacity := &WorkerPoolCapacity{
		FreeCpu:    0,
		FreeMemory: 0,
		FreeGpu:    0,
	}

	for _, worker := range workers {
		// Exclude disabled workers from the capacity calculation
		if worker.Status == types.WorkerStatusDisabled {
			continue
		}

		capacity.FreeCpu += worker.FreeCpu
		capacity.FreeMemory += worker.FreeMemory

		if worker.Gpu != "" && (worker.FreeCpu > 0 && worker.FreeMemory > 0) {
			capacity.FreeGpu += uint(worker.FreeGpuCount)
		}
	}

	return capacity, nil
}

func calculateMemoryQuantity(percentStr string, memoryTotal int64) resource.Quantity {
	percent, err := parseMemoryPercentage(percentStr)
	if err != nil {
		percent = defaultSharedMemoryPct
	}

	return resource.MustParse(fmt.Sprintf("%dMi", int64(float32(memoryTotal)*percent)))
}

func parseMemoryPercentage(percentStr string) (float32, error) {
	ps := strings.TrimSuffix(percentStr, "%")

	percent, err := strconv.ParseFloat(ps, 32)
	if err != nil {
		return 0, err
	}

	if percent <= 0 {
		return 0, errors.New("percent must be greater than 0")
	}

	return float32(percent) / 100, nil
}
