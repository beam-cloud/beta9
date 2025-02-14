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
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
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
	poolHealthCheckInterval             = 10 * time.Second
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
	Mode() types.PoolMode
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
	EventRepo      repository.EventRepository
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

func MonitorPoolHealth(opts PoolHealthMonitorOptions) error {
	poolHealthMonitor := NewPoolHealthMonitor(PoolHealthMonitorOptions{
		Controller:       opts.Controller,
		WorkerPoolConfig: opts.WorkerPoolConfig,
		WorkerConfig:     opts.WorkerConfig,
		WorkerRepo:       opts.WorkerRepo,
		ProviderRepo:     opts.ProviderRepo,
		WorkerPoolRepo:   opts.WorkerPoolRepo,
		ContainerRepo:    opts.ContainerRepo,
		EventRepo:        opts.EventRepo,
	})

	go poolHealthMonitor.Start()

	return nil
}

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

func cleanupWorkers(ctx context.Context, poolName string, workerConfig types.WorkerConfig, kubeClient *kubernetes.Clientset, workerRepo repository.WorkerRepository) {
	jobSelector := strings.Join([]string{
		fmt.Sprintf("%s=%s", Beta9WorkerLabelKey, Beta9WorkerLabelValue),
		fmt.Sprintf("%s=%s", Beta9WorkerLabelPoolNameKey, poolName),
	}, ",")

	jobs, err := kubeClient.BatchV1().Jobs(workerConfig.Namespace).List(ctx, metav1.ListOptions{LabelSelector: jobSelector})
	if err != nil {
		log.Error().Str("pool_name", poolName).Err(err).Msg("failed to list jobs for controller")
		return
	}

	for _, job := range jobs.Items {
		podSelector := fmt.Sprintf("job-name=%s", job.Name)

		pods, err := kubeClient.CoreV1().Pods(workerConfig.Namespace).List(ctx, metav1.ListOptions{LabelSelector: podSelector})
		if err != nil {
			log.Error().Str("job_name", job.Name).Err(err).Msg("failed to list pods for job")
			continue
		}

		for _, pod := range pods.Items {
			workerId, ok := pod.Labels[Beta9WorkerLabelIDKey]
			if !ok {
				log.Warn().Str("job_name", job.Name).Str("pod_name", pod.Name).Msg("worker id not found in pod labels")
				continue
			}

			if _, err := workerRepo.GetWorkerById(workerId); err != nil {
				if _, ok := err.(*types.ErrWorkerNotFound); ok {
					if err := deleteWorker(ctx, workerId, workerRepo, kubeClient, workerConfig.Namespace, job); err != nil {
						log.Error().Str("job_name", job.Name).Err(err).Msg("failed to delete worker job")
					} else {
						log.Info().Str("job_name", job.Name).Msg("deleted worker due to non-existent worker state")
					}
					continue
				}
			}

			// Skip the pod if its scheduled/not pending
			if pod.Status.Phase != corev1.PodPending {
				continue
			}

			duration := time.Since(pod.CreationTimestamp.Time)
			if duration >= workerConfig.CleanupPendingWorkerAgeLimit {
				if err := deleteWorker(ctx, workerId, workerRepo, kubeClient, workerConfig.Namespace, job); err != nil {
					log.Error().Str("job_name", job.Name).Err(err).Msg("failed to delete worker job")
				} else {
					log.Info().Str("job_name", job.Name).Str("duration", duration.String()).Msg("deleted worker due to exceeding age limit")
				}
			}
		}
	}
}

func deleteWorker(ctx context.Context, workerId string, workerRepo repository.WorkerRepository, kubeClient *kubernetes.Clientset, namespace string, job batchv1.Job) error {
	var eg errgroup.Group

	// Remove worker state from Repository
	eg.Go(func() error {
		if err := workerRepo.RemoveWorker(workerId); err != nil {
			if _, ok := err.(*types.ErrWorkerNotFound); !ok {
				return err
			}
		}
		return nil
	})

	// Remove worker job from Kubernetes
	eg.Go(func() error {
		return kubeClient.BatchV1().Jobs(namespace).Delete(ctx, job.Name, metav1.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		})
	})

	return eg.Wait()
}
