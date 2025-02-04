package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

// A "local" k8s worker pool controller means
// the pool is local to the control plane / in-cluster
type LocalKubernetesWorkerPoolController struct {
	ctx            context.Context
	name           string
	config         types.AppConfig
	kubeClient     *kubernetes.Clientset
	workerPool     types.WorkerPoolConfig
	workerRepo     repository.WorkerRepository
	workerPoolRepo repository.WorkerPoolRepository
	backendRepo    repository.BackendRepository
	workspace      *types.Workspace
}

func NewLocalKubernetesWorkerPoolController(ctx context.Context, config types.AppConfig, workerPoolName string, workerRepo repository.WorkerRepository, providerRepo repository.ProviderRepository, backendRepo repository.BackendRepository, workerPoolRepo repository.WorkerPoolRepository) (WorkerPoolController, error) {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	workerPool := config.Worker.Pools[workerPoolName]
	wpc := &LocalKubernetesWorkerPoolController{
		ctx:            ctx,
		name:           workerPoolName,
		config:         config,
		kubeClient:     kubeClient,
		workerPool:     workerPool,
		workerRepo:     workerRepo,
		backendRepo:    backendRepo,
		workerPoolRepo: workerPoolRepo,
	}

	// Start monitoring worker pool size
	err = MonitorPoolSize(wpc, &workerPool, workerRepo, providerRepo)
	if err != nil {
		log.Error().Str("pool_name", wpc.name).Err(err).Msg("unable to monitor pool size")
	}

	// Start monitoring worker pool health
	err = MonitorPoolHealth(wpc, &workerPool, &wpc.config.Worker, workerRepo, providerRepo, workerPoolRepo)
	if err != nil {
		log.Error().Str("pool_name", wpc.name).Err(err).Msg("unable to monitor pool health")
	}

	go wpc.deleteStalePendingWorkerJobs()

	return wpc, nil
}

func (wpc *LocalKubernetesWorkerPoolController) Context() context.Context {
	return wpc.ctx
}

func (wpc *LocalKubernetesWorkerPoolController) IsPreemptable() bool {
	return wpc.workerPool.Preemptable
}

func (wpc *LocalKubernetesWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *LocalKubernetesWorkerPoolController) RequiresPoolSelector() bool {
	return wpc.workerPool.RequiresPoolSelector
}

func (wpc *LocalKubernetesWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	return freePoolCapacity(wpc.workerRepo, wpc)
}

func (wpc *LocalKubernetesWorkerPoolController) State() WorkerPoolState {
	return WorkerPoolState{}
}

func (wpc *LocalKubernetesWorkerPoolController) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	workerId := GenerateWorkerId()
	return wpc.addWorkerWithId(workerId, cpu, memory, wpc.workerPool.GPUType, gpuCount)
}

func (wpc *LocalKubernetesWorkerPoolController) AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineId string) (*types.Worker, error) {
	return nil, errors.New("unimplemented")
}

func (wpc *LocalKubernetesWorkerPoolController) addWorkerWithId(workerId string, cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error) {
	if wpc.workspace == nil {
		adminWorkspace, err := wpc.backendRepo.GetAdminWorkspace(wpc.ctx)
		if err != nil {
			return nil, errors.New("workspace not configured for pool: " + wpc.name)
		}

		wpc.workspace = adminWorkspace
	}

	token, err := wpc.backendRepo.CreateToken(wpc.ctx, wpc.workspace.Id, types.TokenTypeWorker, true)
	if err != nil {
		return nil, err
	}

	// Create a new worker job
	job, worker := wpc.createWorkerJob(workerId, cpu, memory, gpuType, gpuCount, token.Key)

	// Create the job in the cluster
	if err := wpc.createJobInCluster(job); err != nil {
		return nil, err
	}

	worker.PoolName = wpc.name
	worker.RequiresPoolSelector = wpc.workerPool.RequiresPoolSelector

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		log.Error().Err(err).Msg("unable to create worker")
		return nil, err
	}

	return worker, nil
}

func (wpc *LocalKubernetesWorkerPoolController) createWorkerJob(workerId string, cpu int64, memory int64, gpuType string, gpuCount uint32, token string) (*batchv1.Job, *types.Worker) {
	jobName := fmt.Sprintf("%s-%s-%s", Beta9WorkerJobPrefix, wpc.name, workerId)
	labels := map[string]string{
		"app":                       Beta9WorkerLabelValue,
		Beta9WorkerLabelKey:         Beta9WorkerLabelValue,
		Beta9WorkerLabelIDKey:       workerId,
		Beta9WorkerLabelPoolNameKey: wpc.name,
		PrometheusPortKey:           fmt.Sprintf("%d", wpc.config.Monitoring.Prometheus.Port),
		PrometheusScrapeKey:         strconv.FormatBool(wpc.config.Monitoring.Prometheus.ScrapeWorkers),
	}

	workerCpu := cpu
	workerMemory := memory
	workerGpuType := gpuType
	workerGpuCount := gpuCount

	resourceRequests := corev1.ResourceList{}

	if cpu > 0 && cpu > wpc.config.Worker.DefaultWorkerCPURequest {
		cpuString := fmt.Sprintf("%dm", cpu) // convert cpu to millicores string
		resourceRequests[corev1.ResourceCPU] = resource.MustParse(cpuString)
	} else {
		resourceRequests[corev1.ResourceCPU] = resource.MustParse(fmt.Sprintf("%dm", wpc.config.Worker.DefaultWorkerCPURequest))
		workerCpu = wpc.config.Worker.DefaultWorkerCPURequest
	}

	if memory > 0 && memory > wpc.config.Worker.DefaultWorkerMemoryRequest {
		memoryString := fmt.Sprintf("%dMi", memory) // convert memory to Mi string
		resourceRequests[corev1.ResourceMemory] = resource.MustParse(memoryString)
	} else {
		resourceRequests[corev1.ResourceMemory] = resource.MustParse(fmt.Sprintf("%dMi", wpc.config.Worker.DefaultWorkerMemoryRequest))
		workerMemory = wpc.config.Worker.DefaultWorkerMemoryRequest
	}

	// We only support nvidia for now
	if gpuType != "" {
		resourceRequests[corev1.ResourceName("nvidia.com/gpu")] = *resource.NewQuantity(int64(gpuCount), resource.DecimalSI)
	}

	workerImage := fmt.Sprintf("%s/%s:%s",
		wpc.config.Worker.ImageRegistry,
		wpc.config.Worker.ImageName,
		wpc.config.Worker.ImageTag,
	)

	resources := corev1.ResourceRequirements{}
	if wpc.config.Worker.JobResourcesEnforced {
		resources.Requests = resourceRequests
		resources.Limits = resourceRequests
	}

	containers := []corev1.Container{
		{
			Name:  defaultContainerName,
			Image: workerImage,
			Command: []string{
				defaultWorkerEntrypoint,
			},
			Resources: resources,
			SecurityContext: &corev1.SecurityContext{
				Privileged: ptr.To(true),
			},
			Ports: []corev1.ContainerPort{
				{
					Name:          "metrics",
					ContainerPort: int32(wpc.config.Monitoring.Prometheus.Port),
				},
			},
			Env:          wpc.getWorkerEnvironment(workerId, workerCpu, workerMemory, workerGpuType, workerGpuCount, token),
			VolumeMounts: wpc.getWorkerVolumeMounts(),
		},
	}

	// Add user-defined image pull secrets
	imagePullSecrets := []corev1.LocalObjectReference{}
	for _, s := range wpc.config.Worker.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets, corev1.LocalObjectReference{Name: s})
	}

	podTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:           wpc.config.Worker.ServiceAccountName,
			AutomountServiceAccountToken: ptr.To(true),
			HostNetwork:                  wpc.config.Worker.HostNetwork,
			ImagePullSecrets:             imagePullSecrets,
			RestartPolicy:                corev1.RestartPolicyOnFailure,
			NodeSelector:                 wpc.workerPool.JobSpec.NodeSelector,
			Containers:                   containers,
			Volumes:                      wpc.getWorkerVolumes(workerMemory),
			EnableServiceLinks:           ptr.To(false),
		},
	}

	if wpc.config.Worker.HostNetwork {
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}

	if wpc.workerPool.Runtime != "" {
		podTemplate.Spec.RuntimeClassName = ptr.To(wpc.workerPool.Runtime)
	}

	ttl := int32(30)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: wpc.config.Worker.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Template:                podTemplate,
			TTLSecondsAfterFinished: &ttl,
		},
	}

	return job, &types.Worker{
		Id:            workerId,
		FreeCpu:       workerCpu,
		FreeMemory:    workerMemory,
		FreeGpuCount:  workerGpuCount,
		TotalCpu:      workerCpu,
		TotalMemory:   workerMemory,
		TotalGpuCount: workerGpuCount,
		Gpu:           workerGpuType,
		Status:        types.WorkerStatusPending,
		Priority:      wpc.workerPool.Priority,
		BuildVersion:  wpc.config.Worker.ImageTag,
		Preemptable:   wpc.workerPool.Preemptable,
	}
}

func (wpc *LocalKubernetesWorkerPoolController) createJobInCluster(job *batchv1.Job) error {
	_, err := wpc.kubeClient.BatchV1().Jobs(wpc.config.Worker.Namespace).Create(context.Background(), job, metav1.CreateOptions{})
	return err
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerVolumes(workerMemory int64) []corev1.Volume {
	hostPathType := corev1.HostPathDirectoryOrCreate
	sharedMemoryLimit := calculateMemoryQuantity(wpc.workerPool.PoolSizing.SharedMemoryLimitPct, workerMemory)
	tmpSizeLimit := parseTmpSizeLimit(wpc.workerPool.TmpSizeLimit, wpc.config.Worker.TmpSizeLimit)

	volumes := []corev1.Volume{
		{
			Name: logVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: defaultWorkerLogPath,
					Type: &hostPathType,
				},
			},
		},
		{
			Name: "dshm",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium:    corev1.StorageMediumMemory,
					SizeLimit: &sharedMemoryLimit,
				},
			},
		},
		{
			Name: tmpVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					SizeLimit: &tmpSizeLimit,
				},
			},
		},
	}

	if len(wpc.workerPool.JobSpec.Volumes) > 0 {
		for _, volume := range wpc.workerPool.JobSpec.Volumes {
			vol := corev1.Volume{Name: volume.Name}
			if volume.Secret.SecretName != "" {
				vol.Secret = &corev1.SecretVolumeSource{SecretName: volume.Secret.SecretName}
			}
			volumes = append(volumes, vol)
		}
	}

	volumeSource := corev1.VolumeSource{}
	if wpc.config.Worker.ImagePVCName != "" {
		volumeSource.PersistentVolumeClaim = &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: wpc.config.Worker.ImagePVCName,
		}
	} else {
		volumeSource.HostPath = &corev1.HostPathVolumeSource{
			Path: defaultImagesPath,
			Type: &hostPathType,
		}
	}

	return append(volumes,
		corev1.Volume{
			Name:         imagesVolumeName,
			VolumeSource: volumeSource,
		},
	)
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerVolumeMounts() []corev1.VolumeMount {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      tmpVolumeName,
			MountPath: "/tmp",
			ReadOnly:  false,
		},
		{
			Name:      imagesVolumeName,
			MountPath: defaultImagesPath,
			ReadOnly:  false,
		},
		{
			Name:      logVolumeName,
			MountPath: defaultWorkerLogPath,
			ReadOnly:  false,
		},
		{
			MountPath: "/dev/shm",
			Name:      "dshm",
		},
	}

	if len(wpc.workerPool.JobSpec.VolumeMounts) > 0 {
		volumeMounts = append(volumeMounts, wpc.workerPool.JobSpec.VolumeMounts...)
	}

	return volumeMounts
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerEnvironment(workerId string, cpu int64, memory int64, gpuType string, gpuCount uint32, token string) []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		{
			Name:  "WORKER_ID",
			Value: workerId,
		},
		{
			Name:  "WORKER_POOL_NAME",
			Value: wpc.name,
		},
		{
			Name:  "WORKER_TOKEN",
			Value: token,
		},
		{
			Name:  "CPU_LIMIT",
			Value: strconv.FormatInt(cpu, 10),
		},
		{
			Name:  "MEMORY_LIMIT",
			Value: strconv.FormatInt(memory, 10),
		},
		{
			Name:  "GPU_TYPE",
			Value: gpuType,
		},
		{
			Name:  "GPU_COUNT",
			Value: strconv.FormatInt(int64(gpuCount), 10),
		},
		{
			Name: "POD_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name:  "POD_NAMESPACE",
			Value: wpc.config.Worker.Namespace,
		},
		{
			Name: "NETWORK_PREFIX",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		{
			Name:  "PREEMPTABLE",
			Value: strconv.FormatBool(wpc.workerPool.Preemptable),
		},
	}

	if wpc.config.Worker.UseGatewayServiceHostname {
		envVars = append(envVars, []corev1.EnvVar{
			{
				Name:  "BETA9_GATEWAY_HOST",
				Value: wpc.config.GatewayService.Host,
			},
			{
				Name:  "BETA9_GATEWAY_PORT",
				Value: fmt.Sprint(wpc.config.GatewayService.GRPC.Port),
			},
		}...)
	} else {
		envVars = append(envVars, []corev1.EnvVar{
			{
				Name:  "BETA9_GATEWAY_HOST",
				Value: wpc.config.GatewayService.GRPC.ExternalHost,
			},
			{
				Name:  "BETA9_GATEWAY_PORT",
				Value: "443",
			},
		}...)
	}

	if len(wpc.workerPool.JobSpec.Env) > 0 {
		envVars = append(envVars, wpc.workerPool.JobSpec.Env...)
	}

	// Serialize the AppConfig struct to JSON
	configJson, err := json.MarshalIndent(wpc.config, "", "  ")
	if err == nil {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "CONFIG_JSON",
			Value: string(configJson),
		})
	}

	return envVars
}

// deleteStalePendingWorkerJobs ensures that jobs are deleted if they don't
// start a pod after a certain amount of time.
func (wpc *LocalKubernetesWorkerPoolController) deleteStalePendingWorkerJobs() {
	ctx := wpc.ctx
	maxAge := wpc.config.Worker.AddWorkerTimeout
	namespace := wpc.config.Worker.Namespace

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			return // Context has been cancelled
		default: // Continue processing requests
		}

		jobSelector := strings.Join([]string{
			fmt.Sprintf("%s=%s", Beta9WorkerLabelKey, Beta9WorkerLabelValue),
			fmt.Sprintf("%s=%s", Beta9WorkerLabelPoolNameKey, wpc.name),
		}, ",")

		jobs, err := wpc.kubeClient.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{LabelSelector: jobSelector})
		if err != nil {
			log.Error().Str("pool_name", wpc.name).Err(err).Msg("failed to list jobs for controller")
			continue
		}

		for _, job := range jobs.Items {
			podSelector := fmt.Sprintf("job-name=%s", job.Name)

			pods, err := wpc.kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: podSelector})
			if err != nil {
				log.Error().Str("job_name", job.Name).Err(err).Msg("failed to list pods for job")
				continue
			}

			for _, pod := range pods.Items {
				// Skip the pod if its scheduled/not pending
				if pod.Status.Phase != corev1.PodPending {
					continue
				}

				duration := time.Since(pod.CreationTimestamp.Time)
				if duration >= maxAge {
					// Remove worker from repository
					if workerId, ok := pod.Labels[Beta9WorkerLabelIDKey]; ok {
						if err := wpc.workerRepo.RemoveWorker(workerId); err != nil {
							log.Error().Str("worker_id", workerId).Err(err).Msg("failed to delete pending worker")
						}
					}

					// Remove worker job from kubernetes
					if err := wpc.kubeClient.BatchV1().Jobs(namespace).Delete(ctx, job.Name, metav1.DeleteOptions{
						PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
					}); err != nil {
						log.Error().Str("job_name", job.Name).Err(err).Msg("failed to delete pending worker job")
					}

					log.Info().Str("job_name", job.Name).Str("duration", maxAge.String()).Msg("deleted worker due to exceeding age limit")
				}
			}
		}
	}
}
