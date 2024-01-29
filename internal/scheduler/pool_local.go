package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

// A "local" k8s worker pool controller means
// the pool is local to the control plane / in-cluser
type LocalKubernetesWorkerPoolController struct {
	name       string
	config     types.AppConfig
	kubeClient *kubernetes.Clientset
	workerPool types.WorkerPoolConfig
	workerRepo repository.WorkerRepository
}

func NewLocalKubernetesWorkerPoolController(config types.AppConfig, workerPoolName string, workerRepo repository.WorkerRepository) (WorkerPoolController, error) {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	workerPool, ok := config.Worker.Pools[workerPoolName]
	if !ok {
		return nil, fmt.Errorf("worker pool %s not found", workerPoolName)
	}

	wpc := &LocalKubernetesWorkerPoolController{
		name:       workerPoolName,
		config:     config,
		kubeClient: kubeClient,
		workerPool: workerPool,
		workerRepo: workerRepo,
	}

	// Start monitoring worker pool size
	err = wpc.monitorPoolSize(&workerPool)
	if err != nil {
		log.Printf("<pool %s> unable to monitor pool size: %+v\n", wpc.name, err)
	}

	go wpc.deleteStalePendingWorkerJobs()

	return wpc, nil
}

func (wpc *LocalKubernetesWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *LocalKubernetesWorkerPoolController) poolId() string {
	hasher := sha256.New()
	hasher.Write([]byte(wpc.name))
	hash := hasher.Sum(nil)
	poolId := hex.EncodeToString(hash[:8])

	return poolId
}

func (wpc *LocalKubernetesWorkerPoolController) monitorPoolSize(workerPool *types.WorkerPoolConfig) error {
	config, err := ParsePoolSizingConfig(workerPool.PoolSizing)
	if err != nil {
		return err
	}

	poolSizer, err := NewWorkerPoolSizer(wpc, config)
	if err != nil {
		return err
	}

	go poolSizer.Start()
	return nil
}

func (wpc *LocalKubernetesWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	workers, err := wpc.workerRepo.GetAllWorkersInPool(wpc.poolId())
	if err != nil {
		return nil, err
	}

	capacity := &WorkerPoolCapacity{
		FreeCpu:    0,
		FreeMemory: 0,
		FreeGpu:    0,
	}

	for _, worker := range workers {
		capacity.FreeCpu += worker.Cpu
		capacity.FreeMemory += worker.Memory

		if worker.Gpu != "" && (worker.Cpu > 0 && worker.Memory > 0) {
			capacity.FreeGpu += 1
		}
	}

	return capacity, nil
}

func (wpc *LocalKubernetesWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	workerId := wpc.generateWorkerId()
	return wpc.addWorkerWithId(workerId, cpu, memory, gpuType)
}

func (wpc *LocalKubernetesWorkerPoolController) AddWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	return wpc.addWorkerWithId(workerId, cpu, memory, gpuType)
}

func (wpc *LocalKubernetesWorkerPoolController) addWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	// Create a new worker job
	job, worker := wpc.createWorkerJob(workerId, cpu, memory, gpuType)

	// Create the job in the cluster
	if err := wpc.createJobInCluster(job); err != nil {
		return nil, err
	}

	worker.PoolId = wpc.poolId()

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		log.Printf("Unable to create worker: %+v\n", err)
		return nil, err
	}

	return worker, nil
}

func (wpc *LocalKubernetesWorkerPoolController) createWorkerJob(workerId string, cpu int64, memory int64, gpuType string) (*batchv1.Job, *types.Worker) {
	jobName := fmt.Sprintf("%s-%s-%s", Beta9WorkerJobPrefix, wpc.name, workerId)
	labels := map[string]string{
		"app":               Beta9WorkerLabelValue,
		Beta9WorkerLabelKey: Beta9WorkerLabelValue,
		PrometheusScrapeKey: strconv.FormatBool(wpc.config.Metrics.Prometheus.ScrapeWorkers),
	}

	workerCpu := cpu
	workerMemory := memory
	workerGpu := gpuType

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

	if gpuType != "" && wpc.workerPool.Runtime == "nvidia" {
		resourceRequests[corev1.ResourceName("nvidia.com/gpu")] = *resource.NewQuantity(1, resource.DecimalSI)
	}

	workerImage := fmt.Sprintf("%s/%s:%s",
		wpc.config.Worker.ImageRegistry,
		wpc.config.Worker.ImageName,
		wpc.config.Worker.ImageTag,
	)

	resources := corev1.ResourceRequirements{}
	if wpc.config.Worker.ResourcesEnforced {
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
					ContainerPort: int32(wpc.config.Metrics.Prometheus.Port),
				},
			},
			Env:          wpc.getWorkerEnvironment(workerId, workerCpu, workerMemory, workerGpu),
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
		Id:     workerId,
		Cpu:    workerCpu,
		Memory: workerMemory,
		Gpu:    workerGpu,
		Status: types.WorkerStatusPending,
	}
}

func (wpc *LocalKubernetesWorkerPoolController) createJobInCluster(job *batchv1.Job) error {
	_, err := wpc.kubeClient.BatchV1().Jobs(wpc.config.Worker.Namespace).Create(context.Background(), job, metav1.CreateOptions{})
	return err
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerVolumes(workerMemory int64) []corev1.Volume {
	hostPathType := corev1.HostPathDirectoryOrCreate
	sharedMemoryLimit := resource.MustParse(fmt.Sprintf("%dMi", workerMemory/2))

	tmpSizeLimit := resource.MustParse("30Gi")
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

	return append(volumes,
		corev1.Volume{
			Name: imagesVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "images",
				},
			},
		},
	)
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      tmpVolumeName,
			MountPath: "/tmp",
			ReadOnly:  false,
		},
		{
			Name:      imagesVolumeName,
			MountPath: "/images",
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
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerEnvironment(workerId string, cpu int64, memory int64, gpuType string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "WORKER_ID",
			Value: workerId,
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
			Name:  "BETA9_GATEWAY_HOST",
			Value: wpc.config.GatewayService.Host,
		},
		{
			Name:  "BETA9_GATEWAY_PORT",
			Value: fmt.Sprint(wpc.config.GatewayService.GRPCPort),
		},
	}
}

var AddWorkerTimeout = 10 * time.Minute

// deleteStalePendingWorkerJobs ensures that jobs are deleted if they don't
// start a pod after a certain amount of time.
func (wpc *LocalKubernetesWorkerPoolController) deleteStalePendingWorkerJobs() {
	ctx := context.Background()
	maxAge := AddWorkerTimeout
	namespace := wpc.config.Worker.Namespace

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		jobSelector := fmt.Sprintf("%s=%s", Beta9WorkerLabelKey, Beta9WorkerLabelValue)
		jobs, err := wpc.kubeClient.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{LabelSelector: jobSelector})
		if err != nil {
			log.Printf("Failed to list jobs for controller <%s>: %v\n", wpc.name, err)
			continue
		}

		for _, job := range jobs.Items {
			// Skip job if it doesn't belong to the controller
			if !strings.Contains(job.Name, wpc.name) {
				continue
			}

			podSelector := fmt.Sprintf("job-name=%s", job.Name)
			pods, err := wpc.kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: podSelector})
			if err != nil {
				log.Printf("Failed to list pods for job <%v>: %v\n", job.Name, err)
				continue
			}

			for _, pod := range pods.Items {
				// Skip the pod if its scheduled/not pending
				if pod.Status.Phase != corev1.PodPending {
					continue
				}

				duration := time.Since(pod.CreationTimestamp.Time)
				if duration >= maxAge {
					p := metav1.DeletePropagationBackground
					err := wpc.kubeClient.BatchV1().Jobs(namespace).Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: &p})
					if err != nil {
						log.Printf("Failed to delete pending job <%s>: %v\n", job.Name, err)
					} else {
						log.Printf("Deleted job <%s> due to exceeding age limit of <%v>", job.Name, maxAge)
					}
				}
			}
		}
	}
}

func (wpc *LocalKubernetesWorkerPoolController) generateWorkerId() string {
	return uuid.New().String()[:8]
}
