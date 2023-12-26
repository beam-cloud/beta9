package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/google/uuid"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/pointer"
)

func KubernetesWorkerPoolControllerFactory() WorkerPoolControllerFactory {
	return func(resource *types.WorkerPoolResource, config WorkerPoolControllerConfig) (WorkerPoolController, error) {
		if config, ok := config.(*KubernetesWorkerPoolControllerConfig); ok {
			return NewKubernetesWorkerPoolController(resource.Name, config)
		}

		return nil, errors.New("kubernetes worker pool controller factory received invalid config")
	}
}

type KubernetesWorkerPoolControllerConfig struct {
	IsRemote         bool
	ImagePullSecrets []string
	EnvVars          []corev1.EnvVar
	HostNetwork      bool
	Namespace        string
	WorkerPoolConfig *WorkerPoolConfig
	WorkerRepo       repository.WorkerRepository
}

func NewKubernetesWorkerPoolControllerConfig(workerRepo repository.WorkerRepository) (*KubernetesWorkerPoolControllerConfig, error) {
	namespace := common.Secrets().GetWithDefault("WORKER_NAMESPACE", "beam")

	workerPoolConfig, err := NewWorkerPoolConfig()
	if err != nil {
		return nil, err
	}

	return &KubernetesWorkerPoolControllerConfig{
		Namespace:        namespace,
		WorkerPoolConfig: workerPoolConfig,
		WorkerRepo:       workerRepo,
	}, nil
}

type KubernetesWorkerPoolController struct {
	name             string
	controllerConfig *KubernetesWorkerPoolControllerConfig
	provisioner      string
	nodeSelector     map[string]string
	kubeClient       *kubernetes.Clientset
	workerPoolClient *WorkerPoolClient
	workerPoolConfig *WorkerPoolConfig
	workerRepo       repository.WorkerRepository
}

func NewKubernetesWorkerPoolController(workerPoolName string, config *KubernetesWorkerPoolControllerConfig) (WorkerPoolController, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	workerPoolClient, err := NewWorkerPoolClient(cfg)
	if err != nil {
		return nil, err
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	wpc := &KubernetesWorkerPoolController{
		name:             workerPoolName,
		controllerConfig: config,
		workerPoolClient: workerPoolClient,
		kubeClient:       kubeClient,
	}

	workerPool, err := wpc.getWorkerPool()
	if err != nil {
		return nil, err
	}

	wpc.name = workerPoolName
	wpc.provisioner = workerPool.Spec.JobSpec.NodeSelector[defaultProvisionerLabel]
	wpc.nodeSelector = workerPool.Spec.JobSpec.NodeSelector
	wpc.workerPoolConfig = config.WorkerPoolConfig
	wpc.workerRepo = config.WorkerRepo

	// Start monitoring worker pool size
	err = wpc.monitorPoolSize(workerPool)
	if err != nil {
		log.Printf("<pool %s> unable to monitor pool size: %+v\n", wpc.name, err)
	}

	go wpc.deleteStalePendingWorkerJobs()

	return wpc, nil
}

func (wpc *KubernetesWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *KubernetesWorkerPoolController) poolId() string {
	data := fmt.Sprintf("%s-%s", wpc.workerPoolConfig.AgentToken, wpc.name)

	hasher := sha256.New()
	hasher.Write([]byte(data))
	hash := hasher.Sum(nil)
	poolId := hex.EncodeToString(hash[:8])

	return poolId
}

func (wpc *KubernetesWorkerPoolController) monitorPoolSize(workerPool *types.WorkerPoolResource) error {
	config, err := ParsePoolSizingConfig(workerPool.Spec.PoolSizing.Raw)
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

func (wpc *KubernetesWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
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

func (wpc *KubernetesWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	workerId := wpc.generateWorkerId()
	return wpc.addWorkerWithId(workerId, cpu, memory, gpuType)
}

func (wpc *KubernetesWorkerPoolController) AddWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	return wpc.addWorkerWithId(workerId, cpu, memory, gpuType)
}

func (wpc *KubernetesWorkerPoolController) addWorkerWithId(workerId string, cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	workerPool, err := wpc.getWorkerPool()
	if err != nil {
		return nil, err
	}

	// Create a new worker job
	job, worker := wpc.createWorkerJob(workerId, cpu, memory, gpuType, workerPool)

	// Set the WorkerPool resouce as the owner of the new job
	wpc.setJobOwner(job, workerPool)

	// Create the job in the cluster
	err = wpc.createJobInCluster(job)
	if err != nil {
		return nil, err
	}

	worker.PoolId = wpc.poolId()

	// Add the worker state
	err = wpc.workerRepo.AddWorker(worker)
	if err != nil {
		log.Printf("Unable to create worker: %+v\n", err)
		return nil, err
	}

	return worker, nil
}

// Gets worker pool (Custom Resource) from Kubernetes API
func (wpc *KubernetesWorkerPoolController) getWorkerPool() (*types.WorkerPoolResource, error) {
	workerPool, err := wpc.workerPoolClient.GetWorkerPool(wpc.controllerConfig.Namespace, wpc.name)
	if err != nil {
		return nil, err
	}
	return workerPool, nil
}

func (wpc *KubernetesWorkerPoolController) createWorkerJob(workerId string, cpu int64, memory int64, gpuType string, workerPool *types.WorkerPoolResource) (*batchv1.Job, *types.Worker) {
	jobName := fmt.Sprintf("%s-%s-%s", BeamWorkerJobPrefix, workerPool.Name, workerId)
	labels := map[string]string{
		BeamWorkerLabelKey: BeamWorkerLabelValue,
	}

	workerCpu := cpu
	workerMemory := memory
	workerGpu := gpuType

	resourceRequests := corev1.ResourceList{}
	if cpu > 0 && cpu > wpc.workerPoolConfig.DefaultWorkerCpuRequest {
		cpuString := fmt.Sprintf("%dm", cpu) // convert cpu to millicores string
		resourceRequests[corev1.ResourceCPU] = resource.MustParse(cpuString)
	} else {
		resourceRequests[corev1.ResourceCPU] = resource.MustParse(fmt.Sprintf("%dm", wpc.workerPoolConfig.DefaultWorkerCpuRequest))
		workerCpu = wpc.workerPoolConfig.DefaultWorkerCpuRequest
	}

	if memory > 0 && memory > wpc.workerPoolConfig.DefaultWorkerMemoryRequest {
		memoryString := fmt.Sprintf("%dMi", memory) // convert memory to Mi string
		resourceRequests[corev1.ResourceMemory] = resource.MustParse(memoryString)
	} else {
		resourceRequests[corev1.ResourceMemory] = resource.MustParse(fmt.Sprintf("%dMi", wpc.workerPoolConfig.DefaultWorkerMemoryRequest))
		workerMemory = wpc.workerPoolConfig.DefaultWorkerMemoryRequest
	}

	if gpuType != "" {
		resourceRequests[corev1.ResourceName("nvidia.com/gpu")] = *resource.NewQuantity(1, resource.DecimalSI)
	}

	workerImage := fmt.Sprintf("%s/%s:%s",
		common.Secrets().Get("BEAM_WORKER_IMAGE_REGISTRY"),
		common.Secrets().Get("BEAM_WORKER_IMAGE_NAME"),
		common.Secrets().Get("BEAM_WORKER_IMAGE_TAG"),
	)

	containers := []corev1.Container{
		{
			Name:  defaultContainerName,
			Image: workerImage,
			Command: []string{
				defaultWorkerEntrypoint,
			},
			Resources: corev1.ResourceRequirements{
				Requests: resourceRequests,
				Limits:   resourceRequests,
			},
			SecurityContext: &corev1.SecurityContext{
				Privileged: pointer.BoolPtr(true),
			},
			Env:          wpc.getWorkerEnvironment(workerId, workerCpu, workerMemory, workerGpu),
			VolumeMounts: wpc.getWorkerVolumeMounts(),
		},
	}

	// Add user-defined image pull secrets
	imagePullSecrets := []corev1.LocalObjectReference{}
	for _, s := range wpc.controllerConfig.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets, corev1.LocalObjectReference{Name: s})
	}

	podTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		},
		Spec: corev1.PodSpec{
			// TODO: change ServiceAccountName to be pulled from the CR instead of from a secret
			ServiceAccountName:           common.Secrets().GetWithDefault("BEAM_WORKER_SERVICE_ACCOUNT_NAME", "default"),
			AutomountServiceAccountToken: pointer.BoolPtr(true),
			HostNetwork:                  wpc.controllerConfig.HostNetwork,
			ImagePullSecrets:             imagePullSecrets,
			RestartPolicy:                corev1.RestartPolicyOnFailure,
			NodeSelector:                 wpc.getWorkerNodeSelector(),
			Containers:                   containers,
			Volumes:                      wpc.getWorkerVolumes(workerMemory),
			EnableServiceLinks:           pointer.BoolPtr(false),
		},
	}

	ttl := int32(30)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: wpc.controllerConfig.Namespace,
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

// Set the WorkerPool resource as the owner of the worker job
func (wpc *KubernetesWorkerPoolController) setJobOwner(job *batchv1.Job, workerPool *types.WorkerPoolResource) {
	controllerFlag := true
	controllerRef := metav1.OwnerReference{
		APIVersion:         workerPool.APIVersion,
		Kind:               workerPool.Kind,
		Name:               workerPool.Name,
		UID:                workerPool.UID,
		Controller:         &controllerFlag,
		BlockOwnerDeletion: &controllerFlag,
	}
	job.SetOwnerReferences(append(job.GetOwnerReferences(), controllerRef))
}

func (wpc *KubernetesWorkerPoolController) createJobInCluster(job *batchv1.Job) error {
	_, err := wpc.kubeClient.BatchV1().Jobs(wpc.controllerConfig.Namespace).Create(context.Background(), job, metav1.CreateOptions{})
	return err
}

func (wpc *KubernetesWorkerPoolController) getWorkerNodeSelector() map[string]string {
	stage := common.Secrets().GetWithDefault("STAGE", common.EnvLocal)

	if stage == common.EnvLocal {
		return map[string]string{}
	}

	return wpc.nodeSelector
}

func (wpc *KubernetesWorkerPoolController) getWorkerVolumes(workerMemory int64) []corev1.Volume {
	stage := common.Secrets().GetWithDefault("STAGE", common.EnvLocal)

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
		{
			Name: wpc.workerPoolConfig.DataVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: wpc.workerPoolConfig.DataVolumeName,
				},
			},
		},
	}

	// Local volumes
	if stage == common.EnvLocal {
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

	// Staging/Production volumes
	return append(volumes,
		corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  configSecretName,
					DefaultMode: func(mode int32) *int32 { return &mode }(420),
					Optional:    func(optional bool) *bool { return &optional }(false),
				},
			},
		},
		corev1.Volume{
			Name: imagesVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/images",
					Type: &hostPathType,
				},
			},
		},
	)
}

func (wpc *KubernetesWorkerPoolController) getWorkerVolumeMounts() []corev1.VolumeMount {
	stage := common.Secrets().GetWithDefault("STAGE", common.EnvLocal)

	volumeMounts := []corev1.VolumeMount{
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
		{
			Name:      wpc.workerPoolConfig.DataVolumeName,
			MountPath: "/packages",
			SubPath:   "packages/",
			ReadOnly:  true,
		},
		{
			Name:      wpc.workerPoolConfig.DataVolumeName,
			MountPath: "/snapshots",
			SubPath:   "snapshots/",
			ReadOnly:  false,
		},
		{
			Name:      wpc.workerPoolConfig.DataVolumeName,
			MountPath: "/outputs",
			SubPath:   "outputs/",
			ReadOnly:  false,
		},
		{
			Name:      wpc.workerPoolConfig.DataVolumeName,
			MountPath: "/workspaces",
			SubPath:   "workspaces/",
			ReadOnly:  false,
		},
		{
			Name:      wpc.workerPoolConfig.DataVolumeName,
			MountPath: "/volumes",
			SubPath:   "volumes/",
			ReadOnly:  false,
		},
	}

	if stage == common.EnvLocal {
		return volumeMounts
	}

	// Staging/Production volumes
	return append(volumeMounts,
		corev1.VolumeMount{
			Name:      configVolumeName,
			MountPath: "/etc/config",
			ReadOnly:  true,
		},
	)
}

func (wpc *KubernetesWorkerPoolController) getWorkerEnvironment(workerId string, cpu int64, memory int64, gpuType string) []corev1.EnvVar {
	stage := common.Secrets().GetWithDefault("STAGE", common.EnvLocal)

	// Base environment (common to both local/staging/production)
	env := []corev1.EnvVar{
		{
			Name:  "WORKER_ID",
			Value: workerId,
		},
		{
			Name:  "BEAM_NAMESPACE",
			Value: common.Secrets().Get("BEAM_NAMESPACE"),
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
			Name:  "WORKER_S2S_TOKEN",
			Value: common.Secrets().Get("WORKER_S2S_TOKEN"),
		},
		{
			Name:  "BEAM_RUNNER_BASE_IMAGE_REGISTRY",
			Value: common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_REGISTRY"),
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
			Value: common.Secrets().Get("WORKER_NAMESPACE"),
		},
		{
			Name:  "CLUSTER_DOMAIN",
			Value: defaultClusterDomain,
		},
		{
			Name:  "BEAM_CACHE_URL",
			Value: common.Secrets().Get("BEAM_CACHE_URL"),
		},
		{
			Name:  "BEAM_GATEWAY_HOST",
			Value: common.Secrets().Get("BEAM_GATEWAY_HOST"),
		},
		{
			Name:  "BEAM_GATEWAY_PORT",
			Value: common.Secrets().Get("BEAM_GATEWAY_PORT"),
		},
		{
			Name:  "STATSD_HOST",
			Value: common.Secrets().Get("STATSD_HOST"),
		},
		{
			Name:  "STATSD_PORT",
			Value: common.Secrets().Get("STATSD_PORT"),
		},
	}

	// Add env var to let Worker know it should run in remote mode
	if wpc.controllerConfig.IsRemote {
		env = append(env, corev1.EnvVar{Name: "WORKER_IS_REMOTE", Value: "true"})
	}

	// Add user-defined env vars
	env = append(env, wpc.controllerConfig.EnvVars...)

	// Local environmental variables
	if stage == common.EnvLocal {
		return env
	}

	// Staging/Production
	return append(
		env,
		corev1.EnvVar{
			Name:  "CONFIG_PATH",
			Value: "/etc/config/settings.env",
		},
		corev1.EnvVar{
			Name:  "KINESIS_STREAM_REGION",
			Value: common.Secrets().Get("KINESIS_STREAM_REGION"),
		},
	)

}

// deleteStalePendingWorkerJobs ensures that jobs are deleted if they don't
// start a pod after a certain amount of time.
func (wpc *KubernetesWorkerPoolController) deleteStalePendingWorkerJobs() {
	ctx := context.Background()
	maxAge := AddWorkerTimeout
	namespace := wpc.controllerConfig.Namespace

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		jobSelector := fmt.Sprintf("%s=%s", BeamWorkerLabelKey, BeamWorkerLabelValue)
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

func (wpc *KubernetesWorkerPoolController) generateWorkerId() string {
	return uuid.New().String()[:8]
}
