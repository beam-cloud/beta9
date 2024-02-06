package scheduler

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/internal/providers"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"tailscale.com/tsnet"
)

type MetalWorkerPoolController struct {
	name           string
	config         types.AppConfig
	provider       providers.Provider
	workerPool     types.WorkerPoolConfig
	workerRepo     repository.WorkerRepository
	workerPoolRepo repository.WorkerPoolRepository
	tailscaleRepo  repository.TailscaleRepository
	providerName   *types.MachineProvider
	providerRepo   repository.ProviderRepository
}

func NewMetalWorkerPoolController(
	config types.AppConfig,
	workerPoolName string,
	workerRepo repository.WorkerRepository,
	workerPoolRepo repository.WorkerPoolRepository,
	providerRepo repository.ProviderRepository,
	tailscaleRepo repository.TailscaleRepository,
	providerName *types.MachineProvider) (WorkerPoolController, error) {
	var provider providers.Provider = nil
	var err error = nil

	switch *providerName {
	case types.ProviderEC2:
		provider, err = providers.NewEC2Provider(config, tailscaleRepo)
	default:
		return nil, errors.New("invalid provider name")
	}
	if err != nil {
		return nil, err
	}

	workerPool, _ := config.Worker.Pools[workerPoolName]
	wpc := &MetalWorkerPoolController{
		name:           workerPoolName,
		config:         config,
		workerPool:     workerPool,
		workerRepo:     workerRepo,
		workerPoolRepo: workerPoolRepo,
		providerName:   providerName,
		providerRepo:   providerRepo,
		tailscaleRepo:  tailscaleRepo,
		provider:       provider,
	}

	// Start monitoring worker pool size
	err = MonitorPoolSize(wpc, &workerPool)
	if err != nil {
		log.Printf("<pool %s> unable to monitor pool size: %+v\n", wpc.name, err)
	}

	// Reconcile nodes with state
	go provider.Reconcile(context.Background(), wpc.name)

	return wpc, nil
}

func (wpc *MetalWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string) (*types.Worker, error) {
	workerId := GenerateWorkerId()

	machineId, err := wpc.provider.ProvisionMachine(context.TODO(), wpc.name, types.ProviderComputeRequest{
		Cpu:    cpu,
		Memory: memory,
		Gpu:    gpuType,
	})
	if err != nil {
		return nil, err
	}

	log.Printf("Waiting for machine registration <machineId: %s>\n", machineId)
	state, err := wpc.providerRepo.WaitForMachineRegistration(string(*wpc.providerName), wpc.name, machineId)
	if err != nil {
		return nil, err
	}
	log.Printf("Machine registered <machineId: %s>, hostname: %s\n", machineId, state.HostName)

	client, err := wpc.getProxiedClient(state.HostName, state.Token)
	if err != nil {
		return nil, err
	}

	// TODO: fix client to check for connectivity at the hostname before creating a job

	// Create a new worker job
	job, worker := wpc.createWorkerJob(workerId, machineId, cpu, memory, gpuType)
	worker.PoolId = PoolId(wpc.name)
	worker.MachineId = machineId

	// Create the job in the cluster
	_, err = client.BatchV1().Jobs(wpc.config.Worker.Namespace).Create(context.Background(), job, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		log.Printf("Unable to create worker: %+v\n", err)
		return nil, err
	}

	// TODO: if anything at all fails after machine creation, terminate machine immediately (deprovision)

	return worker, nil
}

func (wpc *MetalWorkerPoolController) createWorkerJob(workerId, machineId string, cpu int64, memory int64, gpuType string) (*batchv1.Job, *types.Worker) {
	jobName := fmt.Sprintf("%s-%s-%s", Beta9WorkerJobPrefix, wpc.name, workerId)
	labels := map[string]string{
		"app":               Beta9WorkerLabelValue,
		Beta9WorkerLabelKey: Beta9WorkerLabelValue,
		PrometheusScrapeKey: strconv.FormatBool(wpc.config.Metrics.Prometheus.ScrapeWorkers),
	}

	workerCpu := cpu
	workerMemory := memory
	workerGpu := gpuType

	workerImage := fmt.Sprintf("%s/%s:%s",
		wpc.config.Worker.ImageRegistry,
		wpc.config.Worker.ImageName,
		wpc.config.Worker.ImageTag,
	)

	containers := []corev1.Container{
		{
			Name:  defaultContainerName,
			Image: workerImage,
			Command: []string{
				defaultWorkerEntrypoint,
			},
			SecurityContext: &corev1.SecurityContext{
				Privileged: ptr.To(true),
			},
			Ports: []corev1.ContainerPort{
				{
					Name:          "metrics",
					ContainerPort: int32(wpc.config.Metrics.Prometheus.Port),
				},
			},
			Env:          wpc.getWorkerEnvironment(workerId, machineId, workerCpu, workerMemory, workerGpu),
			VolumeMounts: wpc.getWorkerVolumeMounts(),
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				},
				Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				},
			},
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
			HostNetwork:                  true,
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

func (wpc *MetalWorkerPoolController) getWorkerEnvironment(workerId, machineId string, cpu int64, memory int64, gpuType string) []corev1.EnvVar {
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
		{
			Name:  "POD_HOSTNAME",
			Value: fmt.Sprintf("%s.%s.%s", machineId, wpc.config.Tailscale.User, wpc.config.Tailscale.HostName),
		},
		{
			Name:  "CONFIG_PATH",
			Value: "/etc/config/config.json",
		},
		// TODO: remove these creds
		{
			Name:  "AWS_REGION",
			Value: wpc.config.ImageService.Registries.S3.Region,
		},
		{
			Name:  "AWS_ACCESS_KEY_ID",
			Value: wpc.config.ImageService.Registries.S3.AccessKeyID,
		},
		{
			Name:  "AWS_SECRET_ACCESS_KEY",
			Value: wpc.config.ImageService.Registries.S3.SecretAccessKey,
		},
	}
}

func (wpc *MetalWorkerPoolController) getWorkerVolumes(workerMemory int64) []corev1.Volume {
	hostPathType := corev1.HostPathDirectoryOrCreate
	sharedMemoryLimit := resource.MustParse(fmt.Sprintf("%dMi", workerMemory/2))

	tmpSizeLimit := resource.MustParse("30Gi")
	return []corev1.Volume{
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
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: configSecretName,
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
			Name: imagesVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: defaultImagesPath,
					Type: &hostPathType,
				},
			},
		},
	}
}

func (wpc *MetalWorkerPoolController) getWorkerVolumeMounts() []corev1.VolumeMount {
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
			Name:      configVolumeName,
			MountPath: "/etc/config",
			ReadOnly:  true,
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

func (wpc *MetalWorkerPoolController) getProxiedClient(hostname, token string) (*kubernetes.Clientset, error) {
	var s *tsnet.Server = wpc.tailscaleRepo.GetTailscaleServer()

	// Create a custom transport to skip tls & use tsnet for dialing
	transport := &http.Transport{
		DialContext:     s.Dial,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	kubeConfig := &rest.Config{
		Host:            fmt.Sprintf("https://%s:6443", hostname),
		BearerToken:     token,
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
	}
	kubeConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		return transport
	}

	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	return kubeClient, nil
}

func (wpc *MetalWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *MetalWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	workers, err := wpc.workerRepo.GetAllWorkersInPool(PoolId(wpc.name))
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
