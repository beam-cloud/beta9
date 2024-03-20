package scheduler

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/internal/network"
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
)

type MetalWorkerPoolController struct {
	ctx            context.Context
	name           string
	config         types.AppConfig
	provider       providers.Provider
	tailscale      *network.Tailscale
	backendRepo    repository.BackendRepository
	workerPool     types.WorkerPoolConfig
	workerRepo     repository.WorkerRepository
	workerPoolRepo repository.WorkerPoolRepository
	providerName   *types.MachineProvider
	providerRepo   repository.ProviderRepository
}

func NewMetalWorkerPoolController(
	ctx context.Context,
	config types.AppConfig,
	workerPoolName string,
	backendRepo repository.BackendRepository,
	workerRepo repository.WorkerRepository,
	workerPoolRepo repository.WorkerPoolRepository,
	providerRepo repository.ProviderRepository,
	tailscale *network.Tailscale,
	providerName *types.MachineProvider) (WorkerPoolController, error) {
	var provider providers.Provider = nil
	var err error = nil

	switch *providerName {
	case types.ProviderEC2:
		provider, err = providers.NewEC2Provider(config, providerRepo, workerRepo, tailscale)
	case types.ProviderOCI:
		provider, err = providers.NewOCIProvider(config, providerRepo, workerRepo, tailscale)
	default:
		return nil, errors.New("invalid provider name")
	}
	if err != nil {
		return nil, err
	}

	workerPool := config.Worker.Pools[workerPoolName]
	wpc := &MetalWorkerPoolController{
		ctx:            ctx,
		name:           workerPoolName,
		config:         config,
		workerPool:     workerPool,
		backendRepo:    backendRepo,
		workerRepo:     workerRepo,
		workerPoolRepo: workerPoolRepo,
		providerName:   providerName,
		providerRepo:   providerRepo,
		tailscale:      tailscale,
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

func (wpc *MetalWorkerPoolController) chooseMachine() {

}

func (wpc *MetalWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error) {
	workerId := GenerateWorkerId()

	token, err := wpc.backendRepo.CreateToken(wpc.ctx, 1, types.TokenTypeWorker, false)
	if err != nil {
		return nil, err
	}

	machines, err := wpc.providerRepo.ListAllMachines(string(*wpc.providerName), wpc.name)
	if err != nil {
		return nil, err
	}

	for _, machine := range machines {
		log.Printf("worker keys: %+v\n", machine.WorkerKeys)
	}

	/*
			 Let's think this through. We need to find out if any of the running machines in this pool
			 Have the capacity to support this incoming worker.

			 So we need to:
			  - get all of the machines in the repo
			  - get a list of all their workers
			  - retrieve all their workers

		     A worker is never gonna move from one machine to another (for now), so once we get a list of the workers
			 we can guarantee that the worker is either on that node, occupying capacity, or its gone

			 Because of the worker TTL, if the worker key is there, we can assume that capacity is unavailable on the node.
			 So, I think what we can do here, is:
			   - Get a list of every single worker across all of the machines (using the index)
			   - Create a function that takes in a list of worker state keys, and retrieves all the workers based on that
			   - Map those workers to each machine, and do the math to figure out the current machine capacity for each machine
			   -

	*/

	log.Printf("existing machines: %+v\n", machines)

	machineId, err := wpc.provider.ProvisionMachine(wpc.ctx, wpc.name, token.Key, types.ProviderComputeRequest{
		Cpu:      cpu,
		Memory:   memory,
		Gpu:      gpuType,
		GpuCount: gpuCount,
	})
	if err != nil {
		return nil, err
	}

	err = wpc.providerRepo.SetMachineLock(string(*wpc.providerName), wpc.name, machineId)
	if err != nil {
		return nil, err
	}
	defer wpc.providerRepo.RemoveMachineLock(string(*wpc.providerName), wpc.name, machineId)

	log.Printf("Waiting for machine registration <machineId: %s>\n", machineId)
	state, err := wpc.providerRepo.WaitForMachineRegistration(string(*wpc.providerName), wpc.name, machineId)
	if err != nil {
		return nil, err
	}

	log.Printf("Machine registered <machineId: %s>, hostname: %s\n", machineId, state.HostName)
	log.Printf("State: %+v\n", state)
	client, err := wpc.getProxiedClient(state.HostName, state.Token)
	if err != nil {
		return nil, err
	}

	// Create a new worker job
	job, worker := wpc.createWorkerJob(workerId, machineId, cpu, memory, gpuType, gpuCount)
	worker.PoolId = PoolId(wpc.name)
	worker.MachineId = machineId

	// Create the job in the cluster
	_, err = client.BatchV1().Jobs(wpc.config.Worker.Namespace).Create(wpc.ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		log.Printf("Unable to create worker: %+v\n", err)
		return nil, err
	}

	return worker, nil
}

func (wpc *MetalWorkerPoolController) createWorkerJob(workerId, machineId string, cpu int64, memory int64, gpuType string, gpuCount uint32) (*batchv1.Job, *types.Worker) {
	jobName := fmt.Sprintf("%s-%s-%s", Beta9WorkerJobPrefix, wpc.name, workerId)
	labels := map[string]string{
		"app":               Beta9WorkerLabelValue,
		Beta9WorkerLabelKey: Beta9WorkerLabelValue,
		PrometheusScrapeKey: strconv.FormatBool(wpc.config.Monitoring.Prometheus.ScrapeWorkers),
	}

	workerCpu := cpu
	workerMemory := memory
	workerGpuType := gpuType
	workerGpuCount := gpuCount

	workerImage := fmt.Sprintf("%s/%s:%s",
		wpc.config.Worker.ImageRegistry,
		wpc.config.Worker.ImageName,
		wpc.config.Worker.ImageTag,
	)

	resources := corev1.ResourceRequirements{}
	if workerGpuType != "" {
		resources.Requests = corev1.ResourceList{
			"nvidia.com/gpu": *resource.NewQuantity(int64(gpuCount), resource.DecimalSI),
		}
		resources.Limits = corev1.ResourceList{
			"nvidia.com/gpu": *resource.NewQuantity(int64(gpuCount), resource.DecimalSI),
		}
	}

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
					ContainerPort: int32(wpc.config.Monitoring.Prometheus.Port),
				},
			},
			Env:          wpc.getWorkerEnvironment(workerId, machineId, workerCpu, workerMemory, workerGpuType, workerGpuCount),
			VolumeMounts: wpc.getWorkerVolumeMounts(),
			Resources:    resources,
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
		Id:       workerId,
		Cpu:      workerCpu,
		Memory:   workerMemory,
		Gpu:      workerGpuType,
		GpuCount: workerGpuCount,
		Status:   types.WorkerStatusPending,
	}
}

func (wpc *MetalWorkerPoolController) getWorkerEnvironment(workerId, machineId string, cpu int64, memory int64, gpuType string, gpuCount uint32) []corev1.EnvVar {
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
			Name:  "GPU_COUNT",
			Value: strconv.FormatInt(int64(gpuCount), 10),
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
	// Create a custom transport to skip tls & use tsnet for dialing
	transport := &http.Transport{
		DialContext:     wpc.tailscale.Dial,
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
	return freePoolCapacity(wpc.workerRepo, wpc)
}
