package scheduler

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

const (
	externalWorkerNamespace string = "default"
)

type ExternalWorkerPoolController struct {
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

func NewExternalWorkerPoolController(
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
		provider, err = providers.NewEC2Provider(ctx, config, providerRepo, workerRepo, tailscale)
	case types.ProviderOCI:
		provider, err = providers.NewOCIProvider(ctx, config, providerRepo, workerRepo, tailscale)
	case types.ProviderLambdaLabs:
		provider, err = providers.NewLambdaLabsProvider(ctx, config, providerRepo, workerRepo, tailscale)
	default:
		return nil, errors.New("invalid provider name")
	}
	if err != nil {
		return nil, err
	}

	workerPool := config.Worker.Pools[workerPoolName]
	wpc := &ExternalWorkerPoolController{
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
	err = MonitorPoolSize(wpc, &workerPool, workerPoolRepo)
	if err != nil {
		log.Printf("<pool %s> unable to monitor pool size: %+v\n", wpc.name, err)
	}

	// Reconcile nodes with state
	go provider.Reconcile(context.Background(), wpc.name)

	return wpc, nil
}

func (wpc *ExternalWorkerPoolController) AddWorker(cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error) {
	workerId := GenerateWorkerId()

	machines, err := wpc.providerRepo.ListAllMachines(wpc.provider.GetName(), wpc.name)
	if err != nil {
		return nil, err
	}

	// Attempt to schedule the worker on an existing machine first
	for _, machine := range machines {
		worker := func() *types.Worker {
			err := wpc.providerRepo.SetMachineLock(wpc.provider.GetName(), wpc.name, machine.State.MachineId)
			if err != nil {
				return nil
			}

			defer wpc.providerRepo.RemoveMachineLock(wpc.provider.GetName(), wpc.name, machine.State.MachineId)

			workers, err := wpc.workerRepo.GetAllWorkersOnMachine(machine.State.MachineId)
			if err != nil || machine.State.Status != types.MachineStatusRegistered {
				return nil
			}

			remainingMachineCpu := machine.State.Cpu
			remainingMachineMemory := machine.State.Memory
			remainingMachineGpuCount := machine.State.GpuCount
			for _, worker := range workers {
				remainingMachineCpu -= worker.TotalCpu
				remainingMachineMemory -= worker.TotalMemory
				remainingMachineGpuCount -= uint32(worker.TotalGpuCount)
			}

			if remainingMachineCpu >= int64(cpu) && remainingMachineMemory >= int64(memory) && machine.State.Gpu == gpuType && remainingMachineGpuCount >= gpuCount {
				log.Printf("Using existing machine <machineId: %s>, hostname: %s\n", machine.State.MachineId, machine.State.HostName)

				// If there is only one GPU available on the machine, give the worker access to everything
				// This prevents situations where a user requests a small amount of compute, and the subsequent
				// request has higher compute requirements
				if machine.State.GpuCount == 1 {
					cpu = machine.State.Cpu
					memory = machine.State.Memory
				}

				worker, err := wpc.createWorkerOnMachine(workerId, machine.State.MachineId, machine.State, cpu, memory, gpuType, gpuCount)
				if err != nil {
					return nil
				}

				return worker
			}

			return nil
		}()

		if worker != nil {
			return worker, nil
		}
	}

	// TODO: replace hard-coded workspace ID with look up of cluster admin
	token, err := wpc.backendRepo.CreateToken(wpc.ctx, 1, types.TokenTypeMachine, false)
	if err != nil {
		return nil, err
	}

	// Provision a new machine
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
	machineState, err := wpc.providerRepo.WaitForMachineRegistration(string(*wpc.providerName), wpc.name, machineId)
	if err != nil {
		return nil, err
	}

	log.Printf("Machine registered <machineId: %s>, hostname: %s\n", machineId, machineState.HostName)
	worker, err := wpc.createWorkerOnMachine(workerId, machineId, machineState, cpu, memory, gpuType, gpuCount)
	if err != nil {
		return nil, err
	}

	return worker, nil
}

func (wpc *ExternalWorkerPoolController) createWorkerOnMachine(workerId, machineId string, machineState *types.ProviderMachineState, cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error) {
	client, err := wpc.getProxiedClient(machineState.HostName, machineState.Token)
	if err != nil {
		return nil, err
	}

	// Create a new worker job
	job, worker, err := wpc.createWorkerJob(workerId, machineId, cpu, memory, gpuType, gpuCount)
	if err != nil {
		return nil, err
	}

	worker.PoolName = wpc.name
	worker.MachineId = machineId
	worker.RequiresPoolSelector = wpc.workerPool.RequiresPoolSelector

	// Create the job in the cluster
	_, err = client.BatchV1().Jobs(externalWorkerNamespace).Create(wpc.ctx, job, metav1.CreateOptions{})
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

func (wpc *ExternalWorkerPoolController) createWorkerJob(workerId, machineId string, cpu int64, memory int64, gpuType string, gpuCount uint32) (*batchv1.Job, *types.Worker, error) {
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

	env, err := wpc.getWorkerEnvironment(workerId, machineId, workerCpu, workerMemory, workerGpuType, workerGpuCount)
	if err != nil {
		return nil, nil, err
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
			Env:          env,
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
			HostNetwork:        true,
			ImagePullSecrets:   imagePullSecrets,
			RestartPolicy:      corev1.RestartPolicyOnFailure,
			NodeSelector:       wpc.workerPool.JobSpec.NodeSelector,
			Containers:         containers,
			Volumes:            wpc.getWorkerVolumes(workerMemory),
			EnableServiceLinks: ptr.To(false),
			DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
		},
	}

	if wpc.workerPool.Runtime != "" {
		podTemplate.Spec.RuntimeClassName = ptr.To(wpc.workerPool.Runtime)
	}

	ttl := int32(30)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: externalWorkerNamespace,
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
	}, nil
}

func (wpc *ExternalWorkerPoolController) getWorkerEnvironment(workerId, machineId string, cpu int64, memory int64, gpuType string, gpuCount uint32) ([]corev1.EnvVar, error) {
	podHostname := fmt.Sprintf("%s.%s", machineId, wpc.config.Tailscale.HostName)
	if wpc.config.Tailscale.User != "" {
		podHostname = fmt.Sprintf("%s.%s.%s", machineId, wpc.config.Tailscale.User, wpc.config.Tailscale.HostName)
	}

	envVars := []corev1.EnvVar{
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
			Value: externalWorkerNamespace,
		},
		{
			Name:  "BETA9_GATEWAY_HOST",
			Value: wpc.config.GatewayService.ExternalHost,
		},
		{
			Name:  "BETA9_GATEWAY_PORT",
			Value: "443",
		},
		{
			Name:  "POD_HOSTNAME",
			Value: podHostname,
		},
	}

	remoteConfig, err := providers.GetRemoteConfig(wpc.config, wpc.tailscale)
	if err != nil {
		return nil, err
	}

	// Serialize the AppConfig struct to JSON
	configJson, err := json.MarshalIndent(remoteConfig, "", "  ")
	if err == nil {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "CONFIG_JSON",
			Value: string(configJson),
		})
	}

	return envVars, nil
}

func (wpc *ExternalWorkerPoolController) getWorkerVolumes(workerMemory int64) []corev1.Volume {
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

func (wpc *ExternalWorkerPoolController) getWorkerVolumeMounts() []corev1.VolumeMount {
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

func (wpc *ExternalWorkerPoolController) getProxiedClient(hostname, token string) (*kubernetes.Clientset, error) {
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

func (wpc *ExternalWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *ExternalWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	return freePoolCapacity(wpc.workerRepo, wpc)
}
