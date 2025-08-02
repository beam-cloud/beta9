package scheduler

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

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
	ctx              context.Context
	name             string
	config           types.AppConfig
	provider         providers.Provider
	tailscale        *network.Tailscale
	backendRepo      repository.BackendRepository
	workerPoolConfig types.WorkerPoolConfig
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	providerName     *types.MachineProvider
	providerRepo     repository.ProviderRepository
	containerRepo    repository.ContainerRepository
	eventRepo        repository.EventRepository
	workspace        *types.Workspace
}

func NewExternalWorkerPoolController(opts WorkerPoolControllerOptions) (WorkerPoolController, error) {
	var provider providers.Provider = nil
	var err error = nil

	workerPoolName := opts.Name
	providerName := opts.ProviderName
	opts.Config.Worker.Namespace = externalWorkerNamespace

	switch *providerName {
	case types.ProviderEC2:
		provider, err = providers.NewEC2Provider(opts.Context, opts.Config, opts.ProviderRepo, opts.WorkerRepo, opts.Tailscale)
	case types.ProviderOCI:
		provider, err = providers.NewOCIProvider(opts.Context, opts.Config, opts.ProviderRepo, opts.WorkerRepo, opts.Tailscale)
	case types.ProviderLambdaLabs:
		provider, err = providers.NewLambdaLabsProvider(opts.Context, opts.Config, opts.ProviderRepo, opts.WorkerRepo, opts.Tailscale)
	case types.ProviderCrusoe:
		provider, err = providers.NewCrusoeProvider(opts.Context, opts.Config, opts.ProviderRepo, opts.WorkerRepo, opts.Tailscale)
	case types.ProviderHydra:
		provider, err = providers.NewHydraProvider(opts.Context, opts.Config, opts.ProviderRepo, opts.WorkerRepo, opts.Tailscale)
	case types.ProviderGeneric:
		provider, err = providers.NewGenericProvider(opts.Context, opts.Config, opts.ProviderRepo, opts.WorkerRepo, opts.Tailscale)
	default:
		return nil, errors.New("invalid provider name")
	}
	if err != nil {
		return nil, err
	}

	workerPoolConfig := opts.Config.Worker.Pools[workerPoolName]
	wpc := &ExternalWorkerPoolController{
		ctx:              opts.Context,
		name:             workerPoolName,
		config:           opts.Config,
		workerPoolConfig: workerPoolConfig,
		containerRepo:    opts.ContainerRepo,
		backendRepo:      opts.BackendRepo,
		workerRepo:       opts.WorkerRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
		providerName:     providerName,
		providerRepo:     opts.ProviderRepo,
		eventRepo:        opts.EventRepo,
		tailscale:        opts.Tailscale,
		provider:         provider,
	}

	// Start monitoring worker pool size
	err = MonitorPoolSize(wpc, &workerPoolConfig, wpc.workerRepo, wpc.workerPoolRepo, opts.ProviderRepo)
	if err != nil {
		log.Error().Str("pool_name", wpc.name).Err(err).Msg("unable to monitor pool size")
	}

	// Start monitoring worker pool health
	err = MonitorPoolHealth(PoolHealthMonitorOptions{
		Controller:       wpc,
		WorkerPoolConfig: workerPoolConfig,
		WorkerConfig:     wpc.config.Worker,
		WorkerRepo:       wpc.workerRepo,
		ProviderRepo:     wpc.providerRepo,
		WorkerPoolRepo:   wpc.workerPoolRepo,
		ContainerRepo:    wpc.containerRepo,
		EventRepo:        wpc.eventRepo,
	})
	if err != nil {
		log.Error().Str("pool_name", wpc.name).Err(err).Msg("unable to monitor pool health")
	}

	// Reconcile nodes with state
	go provider.Reconcile(wpc.ctx, wpc.name)

	// Monitor and cleanup workers
	go wpc.monitorAndCleanupWorkers()

	return wpc, nil
}

func (wpc *ExternalWorkerPoolController) Context() context.Context {
	return wpc.ctx
}

func (wpc *ExternalWorkerPoolController) IsPreemptable() bool {
	return wpc.workerPoolConfig.Preemptable
}

func (wpc *ExternalWorkerPoolController) State() (*types.WorkerPoolState, error) {
	return wpc.workerPoolRepo.GetWorkerPoolState(wpc.ctx, wpc.name)
}

func (wpc *ExternalWorkerPoolController) Mode() types.PoolMode {
	return wpc.workerPoolConfig.Mode
}

func (wpc *ExternalWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *ExternalWorkerPoolController) RequiresPoolSelector() bool {
	return wpc.workerPoolConfig.RequiresPoolSelector
}

func (wpc *ExternalWorkerPoolController) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	workerId := GenerateWorkerId()

	machines, err := wpc.providerRepo.ListAllMachines(wpc.provider.GetName(), wpc.name, true)
	if err != nil {
		return nil, err
	}

	// Attempt to schedule the worker on an existing machine first
	for _, machine := range machines {
		worker, err := wpc.attemptToAssignWorkerToMachine(workerId, cpu, memory, wpc.workerPoolConfig.GPUType, gpuCount, machine)
		if err != nil {
			continue
		}

		if worker == nil {
			continue
		}

		return worker, nil
	}

	adminWorkspace, err := wpc.backendRepo.GetAdminWorkspace(wpc.ctx)
	if err != nil {
		return nil, err
	}

	token, err := wpc.backendRepo.CreateToken(wpc.ctx, adminWorkspace.Id, types.TokenTypeMachine, false)
	if err != nil {
		return nil, err
	}

	// Provision a new machine
	machineId, err := wpc.provider.ProvisionMachine(wpc.ctx, wpc.name, token.Key, types.ProviderComputeRequest{
		Cpu:      cpu,
		Memory:   memory,
		Gpu:      wpc.workerPoolConfig.GPUType,
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

	log.Info().Str("machine_id", machineId).Msg("waiting for machine registration")
	machineState, err := wpc.providerRepo.WaitForMachineRegistration(string(*wpc.providerName), wpc.name, machineId)
	if err != nil {
		return nil, err
	}

	log.Info().Str("machine_id", machineId).Str("hostname", machineState.HostName).Msg("machine registered")
	worker, err := wpc.createWorkerOnMachine(workerId, machineId, machineState, cpu, memory, wpc.workerPoolConfig.GPUType, gpuCount)
	if err != nil {
		return nil, err
	}

	return worker, nil
}

func (wpc *ExternalWorkerPoolController) AddWorkerToMachine(cpu int64, memory int64, gpuType string, gpuCount uint32, machineId string) (*types.Worker, error) {
	workerId := GenerateWorkerId()

	machine, err := wpc.providerRepo.GetMachine(wpc.provider.GetName(), wpc.name, machineId)
	if err != nil {
		return nil, err
	}

	worker, err := wpc.attemptToAssignWorkerToMachine(workerId, cpu, memory, gpuType, gpuCount, machine)
	if err != nil {
		return nil, err
	}

	return worker, nil
}

func (wpc *ExternalWorkerPoolController) attemptToAssignWorkerToMachine(workerId string, cpu int64, memory int64, gpuType string, gpuCount uint32, machine *types.ProviderMachine) (*types.Worker, error) {
	err := wpc.providerRepo.SetMachineLock(wpc.provider.GetName(), wpc.name, machine.State.MachineId)
	if err != nil {
		return nil, err
	}

	defer wpc.providerRepo.RemoveMachineLock(wpc.provider.GetName(), wpc.name, machine.State.MachineId)

	workers, err := wpc.workerRepo.GetAllWorkersOnMachine(machine.State.MachineId)
	if err != nil {
		return nil, err
	}

	if machine.State.Status != types.MachineStatusReady {
		return nil, nil
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
		log.Info().Str("machine_id", machine.State.MachineId).Str("hostname", machine.State.HostName).Msg("using existing machine")

		// If there is only one GPU available on the machine, give the worker access to everything
		// This prevents situations where a user requests a small amount of compute, and the subsequent
		// request has higher compute requirements
		if machine.State.GpuCount == 1 {
			cpu = machine.State.Cpu
			memory = machine.State.Memory
			gpuCount = machine.State.GpuCount
		}

		worker, err := wpc.createWorkerOnMachine(workerId, machine.State.MachineId, machine.State, cpu, memory, gpuType, gpuCount)
		if err != nil {
			return nil, err
		}

		return worker, nil
	}

	return nil, nil
}

func (wpc *ExternalWorkerPoolController) createWorkerOnMachine(workerId, machineId string, machineState *types.ProviderMachineState, cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error) {
	if wpc.workspace == nil {
		adminWorkspace, err := wpc.backendRepo.GetAdminWorkspace(wpc.ctx)
		if err != nil {
			return nil, errors.New("workspace not configured for pool: " + wpc.name)
		}

		wpc.workspace = adminWorkspace
	}

	client, err := wpc.getProxiedClient(machineState.HostName, machineState.Token)
	if err != nil {
		return nil, err
	}

	token, err := wpc.backendRepo.CreateToken(wpc.ctx, wpc.workspace.Id, types.TokenTypeWorker, true)
	if err != nil {
		return nil, err
	}

	// Create a new worker job
	job, worker, err := wpc.createWorkerJob(workerId, machineId, cpu, memory, gpuType, gpuCount, token.Key)
	if err != nil {
		return nil, err
	}

	worker.PoolName = wpc.name
	worker.MachineId = machineId
	worker.RequiresPoolSelector = wpc.workerPoolConfig.RequiresPoolSelector

	// Create the job in the cluster
	_, err = client.BatchV1().Jobs(wpc.config.Worker.Namespace).Create(wpc.ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		log.Error().Err(err).Msg("unable to create worker")
		return nil, err
	}

	return worker, nil
}

func (wpc *ExternalWorkerPoolController) createWorkerJob(workerId, machineId string, cpu int64, memory int64, gpuType string, gpuCount uint32, token string) (*batchv1.Job, *types.Worker, error) {
	jobName := fmt.Sprintf("%s-%s-%s", Beta9WorkerJobPrefix, wpc.name, workerId)
	labels := map[string]string{
		"app":                       Beta9WorkerLabelValue,
		Beta9WorkerLabelKey:         Beta9WorkerLabelValue,
		Beta9WorkerLabelPoolNameKey: wpc.name,
		Beta9WorkerLabelIDKey:       workerId,
		Beta9MachineLabelIDKey:      machineId,
		PrometheusScrapeKey:         strconv.FormatBool(wpc.config.Monitoring.Prometheus.ScrapeWorkers),
	}

	workerCpu := cpu
	workerMemory := memory
	workerGpuType := wpc.workerPoolConfig.GPUType
	workerGpuCount := gpuCount

	workerImage := fmt.Sprintf("%s/%s:%s",
		wpc.config.Worker.ImageRegistry,
		wpc.config.Worker.ImageName,
		"ax-6", // wpc.config.Worker.ImageTag,
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

	env, err := wpc.getWorkerEnvironment(workerId, machineId, workerCpu, workerMemory, workerGpuType, workerGpuCount, token)
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
			NodeSelector:       wpc.workerPoolConfig.JobSpec.NodeSelector,
			Containers:         containers,
			Volumes:            wpc.getWorkerVolumes(workerMemory),
			EnableServiceLinks: ptr.To(false),
			DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
		},
	}

	if wpc.workerPoolConfig.Runtime != "" {
		podTemplate.Spec.RuntimeClassName = ptr.To(wpc.workerPoolConfig.Runtime)
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
		Priority:      wpc.workerPoolConfig.Priority,
		BuildVersion:  wpc.config.Worker.ImageTag,
		Preemptable:   wpc.workerPoolConfig.Preemptable,
	}, nil
}

func (wpc *ExternalWorkerPoolController) getWorkerEnvironment(workerId, machineId string, cpu int64, memory int64, gpuType string, gpuCount uint32, token string) ([]corev1.EnvVar, error) {
	// HOTFIX: clean up the way we pass tailscale hostname to remote worker
	podHostname := fmt.Sprintf("machine-%s.%s", machineId, wpc.config.Tailscale.HostName)
	if wpc.config.Tailscale.User != "" {
		podHostname = fmt.Sprintf("machine-%s.%s.%s", machineId, wpc.config.Tailscale.User, wpc.config.Tailscale.HostName)
	}

	locality := wpc.workerPoolConfig.ConfigGroup
	if locality == "" {
		locality = wpc.config.BlobCache.Global.DefaultLocality
	}

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
			Name:  "BLOBCACHE_LOCALITY",
			Value: locality,
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
			Name:  "POD_NAMESPACE",
			Value: wpc.config.Worker.Namespace,
		},
		{
			Name:  "BETA9_GATEWAY_HOST",
			Value: wpc.config.GatewayService.GRPC.ExternalHost,
		},
		{
			Name:  "BETA9_GATEWAY_PORT",
			Value: "443",
		},
		{
			Name:  "BETA9_GATEWAY_HOST_HTTP",
			Value: wpc.config.GatewayService.HTTP.ExternalHost,
		},
		{
			Name:  "BETA9_GATEWAY_PORT_HTTP",
			Value: "443",
		},
		{
			Name:  "POD_HOSTNAME",
			Value: podHostname,
		},
		{
			Name:  "TS_DEBUG_DISABLE_PORTLIST",
			Value: "true",
		},
		{
			Name:  "NETWORK_PREFIX",
			Value: machineId,
		},
		{
			Name:  "PREEMPTABLE",
			Value: strconv.FormatBool(wpc.workerPoolConfig.Preemptable),
		},
		{
			Name:  "ALLUXIO_COORDINATOR_HOSTNAME",
			Value: wpc.workerPoolConfig.AlluxioCoordinatorHostname,
		},
	}

	remoteConfig, err := providers.GetRemoteConfig(wpc.config, wpc.tailscale)
	if err != nil {
		return nil, err
	}

	// TODO: Once we set up dynamic secrets updating in agents, we can remove this
	remoteConfig.Monitoring.FluentBit.Events.Endpoint = "http://beta9-fluent-bit.kube-system:9880"

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
	sharedMemoryLimit := calculateMemoryQuantity(wpc.workerPoolConfig.PoolSizing.SharedMemoryLimitPct, workerMemory)
	tmpSizeLimit := parseTmpSizeLimit(wpc.workerPoolConfig.TmpSizeLimit, wpc.config.Worker.TmpSizeLimit)

	storagePath := wpc.workerPoolConfig.StoragePath
	if storagePath == "" {
		storagePath = defaultStoragePath
	}

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
		{
			Name: storageVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: storagePath,
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
			Name:      storageVolumeName,
			MountPath: "/storage",
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

func (wpc *ExternalWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	return freePoolCapacity(wpc.workerRepo, wpc)
}

func (wpc *ExternalWorkerPoolController) monitorAndCleanupWorkers() {
	cleaner := WorkerResourceCleaner{
		PoolName:   wpc.name,
		Config:     wpc.config.Worker,
		EventRepo:  wpc.eventRepo,
		WorkerRepo: wpc.workerRepo,
	}

	ticker := time.NewTicker(wpc.config.Worker.CleanupWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wpc.ctx.Done():
			return
		case <-ticker.C:
			if err := wpc.workerPoolRepo.SetWorkerCleanerLock(wpc.name); err != nil {
				continue
			}

			machines, err := wpc.providerRepo.ListAllMachines(wpc.provider.GetName(), wpc.name, true)
			if err != nil {
				continue
			}

			for _, machine := range machines {
				kubeClient, err := wpc.getProxiedClient(machine.State.HostName, machine.State.Token)
				if err != nil {
					continue
				}

				cleaner.KubeClient = kubeClient
				cleaner.Clean(wpc.ctx)
			}

			wpc.workerPoolRepo.RemoveWorkerCleanerLock(wpc.name)
		}
	}
}
