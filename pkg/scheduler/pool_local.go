package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/rs/zerolog/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

const (
	gatewayServiceAccountEnv = "BETA9_GATEWAY_SERVICE_ACCOUNT"
	gatewayNamespaceEnv      = "BETA9_GATEWAY_NAMESPACE"

	// workerJobCreateTimeout is how long the cluster is given to accept a
	// worker Job.
	workerJobCreateTimeout = 30 * time.Second
	// workerTokenRevokeTimeout is how long the database is given to revoke
	// the token of a worker that was not added.
	workerTokenRevokeTimeout = 10 * time.Second
)

// A "local" k8s worker pool controller means
// the pool is local to the control plane / in-cluster
type LocalKubernetesWorkerPoolController struct {
	ctx              context.Context
	name             string
	config           types.AppConfig
	kubeClient       kubernetes.Interface
	clock            common.Clock
	workerPoolConfig types.WorkerPoolConfig
	workerRepo       repository.WorkerRepository
	workerPoolRepo   repository.WorkerPoolRepository
	backendRepo      repository.BackendRepository
	containerRepo    repository.ContainerRepository
	eventRepo        repository.EventRepository
	stall            stallCheck
}

func NewLocalKubernetesWorkerPoolController(opts WorkerPoolControllerOptions) (WorkerPoolController, error) {
	kubeClient := opts.KubeClient
	if kubeClient == nil {
		kubeConfig, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}

		kubeClient, err = kubernetes.NewForConfig(kubeConfig)
		if err != nil {
			return nil, err
		}
	}

	workerPoolName := opts.Name
	workerPoolConfig := opts.Config.Worker.Pools[workerPoolName]
	wpc := &LocalKubernetesWorkerPoolController{
		ctx:              opts.Context,
		name:             opts.Name,
		config:           opts.Config,
		kubeClient:       kubeClient,
		clock:            common.ClockOrReal(opts.Clock),
		workerPoolConfig: workerPoolConfig,
		workerRepo:       opts.WorkerRepo,
		backendRepo:      opts.BackendRepo,
		workerPoolRepo:   opts.WorkerPoolRepo,
		containerRepo:    opts.ContainerRepo,
		eventRepo:        opts.EventRepo,
	}

	MonitorPoolSize(wpc, opts.Config, &workerPoolConfig, wpc.workerPoolRepo, wpc.clock)
	MonitorPoolHealth(PoolHealthMonitorOptions{
		Controller:       wpc,
		WorkerPoolConfig: workerPoolConfig,
		WorkerRepo:       wpc.workerRepo,
		WorkerPoolRepo:   wpc.workerPoolRepo,
		ContainerRepo:    wpc.containerRepo,
		PushMetrics:      opts.PushPoolMetrics,
		Clock:            wpc.clock,
	})
	wpc.monitorAndCleanupWorkers()

	return wpc, nil
}

func (wpc *LocalKubernetesWorkerPoolController) Context() context.Context {
	return wpc.ctx
}

func (wpc *LocalKubernetesWorkerPoolController) IsPreemptable() bool {
	return wpc.workerPoolConfig.Preemptable
}

func (wpc *LocalKubernetesWorkerPoolController) Name() string {
	return wpc.name
}

func (wpc *LocalKubernetesWorkerPoolController) ContainerRuntime() string {
	return wpc.workerPoolConfig.ContainerRuntime
}

func (wpc *LocalKubernetesWorkerPoolController) RequiresPoolSelector() bool {
	return wpc.workerPoolConfig.RequiresPoolSelector
}

func (wpc *LocalKubernetesWorkerPoolController) FreeCapacity() (*WorkerPoolCapacity, error) {
	return freePoolCapacity(wpc.workerRepo, wpc.name)
}

func (wpc *LocalKubernetesWorkerPoolController) State() (*types.WorkerPoolState, error) {
	return wpc.workerPoolRepo.GetWorkerPoolState(wpc.ctx, wpc.name)
}

// CanAddWorker is false while the pool is stalled: a worker Job added now
// would wait for a node behind the ones that already are.
func (wpc *LocalKubernetesWorkerPoolController) CanAddWorker(cpu int64, memory int64, gpuCount uint32) (bool, error) {
	stalled, err := wpc.stalled()
	return !stalled, err
}

func (wpc *LocalKubernetesWorkerPoolController) AddWorker(cpu int64, memory int64, gpuCount uint32) (*types.Worker, error) {
	workerId := GenerateWorkerId()
	return wpc.addWorkerWithId(workerId, cpu, memory, wpc.workerPoolConfig.GPUType, gpuCount)
}

func (wpc *LocalKubernetesWorkerPoolController) addWorkerWithId(workerId string, cpu int64, memory int64, gpuType string, gpuCount uint32) (*types.Worker, error) {
	if wpc.workerUsesGatewayServiceAccount() {
		return nil, fmt.Errorf("worker pool %s: workers must use a different service account from the gateway", wpc.name)
	}
	if err := validateLocalWorkerJobSpec(wpc.workerPoolConfig.JobSpec); err != nil {
		return nil, fmt.Errorf("worker pool %s: %w", wpc.name, err)
	}
	// A worker's token is the admin workspace's. The backend remembers the
	// workspace, so it is asked each time: the pool's sizer and the requests
	// that grow the pool add workers at the same time, and would race for a
	// copy kept here.
	workspace, err := wpc.backendRepo.GetAdminWorkspace(wpc.ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to look up the admin workspace for pool %s: %w", wpc.name, err)
	}

	token, err := wpc.backendRepo.CreateWorkerToken(wpc.ctx, workspace.Id, types.TokenTypeWorker, workerId)
	if err != nil {
		return nil, err
	}

	// Create a new worker job
	job, worker := wpc.createWorkerJob(workerId, cpu, memory, gpuType, gpuCount, token.Key)

	// Create the job in the cluster
	if err := wpc.createJobInCluster(job); err != nil {
		wpc.revokeWorkerToken(workerId, token)
		return nil, err
	}

	worker.PoolName = wpc.name
	worker.RequiresPoolSelector = wpc.workerPoolConfig.RequiresPoolSelector

	// Add the worker state
	if err := wpc.workerRepo.AddWorker(worker); err != nil {
		workerLog(log.Error(), worker).Err(err).Msg("unable to create worker")
		// The Job of a worker that is not on record is removed by the
		// cleaner, so its token goes now. But the error may have come after
		// the record was written: when the lock that guards the write could
		// not be released, say. That worker is one the scheduler waits for.
		// It has to be able to start, and keeps its token.
		var notFound *types.ErrWorkerNotFound
		if _, lookupErr := wpc.workerRepo.GetWorkerById(workerId); errors.As(lookupErr, &notFound) {
			wpc.revokeWorkerToken(workerId, token)
		}
		return nil, err
	}

	return worker, nil
}

func (wpc *LocalKubernetesWorkerPoolController) workerUsesGatewayServiceAccount() bool {
	gatewayNamespace := os.Getenv(gatewayNamespaceEnv)
	if gatewayNamespace == "" || gatewayNamespace != wpc.config.Worker.Namespace {
		return false
	}
	workerServiceAccount := wpc.config.Worker.ServiceAccountName
	if workerServiceAccount == "" {
		workerServiceAccount = "default"
	}
	return workerServiceAccount == os.Getenv(gatewayServiceAccountEnv)
}

func validateLocalWorkerJobSpec(spec types.WorkerPoolJobSpecConfig) error {
	for _, env := range spec.Env {
		switch env.Name {
		case types.WorkerConfigPathEnv, types.WorkerConfigJSONEnv, types.WorkerMinimalConfigEnv, types.WorkerTokenEnv, types.CacheServerOnlyEnv:
			return fmt.Errorf("jobSpec.env cannot set %s", env.Name)
		}
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil && localWorkerReservedSecret(env.ValueFrom.SecretKeyRef.Name) {
			return errors.New("jobSpec.env cannot reference a gateway or cache-server config Secret")
		}
	}
	for _, volume := range spec.Volumes {
		if localWorkerReservedSecret(volume.Secret.SecretName) {
			return errors.New("jobSpec.volumes cannot mount a gateway or cache-server config Secret")
		}
	}
	for _, mount := range spec.VolumeMounts {
		// The shared config loader discovers YAML and JSON files here before
		// applying the scheduler's worker projection.
		const configDir = "/etc/beta9.d/"
		mountDir := strings.TrimSuffix(path.Clean(mount.MountPath), "/") + "/"
		if strings.HasPrefix(mountDir, configDir) || strings.HasPrefix(configDir, mountDir) {
			return errors.New("jobSpec.volumeMounts cannot cover /etc/beta9.d")
		}
	}
	return nil
}

func localWorkerReservedSecret(name string) bool {
	switch name {
	case "beta9-config", "beta9-config-helm", "beta9-cache-server-config":
		return true
	default:
		return false
	}
}

// revokeWorkerToken ends the token made for a worker that was not added, so
// that a pool which fails to add workers does not leave a working credential
// behind for each one it tried. It may be the pool's context that ended the
// attempt, so the token is revoked on one that outlives it.
func (wpc *LocalKubernetesWorkerPoolController) revokeWorkerToken(workerId string, token types.Token) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(wpc.ctx), workerTokenRevokeTimeout)
	defer cancel()

	if _, err := wpc.backendRepo.RevokeTokenByExternalId(ctx, token.ExternalId); err != nil {
		log.Error().Err(err).Str("pool_name", wpc.name).Str("worker_id", workerId).Msg("unable to revoke the token of a worker that was not added")
	}
}

func (wpc *LocalKubernetesWorkerPoolController) createWorkerJob(workerId string, cpu int64, memory int64, gpuType string, gpuCount uint32, token string) (*batchv1.Job, *types.Worker) {
	jobName := fmt.Sprintf("%s-%s-%s", Beta9WorkerJobPrefix, wpc.name, workerId)
	workerConfig := wpc.workerPodConfig()
	prometheusScrapeEnabled := workerConfig.Monitoring.MetricsCollector == string(types.MetricsCollectorPrometheus) &&
		workerConfig.Monitoring.Prometheus.ScrapeWorkers
	labels := map[string]string{
		"app":                       Beta9WorkerLabelValue,
		Beta9WorkerLabelKey:         Beta9WorkerLabelValue,
		Beta9WorkerLabelPoolNameKey: wpc.name,
		Beta9WorkerLabelIDKey:       workerId,
		PrometheusScrapeKey:         strconv.FormatBool(prometheusScrapeEnabled),
	}
	if prometheusScrapeEnabled {
		labels[PrometheusPortKey] = fmt.Sprintf("%d", workerConfig.Monitoring.Prometheus.Port)
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
		podResources := applyJobResourceOverhead(resourceRequests, wpc.config.Worker.JobResourceOverhead)
		resources.Requests = podResources
		resources.Limits = podResources
	}

	containers := []corev1.Container{
		{
			Name:      defaultContainerName,
			Image:     workerImage,
			Command:   workerPodCommand(),
			Resources: resources,
			SecurityContext: &corev1.SecurityContext{
				Privileged: ptr.To(true),
			},
			Env:          wpc.getWorkerEnvironment(workerId, workerCpu, workerMemory, workerGpuType, workerGpuCount, token, workerConfig),
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
			ServiceAccountName:            wpc.config.Worker.ServiceAccountName,
			AutomountServiceAccountToken:  ptr.To(false),
			HostNetwork:                   wpc.config.Worker.HostNetwork,
			ImagePullSecrets:              imagePullSecrets,
			RestartPolicy:                 corev1.RestartPolicyOnFailure,
			NodeSelector:                  wpc.workerPoolConfig.JobSpec.NodeSelector,
			Containers:                    containers,
			Volumes:                       wpc.getWorkerVolumes(workerMemory),
			EnableServiceLinks:            ptr.To(false),
			TerminationGracePeriodSeconds: ptr.To(workerPodTerminationGracePeriod(wpc.config.Worker.TerminationGracePeriod)),
		},
	}

	if wpc.config.Worker.HostNetwork {
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
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

	worker := pendingWorker(workerId, wpc.workerPoolConfig.CPUShareMultiplier.Schedulable(workerCpu), workerMemory, workerGpuType, workerGpuCount)
	worker.Priority = wpc.workerPoolConfig.Priority
	worker.Runtime = wpc.workerPoolConfig.ContainerRuntime
	worker.BuildVersion = wpc.config.Worker.ImageTag
	worker.Preemptable = wpc.workerPoolConfig.Preemptable
	return job, worker
}

func (wpc *LocalKubernetesWorkerPoolController) workerPodConfig() types.AppConfig {
	source := &wpc.config
	storage := source.Storage.WorkspaceStorage
	monitoring := source.Monitoring
	image := source.ImageService
	imageArchive := image.Registries.S3
	if wpc.workerPoolConfig.Mode != types.PoolModePrivate {
		// Shared workers use their workload identity for direct archive I/O.
		imageArchive.AccessKey = ""
		imageArchive.SecretKey = ""
	}
	config := types.AppConfig{
		ClusterName: source.ClusterName,
		DebugMode:   source.DebugMode,
		PrettyLogs:  source.PrettyLogs,
		Database:    types.DatabaseConfig{S2: source.Database.S2},
		Events:      source.Events,
		GatewayService: types.GatewayServiceConfig{
			Host: source.GatewayService.Host,
			GRPC: source.GatewayService.GRPC,
			HTTP: source.GatewayService.HTTP,
		},
		ImageService: types.ImageServiceConfig{
			LocalCacheEnabled:     image.LocalCacheEnabled,
			RegistryStore:         image.RegistryStore,
			Registries:            types.ImageRegistriesConfig{S3: imageArchive},
			Runner:                types.RunnerConfig{BaseImageRegistry: image.Runner.BaseImageRegistry},
			EnableTLS:             image.EnableTLS,
			ClipVersion:           image.ClipVersion,
			BuildRegistry:         image.BuildRegistry,
			BuildRepositoryName:   image.BuildRepositoryName,
			BuildRegistryInsecure: image.BuildRegistryInsecure,
			LayeredBuilds:         image.LayeredBuilds,
			BuildApt:              image.BuildApt,
			BuildLayerCacheMaxPct: image.BuildLayerCacheMaxPct,
		},
		Storage: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
			BaseMountPath:      storage.BaseMountPath,
			DefaultStorageMode: storage.DefaultStorageMode,
			Geese:              storage.Geese,
		}},
		Worker: source.Worker,
		Monitoring: types.MonitoringConfig{
			MetricsCollector:         monitoring.MetricsCollector,
			ContainerMetricsInterval: monitoring.ContainerMetricsInterval,
			Prometheus: types.PrometheusConfig{
				Port:          monitoring.Prometheus.Port,
				ScrapeWorkers: monitoring.Prometheus.ScrapeWorkers,
			},
			VictoriaMetrics: types.VictoriaMetricsConfig{
				PushURL:   monitoring.VictoriaMetrics.PushURL,
				AuthToken: monitoring.VictoriaMetrics.AuthToken,
				PushSecs:  monitoring.VictoriaMetrics.PushSecs,
			},
			OpenMeter:               monitoring.OpenMeter,
			ContainerCostHookConfig: monitoring.ContainerCostHookConfig,
		},
		ManagedCompute: types.ManagedComputeConfig{Billing: source.ManagedCompute.Billing},
		Abstractions:   types.AbstractionConfig{Pod: source.Abstractions.Pod},
		Cache:          source.Cache,
	}
	if config.Worker.UseGatewayServiceHostname {
		config.GatewayService.GRPC.ExternalHost = config.GatewayService.Host
		config.GatewayService.GRPC.ExternalPort = config.GatewayService.GRPC.Port
		config.GatewayService.GRPC.TLS = false
		config.GatewayService.HTTP.ExternalHost = config.GatewayService.Host
		config.GatewayService.HTTP.ExternalPort = config.GatewayService.HTTP.Port
		config.GatewayService.HTTP.TLS = false
	}
	if config.Worker.HostNetwork && config.Monitoring.MetricsCollector == string(types.MetricsCollectorPrometheus) {
		config.Monitoring.MetricsCollector = string(types.MetricsCollectorNone)
		config.Monitoring.Prometheus.ScrapeWorkers = false
	}

	// Workers use gateway-provided cache metadata and per-request registry and
	// workspace mount credentials.
	config.Cache.Server.Metadata = cache.MetadataConfig{}
	config.Cache.Coordinator.Token = ""
	config.Storage.WorkspaceStorage.Geese.AccessKey = ""
	config.Storage.WorkspaceStorage.Geese.SecretKey = ""

	// JobSpec is applied when creating each Job. The worker only needs the
	// pool's runtime settings, and never needs another pool's Job environment.
	config.Worker.Pools = maps.Clone(config.Worker.Pools)
	for name, pool := range config.Worker.Pools {
		pool.JobSpec = types.WorkerPoolJobSpecConfig{}
		config.Worker.Pools[name] = pool
	}

	// Workers report sandbox usage to the billing pool routes, so each route
	// keeps the token it resolves to. Only the gateway uses the base token.
	billing := &config.ManagedCompute.Billing
	billing.PoolRoutes = slices.Clone(billing.PoolRoutes)
	for i := range billing.PoolRoutes {
		billing.PoolRoutes[i].AuthToken = billing.PoolRoutes[i].BillingConfig(*billing).AuthToken
	}
	billing.AuthToken = ""
	return config
}

// createJobInCluster asks the cluster for the Job. The wait for its answer
// ends with the pool, or after workerJobCreateTimeout: workers are added one
// at a time by the pool's sizer, which a cluster that does not answer would
// otherwise hold for good.
func (wpc *LocalKubernetesWorkerPoolController) createJobInCluster(job *batchv1.Job) error {
	ctx, cancel := context.WithTimeout(wpc.ctx, workerJobCreateTimeout)
	defer cancel()

	_, err := wpc.kubeClient.BatchV1().Jobs(wpc.config.Worker.Namespace).Create(ctx, job, metav1.CreateOptions{})
	return err
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerVolumes(workerMemory int64) []corev1.Volume {
	hostPathType := corev1.HostPathDirectoryOrCreate
	sharedMemoryLimit := calculateMemoryQuantity(wpc.workerPoolConfig.PoolSizing.SharedMemoryLimitPct, workerMemory)
	tmpSizeLimit := parseTmpSizeLimit(wpc.workerPoolConfig.TmpSizeLimit, wpc.config.Worker.TmpSizeLimit)

	volumes := []corev1.Volume{
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

	if len(wpc.workerPoolConfig.JobSpec.Volumes) > 0 {
		for _, volume := range wpc.workerPoolConfig.JobSpec.Volumes {
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
			Path: workerImagesHostPath(wpc.workerPoolConfig),
			Type: &hostPathType,
		}
	}

	if workerCacheEnabled(wpc.config, wpc.workerPoolConfig) {
		volumes = append(volumes, corev1.Volume{
			Name: cacheVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: workerCacheHostPath(wpc.config, wpc.workerPoolConfig),
					Type: &hostPathType,
				},
			},
		})
	}

	volumes = append(volumes, corev1.Volume{
		Name: durableDiskVolumeName,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: workerDurableDisksHostPath(wpc.workerPoolConfig),
				Type: &hostPathType,
			},
		},
	})

	// Host kernel modules let the worker modprobe nbd for qcow durable disks.
	// DirectoryOrCreate keeps scheduling working on hosts without the path;
	// the qcow attach then fails with a clear module error instead.
	volumes = append(volumes, corev1.Volume{
		Name: kernelModulesVolumeName,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: types.HostKernelModulesPath,
				Type: &hostPathType,
			},
		},
	})

	hostPathDir := corev1.HostPathDirectory
	volumes = append(volumes, corev1.Volume{
		Name: devicePluginVolumeName,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: defaultDevicePluginPath,
				Type: &hostPathDir,
			},
		},
	})

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
			MountPath: "/dev/shm",
			Name:      "dshm",
		},
	}

	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      devicePluginVolumeName,
		MountPath: defaultDevicePluginPath,
		ReadOnly:  true,
	})

	if len(wpc.workerPoolConfig.JobSpec.VolumeMounts) > 0 {
		volumeMounts = append(volumeMounts, wpc.workerPoolConfig.JobSpec.VolumeMounts...)
	}

	if workerCacheEnabled(wpc.config, wpc.workerPoolConfig) {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      cacheVolumeName,
			MountPath: workerCacheMountPath(wpc.config, wpc.workerPoolConfig),
			ReadOnly:  false,
		})
	}

	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      durableDiskVolumeName,
		MountPath: types.DefaultDurableDisksPath,
		ReadOnly:  false,
	})

	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      kernelModulesVolumeName,
		MountPath: types.HostKernelModulesPath,
		ReadOnly:  true,
	})

	return volumeMounts
}

func (wpc *LocalKubernetesWorkerPoolController) getWorkerEnvironment(workerId string, cpu int64, memory int64, gpuType string, gpuCount uint32, token string, workerConfig types.AppConfig) []corev1.EnvVar {
	locality := wpc.workerPoolConfig.ConfigGroup
	if locality == "" {
		locality = wpc.config.Cache.Global.DefaultLocality
	}

	gateway := workerConfig.GatewayService
	grpcPort, httpPort := 443, 443
	if workerConfig.Worker.UseGatewayServiceHostname {
		grpcPort, httpPort = gateway.GRPC.ExternalPort, gateway.HTTP.ExternalPort
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
			Name: types.WorkerMachineEnv,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		{
			Name:  "CACHE_LOCALITY",
			Value: locality,
		},
		{
			Name:  types.WorkerTokenEnv,
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
			Name: "POD_UID",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.uid",
				},
			},
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
			Name: "NETWORK_NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		{
			Name:  "NETWORK_PREFIX",
			Value: common.WorkerNetworkPrefix(wpc.config.ClusterName, "$(NETWORK_NODE_NAME)"),
		},
		{
			Name: "CACHE_NODE_ID",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		{
			Name:  "CACHE_HOST_NETWORK",
			Value: strconv.FormatBool(wpc.config.Worker.HostNetwork),
		},
		{Name: types.ContainerGatewayGRPCHostEnv, Value: gateway.GRPC.ExternalHost},
		{Name: types.ContainerGatewayGRPCPortEnv, Value: strconv.Itoa(grpcPort)},
		{Name: types.ContainerGatewayHTTPHostEnv, Value: gateway.HTTP.ExternalHost},
		{Name: types.ContainerGatewayHTTPPortEnv, Value: strconv.Itoa(httpPort)},
	}

	envVars = append(envVars, wpc.workerPoolConfig.JobSpec.Env...)

	configJSON, err := json.MarshalIndent(workerConfig, "", "  ")
	if err == nil {
		envVars = append(envVars, corev1.EnvVar{
			Name:  types.WorkerConfigJSONEnv,
			Value: string(configJSON),
		})
	}

	return envVars
}

// monitorAndCleanupWorkers binds the tokens of workers that are already
// running, and removes finished, stale and stuck worker Jobs on an interval.
func (wpc *LocalKubernetesWorkerPoolController) monitorAndCleanupWorkers() {
	cleaner := &WorkerResourceCleaner{
		PoolName:    wpc.name,
		Config:      wpc.config.Worker,
		KubeClient:  wpc.kubeClient,
		EventRepo:   wpc.eventRepo,
		WorkerRepo:  wpc.workerRepo,
		BackendRepo: wpc.backendRepo,
	}
	// Binding lists every Job in the pool, so it stays off the caller's path.
	go cleaner.BindWorkerTokens(wpc.ctx)

	wpc.clock.Every(wpc.ctx, wpc.config.Worker.CleanupWorkerInterval, func() { wpc.cleanupTick(cleaner) })
}

// cleanupTick runs one cleanup pass. Only one scheduler replica cleans a pool
// at a time.
func (wpc *LocalKubernetesWorkerPoolController) cleanupTick(cleaner *WorkerResourceCleaner) {
	err := wpc.workerPoolRepo.WithWorkerPoolCleanerLock(wpc.name, func() { cleaner.Clean(wpc.ctx) })
	if err != nil && !errors.Is(err, redislock.ErrNotObtained) {
		log.Error().Str("pool_name", wpc.name).Err(err).Msg("failed to hold worker pool cleaner lock")
	}
}

// applyJobResourceOverhead returns a copy of the container-schedulable
// resources with the configured worker-process overhead added to CPU and
// memory. Invalid quantities are logged and ignored so a config typo can't
// stop workers from being provisioned.
func applyJobResourceOverhead(schedulable corev1.ResourceList, overhead types.JobResourceOverheadConfig) corev1.ResourceList {
	out := schedulable.DeepCopy()
	add := func(name corev1.ResourceName, raw string) {
		if strings.TrimSpace(raw) == "" {
			return
		}
		q, err := resource.ParseQuantity(raw)
		if err == nil {
			err = validateJobResourceOverheadQuantity(name, q)
		}
		if err != nil {
			log.Warn().Str("resource", string(name)).Str("value", raw).Err(err).Msg("ignoring invalid worker jobResourceOverhead")
			return
		}
		if q.Sign() <= 0 {
			return
		}
		base := out[name]
		base.Add(q)
		out[name] = base
	}
	add(corev1.ResourceCPU, overhead.CPU)
	add(corev1.ResourceMemory, overhead.Memory)
	return out
}

// validateJobResourceOverheadQuantity rejects quantities that parse but belong
// to the other resource family: a CPU overhead in bytes ("512Mi") would make
// every worker pod unschedulable, and a memory overhead in fractional bytes
// ("500m") is refused by the API server ("must be an integer").
func validateJobResourceOverheadQuantity(name corev1.ResourceName, q resource.Quantity) error {
	switch name {
	case corev1.ResourceCPU:
		if q.Format == resource.BinarySI {
			return fmt.Errorf("cpu overhead %s uses a memory unit", q.String())
		}
	case corev1.ResourceMemory:
		if q.MilliValue()%1000 != 0 {
			return fmt.Errorf("memory overhead %s is not a whole number of bytes", q.String())
		}
	}
	return nil
}
