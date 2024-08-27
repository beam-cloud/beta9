package worker

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"

	common "github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
)

const (
	requestProcessingInterval     time.Duration = 100 * time.Millisecond
	containerStatusUpdateInterval time.Duration = 30 * time.Second
)

type Worker struct {
	cpuLimit                int64
	memoryLimit             int64
	gpuType                 string
	gpuCount                uint32
	podAddr                 string
	podHostName             string
	imageMountPath          string
	runcHandle              runc.Runc
	runcServer              *RunCServer
	fileCacheManager        *FileCacheManager
	containerNetworkManager *ContainerNetworkManager
	containerCudaManager    GPUManager
	containerMountManager   *ContainerMountManager
	redisClient             *common.RedisClient
	imageClient             *ImageClient
	workerId                string
	eventBus                *common.EventBus
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerLock           sync.Mutex
	containerWg             sync.WaitGroup
	containerRepo           repo.ContainerRepository
	containerLogger         *ContainerLogger
	workerMetrics           *WorkerMetrics
	completedRequests       chan *types.ContainerRequest
	stopContainerChan       chan stopContainerEvent
	workerRepo              repo.WorkerRepository
	eventRepo               repo.EventRepository
	storage                 storage.Storage
	ctx                     context.Context
	cancel                  func()
	config                  types.AppConfig
}

type ContainerInstance struct {
	Id           string
	StubId       string
	BundlePath   string
	Overlay      *common.ContainerOverlay
	Spec         *specs.Spec
	Err          error
	ExitCode     int
	Port         int
	OutputWriter *common.OutputWriter
	LogBuffer    *common.LogBuffer
}

type ContainerOptions struct {
	BundlePath  string
	BindPort    int
	InitialSpec *specs.Spec
}

type stopContainerEvent struct {
	ContainerId string
	Kill        bool
}

var (
	//go:embed base_runc_config.json
	baseRuncConfigRaw          string
	baseConfigPath             string  = "/tmp"
	containerLogsPath          string  = "/var/log/worker"
	defaultContainerDirectory  string  = "/mnt/code"
	defaultWorkerSpindownTimeS float64 = 300 // 5 minutes
)

func NewWorker() (*Worker, error) {
	containerInstances := common.NewSafeMap[*ContainerInstance]()

	gpuType := os.Getenv("GPU_TYPE")
	workerId := os.Getenv("WORKER_ID")
	podHostName := os.Getenv("HOSTNAME")

	podAddr, err := GetPodAddr()
	if err != nil {
		return nil, err
	}

	gpuCount, err := strconv.ParseInt(os.Getenv("GPU_COUNT"), 10, 64)
	if err != nil {
		return nil, err
	}

	cpuLimit, err := strconv.ParseInt(os.Getenv("CPU_LIMIT"), 10, 64)
	if err != nil {
		return nil, err
	}

	memoryLimit, err := strconv.ParseInt(os.Getenv("MEMORY_LIMIT"), 10, 64)
	if err != nil {
		return nil, err
	}

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	redisClient, err := common.NewRedisClient(config.Database.Redis, common.WithClientName("Beta9Worker"))
	if err != nil {
		return nil, err
	}

	containerRepo := repo.NewContainerRedisRepository(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient, config.Worker)
	eventRepo := repo.NewTCPEventClientRepo(config.Monitoring.FluentBit.Events)

	storage, err := storage.NewStorage(config.Storage)
	if err != nil {
		return nil, err
	}

	var cacheClient *blobcache.BlobCacheClient = nil
	if config.Worker.BlobCacheEnabled {
		cacheClient, err = blobcache.NewBlobCacheClient(context.TODO(), config.BlobCache)
		if err != nil {
			log.Printf("[WARNING] Cache unavailable, performance may be degraded: %+v\n", err)
		}
	}

	fileCacheManager := NewFileCacheManager(config, cacheClient)
	imageClient, err := NewImageClient(config, workerId, workerRepo, fileCacheManager)
	if err != nil {
		return nil, err
	}

	runcServer, err := NewRunCServer(containerInstances, imageClient)
	if err != nil {
		return nil, err
	}

	err = runcServer.Start()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	containerNetworkManager, err := NewContainerNetworkManager(ctx, workerId, workerRepo, containerRepo, config)
	if err != nil {
		cancel()
		return nil, err
	}

	workerMetrics, err := NewWorkerMetrics(ctx, workerId, workerRepo, config.Monitoring)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Worker{
		ctx:                     ctx,
		cancel:                  cancel,
		config:                  config,
		imageMountPath:          getImageMountPath(workerId),
		cpuLimit:                cpuLimit,
		memoryLimit:             memoryLimit,
		gpuType:                 gpuType,
		gpuCount:                uint32(gpuCount),
		runcHandle:              runc.Runc{},
		runcServer:              runcServer,
		fileCacheManager:        fileCacheManager,
		containerCudaManager:    NewContainerNvidiaManager(uint32(gpuCount)),
		containerNetworkManager: containerNetworkManager,
		redisClient:             redisClient,
		containerMountManager:   NewContainerMountManager(),
		podAddr:                 podAddr,
		imageClient:             imageClient,
		podHostName:             podHostName,
		eventBus:                nil,
		workerId:                workerId,
		containerInstances:      containerInstances,
		containerLock:           sync.Mutex{},
		containerWg:             sync.WaitGroup{},
		containerRepo:           containerRepo,
		containerLogger: &ContainerLogger{
			containerInstances: containerInstances,
		},
		workerMetrics:     workerMetrics,
		workerRepo:        workerRepo,
		eventRepo:         eventRepo,
		completedRequests: make(chan *types.ContainerRequest, 1000),
		stopContainerChan: make(chan stopContainerEvent, 1000),
		storage:           storage,
	}, nil
}

func (s *Worker) Run() error {
	err := s.startup()
	if err != nil {
		return err
	}

	go s.manageWorkerCapacity()
	go s.processStopContainerEvents()
	defer func() {
		close(s.completedRequests)
		close(s.stopContainerChan)
	}()

	lastContainerRequest := time.Now()
	for {
		request, err := s.workerRepo.GetNextContainerRequest(s.workerId)
		if request != nil && err == nil {
			lastContainerRequest = time.Now()
			containerId := request.ContainerId

			s.containerLock.Lock()

			_, exists := s.containerInstances.Get(containerId)
			if !exists {
				log.Printf("<%s> - running container.\n", containerId)

				err := s.RunContainer(request)
				if err != nil {
					log.Printf("Unable to run container <%s>: %v\n", containerId, err)

					// Set a non-zero exit code for the container (both in memory, and in repo)
					exitCode := 1
					err := s.containerRepo.SetContainerExitCode(containerId, exitCode)
					if err != nil {
						log.Printf("<%s> - failed to set exit code: %v\n", containerId, err)
					}

					s.containerLock.Unlock()
					s.clearContainer(containerId, request, time.Duration(0), exitCode)
					continue
				}
			}

			s.containerLock.Unlock()
		}

		if exit := s.shouldShutDown(lastContainerRequest); exit {
			break
		}

		time.Sleep(requestProcessingInterval)
	}

	return s.shutdown()
}

// Only exit if there are no containers running, and no containers have recently been spun up on this worker
func (s *Worker) shouldShutDown(lastContainerRequest time.Time) bool {
	if (time.Since(lastContainerRequest).Seconds() > defaultWorkerSpindownTimeS) && s.containerInstances.Len() == 0 {
		return true
	}
	return false
}

// Spawn a single container and stream output to stdout/stderr
func (s *Worker) RunContainer(request *types.ContainerRequest) error {
	containerId := request.ContainerId
	bundlePath := filepath.Join(s.imageMountPath, request.ImageId)

	// Pull image
	log.Printf("<%s> - lazy-pulling image: %s\n", containerId, request.ImageId)
	err := s.imageClient.PullLazy(request)
	if err != nil && request.SourceImage != nil {
		log.Printf("<%s> - lazy-pull failed, pulling source image: %s\n", containerId, *request.SourceImage)
		err = s.imageClient.PullAndArchiveImage(context.TODO(), *request.SourceImage, request.ImageId, request.SourceImageCreds)
		if err == nil {
			err = s.imageClient.PullLazy(request)
		}
	}
	if err != nil {
		return err
	}

	bindPort, err := getRandomFreePort()
	if err != nil {
		return err
	}
	log.Printf("<%s> - acquired port: %d\n", containerId, bindPort)

	// Read spec from bundle
	initialBundleSpec, _ := s.readBundleConfig(request.ImageId)

	opts := &ContainerOptions{
		BundlePath:  bundlePath,
		BindPort:    bindPort,
		InitialSpec: initialBundleSpec,
	}

	err = s.containerMountManager.SetupContainerMounts(request)
	if err != nil {
		s.containerLogger.Log(request.ContainerId, request.StubId, fmt.Sprintf("failed to setup container mounts: %v", err))
	}

	// Generate dynamic runc spec for this container
	spec, err := s.specFromRequest(request, opts)
	if err != nil {
		return err
	}
	log.Printf("<%s> - successfully created spec from request.\n", containerId)

	// Set an address (ip:port) for the pod/container in Redis. Depending on the stub type,
	// gateway may need to directly interact with this pod/container.
	containerAddr := fmt.Sprintf("%s:%d", s.podAddr, bindPort)
	err = s.containerRepo.SetContainerAddress(request.ContainerId, containerAddr)
	if err != nil {
		return err
	}
	log.Printf("<%s> - set container address.\n", containerId)

	outputChan := make(chan common.OutputMsg)
	go s.containerWg.Add(1)

	// Start the container
	go s.spawn(request, spec, outputChan, opts)

	log.Printf("<%s> - spawned successfully.\n", containerId)
	return nil
}

func (s *Worker) updateContainerStatus(request *types.ContainerRequest) error {
	for {
		time.Sleep(containerStatusUpdateInterval)

		s.containerLock.Lock()
		_, exists := s.containerInstances.Get(request.ContainerId)
		s.containerLock.Unlock()

		if !exists {
			return nil
		}

		log.Printf("<%s> - container still running: %s\n", request.ContainerId, request.ImageId)

		// Stop container if it is "orphaned" - meaning it's running but has no associated state
		state, err := s.containerRepo.GetContainerState(request.ContainerId)
		if err != nil {
			if _, ok := err.(*types.ErrContainerStateNotFound); ok {
				log.Printf("<%s> - container state not found, stopping container\n", request.ContainerId)
				s.stopContainerChan <- stopContainerEvent{ContainerId: request.ContainerId, Kill: true}
				return nil
			}

			continue
		}

		err = s.containerRepo.UpdateContainerStatus(request.ContainerId, state.Status, time.Duration(types.ContainerStateTtlS)*time.Second)
		if err != nil {
			log.Printf("<%s> - unable to update container state: %v\n", request.ContainerId, err)
		}

		// If container is supposed to be stopped, but isn't gone after TerminationGracePeriod seconds
		// ensure it is killed after that
		if state.Status == types.ContainerStatusStopping {
			go func() {
				time.Sleep(time.Duration(s.config.Worker.TerminationGracePeriod) * time.Second)

				_, exists := s.containerInstances.Get(request.ContainerId)
				if !exists {
					return
				}

				log.Printf("<%s> - container still running after stop event %ds ago - force killing\n", request.ContainerId, s.config.Worker.TerminationGracePeriod)
				s.stopContainerChan <- stopContainerEvent{
					ContainerId: request.ContainerId,
					Kill:        true,
				}
			}()
		}
	}
}

// stopContainer stops a running container by containerId, if it exists on this worker
func (s *Worker) stopContainer(event *common.Event) bool {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	containerId := event.Args["container_id"].(string)

	var err error = nil
	if _, exists := s.containerInstances.Get(containerId); exists {
		log.Printf("<%s> - received stop container event.\n", containerId)
		s.stopContainerChan <- stopContainerEvent{ContainerId: containerId, Kill: false}
	}

	return err == nil
}

func (s *Worker) processStopContainerEvents() {
	for event := range s.stopContainerChan {
		log.Printf("<%s> - stopping container.\n", event.ContainerId)

		instance, exists := s.containerInstances.Get(event.ContainerId)
		if !exists {
			continue
		}

		// Container has already exited, just skip this event
		if instance.ExitCode >= 0 {
			continue
		}

		signal := int(syscall.SIGTERM)
		if event.Kill {
			signal = int(syscall.SIGKILL)
		}

		err := s.runcHandle.Kill(s.ctx, event.ContainerId, signal, &runc.KillOpts{
			All: true,
		})
		if err != nil {
			log.Printf("<%s> - unable to stop container: %v\n", event.ContainerId, err)

			if strings.Contains(err.Error(), "container does not exist") {
				// In case container network is still around for some reason, get rid of it
				s.containerNetworkManager.TearDown(event.ContainerId)
				continue
			}

			s.stopContainerChan <- event
			time.Sleep(time.Second)
			continue
		}

		log.Printf("<%s> - container stopped.\n", event.ContainerId)
	}
}

const ExitCodeSigterm = 143

func (s *Worker) terminateContainer(containerId string, request *types.ContainerRequest, exitCode *int, containerErr *error) {
	defer s.containerWg.Done()

	if *exitCode < 0 {
		*exitCode = 1
	} else if *exitCode == ExitCodeSigterm {
		*exitCode = 0
	}

	err := s.containerRepo.SetContainerExitCode(containerId, *exitCode)
	if err != nil {
		log.Printf("<%s> - failed to set exit code: %v\n", containerId, err)
	}

	s.clearContainer(containerId, request, time.Duration(s.config.Worker.TerminationGracePeriod)*time.Second, *exitCode)
}

func (s *Worker) clearContainer(containerId string, request *types.ContainerRequest, delay time.Duration, exitCode int) {
	s.containerLock.Lock()

	// De-allocate GPU devices so they are available for new containers
	if request.Gpu != "" {
		s.containerCudaManager.UnassignGPUDevices(containerId)
	}

	// Tear down container network components
	err := s.containerNetworkManager.TearDown(request.ContainerId)
	if err != nil {
		log.Printf("<%s> - failed to clean up container network: %v\n", request.ContainerId, err)
	}

	s.completedRequests <- request
	s.containerLock.Unlock()

	// Set container exit code on instance
	instance, exists := s.containerInstances.Get(containerId)
	if exists {
		instance.ExitCode = exitCode
		s.containerInstances.Set(containerId, instance)
	}

	go func() {
		// Allow for some time to pass before clearing the container. This way we can handle some last
		// minute logs or events or if the user wants to inspect the container before it's cleared.
		time.Sleep(delay)

		s.containerInstances.Delete(containerId)
		err := s.containerRepo.DeleteContainerState(request)
		if err != nil {
			log.Printf("<%s> - failed to remove container state: %v\n", containerId, err)
		}

	}()
}

// isBuildRequest checks if the sourceImage field is not-nil, which means the container request is for a build container
func (s *Worker) isBuildRequest(request *types.ContainerRequest) bool {
	return request.SourceImage != nil
}

func (s *Worker) createOverlay(request *types.ContainerRequest, bundlePath string) *common.ContainerOverlay {
	// For images that have a rootfs, set that as the root path
	// otherwise, assume runc config files are in the rootfs themselves
	rootPath := filepath.Join(bundlePath, "rootfs")
	if _, err := os.Stat(rootPath); os.IsNotExist(err) {
		rootPath = bundlePath
	}

	overlayPath := baseConfigPath
	if s.isBuildRequest(request) {
		overlayPath = "/dev/shm"
	}

	return common.NewContainerOverlay(request.ContainerId, rootPath, overlayPath)
}

// spawn a container using runc binary
func (s *Worker) spawn(request *types.ContainerRequest, spec *specs.Spec, outputChan chan common.OutputMsg, opts *ContainerOptions) {
	ctx, cancel := context.WithCancel(s.ctx)

	s.workerRepo.AddContainerToWorker(s.workerId, request.ContainerId)
	defer s.workerRepo.RemoveContainerFromWorker(s.workerId, request.ContainerId)
	defer cancel()

	var containerErr error = nil

	exitCode := -1
	containerId := request.ContainerId

	// Unmount external s3 buckets
	defer s.containerMountManager.RemoveContainerMounts(containerId)

	// Clear out all files in the container's directory
	defer func() {
		s.terminateContainer(containerId, request, &exitCode, &containerErr)
	}()

	// Create overlayfs for container
	overlay := s.createOverlay(request, opts.BundlePath)

	containerInstance := &ContainerInstance{
		Id:         containerId,
		StubId:     request.StubId,
		BundlePath: opts.BundlePath,
		Overlay:    overlay,
		Spec:       spec,
		ExitCode:   -1,
		OutputWriter: common.NewOutputWriter(func(s string) {
			outputChan <- common.OutputMsg{
				Msg:     string(s),
				Done:    false,
				Success: false,
			}
		}),
		LogBuffer: common.NewLogBuffer(),
	}
	s.containerInstances.Set(containerId, containerInstance)

	// Every 30 seconds, update container status
	go s.updateContainerStatus(request)

	// Set worker hostname
	hostname := fmt.Sprintf("%s:%d", s.podAddr, s.runcServer.port)
	s.containerRepo.SetWorkerAddress(request.ContainerId, hostname)

	// Handle stdout/stderr from spawned container
	go s.containerLogger.CaptureLogs(request.ContainerId, outputChan)

	go func() {
		time.Sleep(time.Second)

		s.containerLock.Lock()
		defer s.containerLock.Unlock()

		_, exists := s.containerInstances.Get(containerId)
		if !exists {
			return
		}

		if _, err := s.containerRepo.GetContainerState(containerId); err != nil {
			if _, ok := err.(*types.ErrContainerStateNotFound); ok {
				log.Printf("<%s> - container state not found, returning\n", containerId)
				return
			}
		}

		// Update container status to running
		s.containerRepo.UpdateContainerStatus(containerId, types.ContainerStatusRunning, time.Duration(types.ContainerStateTtlS)*time.Second)
	}()

	// Setup container overlay filesystem
	err := containerInstance.Overlay.Setup()
	if err != nil {
		log.Printf("<%s> failed to setup overlay: %v", containerId, err)
		containerErr = err
		return
	}
	defer containerInstance.Overlay.Cleanup()
	spec.Root.Path = containerInstance.Overlay.TopLayerPath()

	// Setup container network namespace / devices
	err = s.containerNetworkManager.Setup(containerId, spec)
	if err != nil {
		log.Printf("<%s> failed to setup container network: %v", containerId, err)
		containerErr = err
		return
	}

	// Expose the bind port
	err = s.containerNetworkManager.ExposePort(containerId, opts.BindPort, opts.BindPort)
	if err != nil {
		log.Printf("<%s> failed to expose container bind port: %v", containerId, err)
		containerErr = err
		return
	}

	// Write runc config spec to disk
	configContents, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		containerErr = err
		return
	}

	configPath := filepath.Join(baseConfigPath, containerId, "config.json")
	err = os.WriteFile(configPath, configContents, 0644)
	if err != nil {
		containerErr = err
		return
	}

	// Log metrics
	go s.workerMetrics.EmitContainerUsage(ctx, request)
	go s.eventRepo.PushContainerStartedEvent(request.ContainerId, s.workerId, request)
	defer func() { go s.eventRepo.PushContainerStoppedEvent(request.ContainerId, s.workerId, request) }()

	// Capture resource usage (cpu/mem/gpu)
	pidChan := make(chan int, 1)
	go s.collectAndSendContainerMetrics(ctx, request, spec, pidChan)

	// Invoke runc process (launch the container)
	exitCode, err = s.runcHandle.Run(s.ctx, containerId, opts.BundlePath, &runc.CreateOpts{
		OutputWriter: containerInstance.OutputWriter,
		ConfigPath:   configPath,
		Started:      pidChan,
	})

	// Send last log message since the container has exited
	outputChan <- common.OutputMsg{
		Msg:     "",
		Done:    true,
		Success: err == nil,
	}

	if err != nil {
		containerErr = err
		return
	}

	if exitCode != 0 {
		containerErr = fmt.Errorf("unable to run container: %d", exitCode)
	}
}

func (s *Worker) getContainerEnvironment(request *types.ContainerRequest, options *ContainerOptions) []string {
	env := []string{
		fmt.Sprintf("BIND_PORT=%d", options.BindPort),
		fmt.Sprintf("CONTAINER_HOSTNAME=%s", fmt.Sprintf("%s:%d", s.podAddr, options.BindPort)),
		fmt.Sprintf("CONTAINER_ID=%s", request.ContainerId),
		fmt.Sprintf("BETA9_GATEWAY_HOST=%s", os.Getenv("BETA9_GATEWAY_HOST")),
		fmt.Sprintf("BETA9_GATEWAY_PORT=%s", os.Getenv("BETA9_GATEWAY_PORT")),
		"PYTHONUNBUFFERED=1",
	}

	env = append(env, request.Env...)
	return env
}

func (s *Worker) readBundleConfig(imageId string) (*specs.Spec, error) {
	imageConfigPath := filepath.Join(s.imageMountPath, imageId, "initial_config.json")

	data, err := os.ReadFile(imageConfigPath)
	if err != nil {
		return nil, err
	}

	specTemplate := strings.TrimSpace(string(data))
	var spec specs.Spec

	err = json.Unmarshal([]byte(specTemplate), &spec)
	if err != nil {
		return nil, err
	}

	return &spec, nil
}

func (s *Worker) newSpecTemplate() (*specs.Spec, error) {
	var newSpec specs.Spec
	specTemplate := strings.TrimSpace(string(baseRuncConfigRaw))
	err := json.Unmarshal([]byte(specTemplate), &newSpec)
	if err != nil {
		return nil, err
	}
	return &newSpec, nil
}

// Generate a runc spec from a given request
func (s *Worker) specFromRequest(request *types.ContainerRequest, options *ContainerOptions) (*specs.Spec, error) {
	os.MkdirAll(filepath.Join(baseConfigPath, request.ContainerId), os.ModePerm)
	configPath := filepath.Join(baseConfigPath, request.ContainerId, "config.json")

	spec, err := s.newSpecTemplate()
	if err != nil {
		return nil, err
	}

	spec.Process.Cwd = defaultContainerDirectory
	spec.Process.Args = request.EntryPoint

	env := s.getContainerEnvironment(request, options)
	if request.Gpu != "" {
		spec.Hooks.Prestart[0].Args = append(spec.Hooks.Prestart[0].Args, configPath, "prestart")

		existingCudaFound := false
		env, existingCudaFound = s.containerCudaManager.InjectEnvVars(env, options)
		if !existingCudaFound {
			// If the container image does not have cuda libraries installed, mount cuda libs from the host
			spec.Mounts = s.containerCudaManager.InjectMounts(spec.Mounts)
		}

		// Assign n-number of GPUs to a container
		assignedGpus, err := s.containerCudaManager.AssignGPUDevices(request.ContainerId, request.GpuCount)
		if err != nil {
			return nil, err
		}
		env = append(env, fmt.Sprintf("NVIDIA_VISIBLE_DEVICES=%s", assignedGpus.String()))

		spec.Linux.Resources.Devices = append(spec.Linux.Resources.Devices, assignedGpus.devices...)

	} else {
		spec.Hooks.Prestart = nil
	}

	spec.Process.Env = append(spec.Process.Env, env...)
	spec.Root.Readonly = false

	// Create local workspace path so we can symlink volumes before the container starts
	os.MkdirAll(defaultContainerDirectory, os.FileMode(0755))

	// Add bind mounts to runc spec
	for _, m := range request.Mounts {

		// Skip mountpoint storage if the local path does not exist (mounting failed)
		if m.MountType == storage.StorageModeMountPoint {
			if _, err := os.Stat(m.LocalPath); os.IsNotExist(err) {
				continue
			}
		} else {
			if _, err := os.Stat(m.LocalPath); os.IsNotExist(err) {
				err := os.MkdirAll(m.LocalPath, 0755)
				if err != nil {
					log.Printf("<%s> - failed to create mount directory: %v\n", request.ContainerId, err)
				}
			}
		}

		mode := "rw"
		if m.ReadOnly {
			mode = "ro"
		}

		if m.LinkPath != "" {
			err = forceSymlink(m.MountPath, m.LinkPath)
			if err != nil {
				log.Printf("unable to symlink volume: %v", err)
			}
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "none",
			Source:      m.LocalPath,
			Destination: m.MountPath,
			Options:     []string{"rbind", mode},
		})
	}

	// Configure resolv.conf
	resolvMount := specs.Mount{
		Type:        "none",
		Source:      "/workspace/resolv.conf",
		Destination: "/etc/resolv.conf",
		Options: []string{"ro",
			"rbind",
			"rprivate",
			"nosuid",
			"noexec",
			"nodev"},
	}

	if s.config.Worker.UseHostResolvConf {
		resolvMount.Source = "/etc/resolv.conf"
	}

	spec.Mounts = append(spec.Mounts, resolvMount)

	return spec, nil
}

func (s *Worker) manageWorkerCapacity() {
	for request := range s.completedRequests {
		err := s.processCompletedRequest(request)
		if err != nil {
			log.Printf("Unable to process completed request: %v\n", err)
			s.completedRequests <- request
			continue
		}
	}
}

func (s *Worker) processCompletedRequest(request *types.ContainerRequest) error {
	worker, err := s.workerRepo.GetWorkerById(s.workerId)
	if err != nil {
		return err
	}

	return s.workerRepo.UpdateWorkerCapacity(worker, request, types.AddCapacity)
}

func (s *Worker) keepalive() {
	ticker := time.NewTicker(types.WorkerKeepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.workerRepo.SetWorkerKeepAlive(s.workerId)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Worker) startup() error {
	log.Printf("Worker starting up.")

	err := s.workerRepo.ToggleWorkerAvailable(s.workerId)
	if err != nil {
		return err
	}

	eventBus := common.NewEventBus(
		s.redisClient,
		common.EventBusSubscriber{Type: common.EventTypeStopContainer, Callback: s.stopContainer},
	)

	s.eventBus = eventBus
	go s.eventBus.ReceiveEvents(s.ctx)
	go s.keepalive()

	err = os.MkdirAll(containerLogsPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	go s.eventRepo.PushWorkerStartedEvent(s.workerId)

	return nil
}

func (s *Worker) shutdown() error {
	log.Println("Shutting down...")
	defer s.eventRepo.PushWorkerStoppedEvent(s.workerId)

	var errs error

	if worker, err := s.workerRepo.GetWorkerById(s.workerId); err != nil {
		errs = errors.Join(errs, err)
	} else if worker != nil {
		err = s.workerRepo.RemoveWorker(worker)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}

	s.cancel()
	err := s.storage.Unmount(s.config.Storage.FilesystemPath)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to unmount storage: %v", err))
	}

	err = s.imageClient.Cleanup()
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("failed to cleanup fuse mounts: %v", err))
	}

	err = os.RemoveAll(s.imageMountPath)
	if err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}
