package worker

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	common "github.com/beam-cloud/beam/internal/common"
	repo "github.com/beam-cloud/beam/internal/repository"
	types "github.com/beam-cloud/beam/internal/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/slai-labs/go-runc"
)

const (
	RequestProcessingInterval     time.Duration = 100 * time.Millisecond
	ContainerStatusUpdateInterval time.Duration = 30 * time.Second
)

type Worker struct {
	cpuLimit             int64
	memoryLimit          int64
	gpuType              string
	podIPAddr            string
	podHostName          string
	userImagePath        string
	runcHandle           runc.Runc
	runcServer           *RunCServer
	containerCudaManager *ContainerCudaManager
	redisClient          *common.RedisClient
	imageClient          *ImageClient
	workerId             string
	eventBus             *common.EventBus
	containerInstances   *common.SafeMap[*ContainerInstance]
	containerLock        sync.Mutex
	containerWg          sync.WaitGroup
	containerRepo        repo.ContainerRepository
	containerLogger      *ContainerLogger
	workerMetrics        *WorkerMetrics
	completedRequests    chan *types.ContainerRequest
	stopContainerChan    chan string
	workerRepo           repo.WorkerRepository
	ctx                  context.Context
	cancel               func()
}

type ContainerInstance struct {
	Id           string
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
	BindPort    int
	InitialSpec *specs.Spec
}

var (
	//go:embed base_runc_config.json
	baseRuncConfigRaw          string
	imagePath                  string  = "/images"
	containerLogsPath          string  = "/var/log/worker"
	baseConfigPath             string  = "/tmp"
	defaultContainerDirectory  string  = "/workspace"
	defaultWorkerSpindownTimeS float64 = 300 // 5 minutes
)

func NewWorker() (*Worker, error) {
	containerInstances := common.NewSafeMap[*ContainerInstance]()

	gpuType := os.Getenv("GPU_TYPE")
	workerId := os.Getenv("WORKER_ID")
	podHostName := os.Getenv("HOSTNAME")

	podIPAddr, err := GetPodIP()
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

	redisClient, err := common.NewRedisClient(common.WithClientName("BeamWorker"))
	if err != nil {
		return nil, err
	}

	imageClient, err := NewImageClient()
	if err != nil {
		return nil, err
	}

	runcServer, err := NewRunCServer(containerInstances)
	if err != nil {
		return nil, err
	}

	err = runcServer.Start()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	containerRepo := repo.NewContainerRedisRepository(redisClient)
	workerRepo := repo.NewWorkerRedisRepository(redisClient)
	statsdRepo := repo.NewMetricsStatsdRepository()

	workerMetrics := NewWorkerMetrics(ctx, podHostName, statsdRepo, workerRepo, repo.NewMetricsStreamRepository(ctx))
	workerMetrics.InitNvml()

	return &Worker{
		ctx:                  ctx,
		cancel:               cancel,
		userImagePath:        imagePath,
		cpuLimit:             cpuLimit,
		memoryLimit:          memoryLimit,
		gpuType:              gpuType,
		runcHandle:           runc.Runc{},
		runcServer:           runcServer,
		containerCudaManager: NewContainerCudaManager(),
		redisClient:          redisClient,
		podIPAddr:            podIPAddr,
		imageClient:          imageClient,
		podHostName:          podHostName,
		eventBus:             nil,
		workerId:             workerId,
		containerInstances:   containerInstances,
		containerLock:        sync.Mutex{},
		containerWg:          sync.WaitGroup{},
		containerRepo:        containerRepo,
		containerLogger: &ContainerLogger{
			containerInstances: containerInstances,
		},
		workerMetrics:     workerMetrics,
		workerRepo:        workerRepo,
		completedRequests: make(chan *types.ContainerRequest, 1000),
		stopContainerChan: make(chan string, 1000),
	}, nil
}

func (s *Worker) Run() error {
	err := s.startup()
	if err != nil {
		return err
	}

	go s.manageWorkerCapacity()
	go s.processStopContainerEvents()

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

					// HOTFIX: set a non-zero exit code for deployments that failed to launch due to some
					// unhandled issue. This is just here to prevent infinite crashlooping
					// TODO: handle these sorts of failures in a more graceful way
					err := s.containerRepo.SetContainerExitCode(containerId, 1)
					if err != nil {
						log.Printf("<%s> - failed to set exit code: %v\n", containerId, err)
					}

					s.containerLock.Unlock()
					s.clearContainer(containerId, request)
					continue
				}
			}

			s.containerLock.Unlock()
		}

		if exit := s.shouldShutDown(lastContainerRequest); exit {
			break
		}

		time.Sleep(RequestProcessingInterval)
	}

	log.Println("Shutting down...")
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
	containerID := request.ContainerId
	bundlePath := filepath.Join(s.userImagePath, request.ImageId)

	// TODO: move this down
	containerServerAddr := fmt.Sprintf("%s:%d", s.podIPAddr, defaultContainerServerPort)
	err := s.containerRepo.SetContainerServer(request.ContainerId, containerServerAddr)
	if err != nil {
		return err
	}

	log.Printf("<%s> - lazy-pulling image: %s\n", containerID, request.ImageId)
	err = s.imageClient.PullLazy(request.ImageId)
	if err != nil && request.SourceImage != nil {
		log.Printf("<%s> - lazy-pull failed, pulling source image: %s\n", containerID, *request.SourceImage)
		err = s.imageClient.PullAndArchiveImage(context.TODO(), *request.SourceImage, request.ImageId, nil)
		if err == nil {
			err = s.imageClient.PullLazy(request.ImageId)
		}
	}

	if err != nil {
		return err
	}

	// Every 30 seconds, update container status
	go s.updateContainerStatus(request)

	bindPort, err := GetRandomFreePort()
	if err != nil {
		return err
	}
	log.Printf("<%s> - acquired port: %d\n", containerID, bindPort)

	// Read spec from bundle
	initialBundleSpec, _ := s.readBundleConfig(request.ImageId)

	// Generate dynamic runc spec for this container
	spec, err := s.specFromRequest(request, &ContainerOptions{
		BindPort:    bindPort,
		InitialSpec: initialBundleSpec,
	})
	if err != nil {
		return err
	}

	log.Printf("<%s> - successfully created spec from request.\n", containerID)

	// Set an address (ip:port) for the pod/container in Redis. Depending on the trigger type,
	// Gateway will need to directly interact with this pod/container.
	containerAddr := fmt.Sprintf("%s:%d", s.podIPAddr, bindPort)
	err = s.containerRepo.SetContainerAddress(request.ContainerId, containerAddr)
	if err != nil {
		return err
	}
	log.Printf("<%s> - set container address.\n", containerID)

	// Start the container
	err = s.SpawnAsync(request, bundlePath, spec)
	if err != nil {
		return err
	}

	log.Printf("<%s> - spawned successfully.\n", containerID)
	return nil
}

func (s *Worker) updateContainerStatus(request *types.ContainerRequest) error {
	for {
		time.Sleep(ContainerStatusUpdateInterval)

		s.containerLock.Lock()
		_, exists := s.containerInstances.Get(request.ContainerId)
		s.containerLock.Unlock()

		if !exists {
			return nil
		}

		// Stop container if it is "orphaned" - meaning it's running but has no associated state
		if _, err := s.containerRepo.GetContainerState(request.ContainerId); err != nil {
			if _, ok := err.(*types.ErrContainerStateNotFound); ok {
				log.Printf("<%s> - container state not found, stopping container\n", request.ContainerId)
				s.stopContainerChan <- request.ContainerId
				return nil
			}
		}

		err := s.containerRepo.UpdateContainerStatus(request.ContainerId, types.ContainerStatusRunning, time.Duration(types.ContainerStateTtlS)*time.Second)
		if err != nil {
			log.Printf("<%s> - unable to update container state: %v\n", request.ContainerId, err)
		}

		log.Printf("<%s> - container still running: %s\n", request.ContainerId, request.ImageId)
	}
}

// Invoke a runc container using a predefined config spec
func (s *Worker) SpawnAsync(request *types.ContainerRequest, bundlePath string, spec *specs.Spec) error {
	outputChan := make(chan common.OutputMsg)

	go s.containerWg.Add(1)
	go s.spawn(request, bundlePath, spec, outputChan)

	return nil
}

// stopContainer stops a running container by containerId, if it exists on this worker
func (s *Worker) stopContainer(event *common.Event) bool {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	containerId := event.Args["container_id"].(string)

	var err error = nil
	if _, containerExists := s.containerInstances.Get(containerId); containerExists {
		log.Printf("<%s> - received stop container event.\n", containerId)
		s.stopContainerChan <- containerId
	}

	return err == nil
}

func (s *Worker) processStopContainerEvents() {
	for containerId := range s.stopContainerChan {
		log.Printf("<%s> - stopping container.\n", containerId)

		if _, containerExists := s.containerInstances.Get(containerId); !containerExists {
			continue
		}

		err := s.runcHandle.Kill(s.ctx, containerId, int(syscall.SIGTERM), &runc.KillOpts{
			All: true,
		})

		if err != nil {
			log.Printf("<%s> - unable to stop container: %v\n", containerId, err)

			s.stopContainerChan <- containerId
			time.Sleep(time.Second)
			continue
		}

		log.Printf("<%s> - container stopped.\n", containerId)
	}
}

func (s *Worker) terminateContainer(containerId string, request *types.ContainerRequest, exitCode *int, containerErr *error) {
	if *exitCode < 0 {
		*exitCode = 1
	}

	if *exitCode != 0 {
		err := s.containerRepo.SetContainerExitCode(containerId, *exitCode)
		if err != nil {
			log.Printf("<%s> - failed to set exit code: %v\n", containerId, err)
		}
	}

	defer s.containerWg.Done()
	s.clearContainer(containerId, request)
}

func (s *Worker) clearContainer(containerId string, request *types.ContainerRequest) {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	s.containerInstances.Delete(containerId)

	err := s.containerRepo.DeleteContainerState(request)
	if err != nil {
		log.Printf("<%s> - failed to remove container state: %v\n", containerId, err)
	}

	s.completedRequests <- request
}

// spawn a container using runc binary
func (s *Worker) spawn(request *types.ContainerRequest, bundlePath string, spec *specs.Spec, outputChan chan common.OutputMsg) {
	s.workerRepo.AddContainerRequestToWorker(s.workerId, request.ContainerId, request)
	defer s.workerRepo.RemoveContainerRequestFromWorker(s.workerId, request.ContainerId)

	var containerErr error = nil

	exitCode := -1
	containerId := request.ContainerId

	// Channel to signal when container has finished
	done := make(chan bool)
	defer func() {
		done <- true
		close(done)
	}()

	// Clear out all files in the container's directory
	defer func() {
		s.terminateContainer(containerId, request, &exitCode, &containerErr)
	}()

	// Add the container instance to the runningContainers map
	containerInstance := &ContainerInstance{
		Id:         containerId,
		BundlePath: bundlePath,
		Overlay:    common.NewContainerOverlay(containerId, bundlePath, baseConfigPath, filepath.Join(bundlePath, "rootfs")),
		Spec:       spec,
		ExitCode:   -1,
		OutputWriter: common.NewOutputWriter(func(s string) {
			outputChan <- common.OutputMsg{
				Msg:     strings.TrimSuffix(string(s), "\n"),
				Done:    false,
				Success: false,
			}
		}),
		LogBuffer: common.NewLogBuffer(),
	}
	s.containerInstances.Set(containerId, containerInstance)

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
	go s.workerMetrics.EmitContainerUsage(request, done)
	s.workerMetrics.ContainerStarted(containerId)
	defer s.workerMetrics.ContainerStopped(containerId)

	pidChan := make(chan int, 1)
	go s.workerMetrics.EmitResourceUsage(request, pidChan, done)

	// Invoke runc process (launch the container)
	exitCode, err = s.runcHandle.Run(s.ctx, containerId, bundlePath, &runc.CreateOpts{
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

func (s *Worker) getContainerEnvironment(request *types.ContainerRequest, containerConfig *ContainerConfigResponse, options *ContainerOptions) []string {
	env := []string{
		fmt.Sprintf("IDENTITY_ID=%s", containerConfig.IdentityId),
		fmt.Sprintf("S2S_TOKEN=%s", containerConfig.S2SToken),
		fmt.Sprintf("BIND_PORT=%d", options.BindPort),
		fmt.Sprintf("CONTAINER_HOSTNAME=%s", fmt.Sprintf("%s:%d", s.podIPAddr, options.BindPort)),
		fmt.Sprintf("CONTAINER_ID=%s", request.ContainerId),
		fmt.Sprintf("STATSD_HOST=%s", os.Getenv("STATSD_HOST")),
		fmt.Sprintf("STATSD_PORT=%s", os.Getenv("STATSD_PORT")),
	}
	env = append(env, containerConfig.Env...)
	env = append(env, request.Env...)
	return env
}

func (s *Worker) readBundleConfig(imageId string) (*specs.Spec, error) {
	imageConfigPath := filepath.Join(s.userImagePath, imageId, "initial_config.json")

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

	var containerConfig *ContainerConfigResponse = &ContainerConfigResponse{
		WorkspacePath: defaultWorkingDirectory,
	}

	env := s.getContainerEnvironment(request, containerConfig, options)
	if request.Gpu != "" {
		spec.Hooks.Prestart[0].Args = append(spec.Hooks.Prestart[0].Args, configPath, "prestart")

		existingCudaFound := false
		env, existingCudaFound = s.containerCudaManager.InjectCudaEnvVars(env, options)
		if !existingCudaFound {
			// If the container image does not have cuda libraries installed, mount cuda from the host
			spec.Mounts = s.containerCudaManager.InjectCudaMounts(spec.Mounts)
		}
	} else {
		spec.Hooks.Prestart = nil
	}

	spec.Process.Env = append(spec.Process.Env, env...)
	spec.Root.Readonly = false

	// Create local workspace path so we can symlink volumes before the container starts
	os.MkdirAll(containerConfig.WorkspacePath, os.FileMode(0755))

	// Add bind mounts to runc spec
	for _, m := range containerConfig.Mounts {
		mode := "rw"
		if m.ReadOnly {
			mode = "r"
		}

		if strings.HasPrefix(m.MountPath, "/volumes") {
			// Creates a symlink in the local workspace path to the container's volume path
			linkPath := filepath.Join(containerConfig.WorkspacePath, filepath.Base(m.MountPath))
			err = forceSymlink(m.MountPath, linkPath)
			if err != nil {
				log.Printf("unable to symlink volume: %v", err)
			}
		}

		// If the local mount path does not exist, create it
		if _, err := os.Stat(m.LocalPath); os.IsNotExist(err) {
			err := os.MkdirAll(m.LocalPath, 0755)
			if err != nil {
				log.Printf("<%s> - failed to create mount directory: %v\n", request.ContainerId, err)
				continue
			}
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "none",
			Source:      m.LocalPath,
			Destination: m.MountPath,
			Options:     []string{"rbind", mode},
		})
	}

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

	// NOTE: because we only handle one GPU request at a time
	// We need to reset this back to the original worker pool limits
	// TODO: Manage number of GPUs explicitly instead of this
	if s.gpuType != "" {
		request.Cpu = s.cpuLimit
		request.Memory = s.memoryLimit
	}

	return s.workerRepo.UpdateWorkerCapacity(worker, request, types.AddCapacity)
}

func (s *Worker) startup() error {
	log.Printf("Worker starting up.")
	s.workerMetrics.WorkerStarted()
	go s.workerMetrics.EmitWorkerDuration()

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

	err = os.MkdirAll(containerLogsPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	return nil
}

func (s *Worker) shutdown() error {
	log.Printf("Worker spinning down.")
	s.workerMetrics.WorkerStopped()

	worker, err := s.workerRepo.GetWorkerById(s.workerId)
	if err != nil {
		return err
	}

	err = s.workerRepo.RemoveWorker(worker)
	if err != nil {
		return err
	}

	s.workerMetrics.Shutdown()
	s.cancel()
	return nil
}
