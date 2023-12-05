package worker

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	common "github.com/beam-cloud/beam/internal/common"
	repo "github.com/beam-cloud/beam/internal/repository"
	types "github.com/beam-cloud/beam/internal/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/slai-labs/go-runc"
)

const (
	RequestProcessingInterval time.Duration = 100 * time.Millisecond
)

type Worker struct {
	apiClient            *ApiClient
	cpuLimit             int64
	memoryLimit          int64
	gpuType              string
	isRemote             bool
	podIPAddr            string
	podHostName          string
	userImagePath        string
	runcHandle           runc.Runc
	containerCudaManager *ContainerCudaManager
	redisClient          *common.RedisClient
	imageClient          *ImageClient
	workerId             string
	runningContainers    map[string]*ContainerInstance
	containerLock        sync.Mutex
	eventBus             *common.EventBus
	containerWg          sync.WaitGroup
	containerRepo        repo.ContainerRepository
	workerMetrics        *WorkerMetrics
	completedRequests    chan *types.ContainerRequest
	stopContainerChan    chan string
	workerRepo           repo.WorkerRepository
	ctx                  context.Context
	cancel               func()
}

type ContainerInstance struct {
	Id         string
	BundlePath string
	Overlay    *common.ContainerOverlay
	Spec       *specs.Spec
	Err        error
	ExitCode   int
	Port       int
}

type ContainerOptions struct {
	BindPort    int
	InitialSpec *specs.Spec
}

type MessageOutputStruct struct {
	Level   string  `json:"level"`
	Message string  `json:"message"`
	TaskID  *string `json:"task_id"`
}

var (
	//go:embed base_runc_config.json
	baseRuncConfigRaw          string
	imagePath                  string   = "/images"
	containerLogsPath          string   = "/var/log/worker"
	baseConfigPath             string   = "/tmp"
	defaultContainerDirectory  string   = "/workspace"
	defaultWorkerSpindownTimeS float64  = 300 // 5 minutes
	containerTmpDirs           []string = []string{"/tmp", "/task"}
)

func NewWorker() (*Worker, error) {
	gpuType := os.Getenv("GPU_TYPE")
	workerId := os.Getenv("WORKER_ID")
	podHostName := os.Getenv("HOSTNAME")
	isRemote := os.Getenv("WORKER_IS_REMOTE") == "true"

	// podIPAddr, err := GetPodIP(isRemote)
	// if err != nil {
	// 	return nil, err
	// }

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

	ctx, cancel := context.WithCancel(context.Background())
	identityRepo := repo.NewIdentityRedisRepository(redisClient)
	containerRepo := repo.NewContainerRedisRepository(redisClient, identityRepo)
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
		isRemote:             isRemote,
		runcHandle:           runc.Runc{},
		containerCudaManager: NewContainerCudaManager(),
		redisClient:          redisClient,
		podIPAddr:            "0.0.0.0",
		imageClient:          imageClient,
		podHostName:          podHostName,
		eventBus:             nil,
		apiClient:            NewApiClient(os.Getenv("WORKER_S2S_TOKEN")),
		workerId:             workerId,
		containerLock:        sync.Mutex{},
		containerWg:          sync.WaitGroup{},
		containerRepo:        containerRepo,
		workerMetrics:        workerMetrics,
		workerRepo:           workerRepo,
		runningContainers:    make(map[string]*ContainerInstance),
		completedRequests:    make(chan *types.ContainerRequest, 1000),
		stopContainerChan:    make(chan string, 1000),
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
		log.Println("hi")

		request, err := s.workerRepo.GetNextContainerRequest(s.workerId)
		if request != nil && err == nil {
			lastContainerRequest = time.Now()
			containerId := request.ContainerId

			s.containerLock.Lock()

			_, ok := s.runningContainers[containerId]
			if !ok {
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
	if (time.Since(lastContainerRequest).Seconds() > defaultWorkerSpindownTimeS) && len(s.runningContainers) == 0 {
		return true
	}
	return false
}

// Spawn a single container and stream output to stdout/stderr
func (s *Worker) RunContainer(request *types.ContainerRequest) error {
	containerID := request.ContainerId
	bundlePath := filepath.Join(s.userImagePath, request.ImageTag)

	log.Printf("<%s> - pulling image: %s\n", containerID, request.ImageTag)
	err := s.imageClient.Pull(request.ImageTag)
	if err != nil {
		return err
	}
	log.Printf("<%s> - successfully pulled image: %s\n", containerID, request.ImageTag)

	// Every 30 seconds, update container status
	go func() {
		for {
			time.Sleep(30 * time.Second)

			s.containerLock.Lock()
			_, ok := s.runningContainers[containerID]
			s.containerLock.Unlock()

			if !ok {
				return
			}

			// Stop container if it is "orphaned" - meaning it's running but has no associated state
			if _, err := s.containerRepo.GetContainerState(containerID); err != nil {
				if _, ok := err.(*types.ErrContainerStateNotFound); ok {
					log.Printf("<%s> - container state not found, stopping container\n", containerID)
					s.stopContainerChan <- containerID
					return
				}
			}

			err = s.containerRepo.UpdateContainerStatus(containerID, types.ContainerStatusRunning, time.Duration(types.ContainerStateTtlS)*time.Second)
			if err != nil {
				log.Printf("<%s> - unable to update container state: %v\n", containerID, err)
			}

			log.Printf("<%s> - container still running: %s\n", containerID, request.ImageTag)
		}
	}()

	bindPort, err := GetRandomFreePort()
	if err != nil {
		return err
	}
	log.Printf("<%s> - acquired port: %d\n", containerID, bindPort)

	// Read spec from bundle
	initialBundleSpec, _ := s.readBundleConfig(request.ImageTag)

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

// Invoke a runc container using a predefined config spec
func (s *Worker) SpawnAsync(request *types.ContainerRequest, bundlePath string, spec *specs.Spec) error {
	containerId := request.ContainerId
	outputChan := make(chan common.OutputMsg)

	// RX and print stdout/stderr from spawned container
	logFilePath := path.Join(containerLogsPath, fmt.Sprintf("%s.log", containerId))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to create container log file: %w", err)
	}

	// Create a new logger
	containerLogger := logrus.New()
	containerLogger.SetOutput(logFile)
	containerLogger.SetFormatter(&logrus.JSONFormatter{})

	// Handle stdout/stderr from spawned container
	go func() {
		defer logFile.Close()

		for o := range outputChan {
			dec := json.NewDecoder(strings.NewReader(o.Msg))
			msgDecoded := false

			for {
				var msg MessageOutputStruct

				err := dec.Decode(&msg)
				if err != nil {
					/*
						Either the json parsing ends with an EOF error indicating that the
						JSON string is complete, or with a json decode error indicating that
						the JSON string is invalid. In either case, we can break out of the
						decode loop and continue processing the next message.
					*/
					break
				}

				msgDecoded = true

				containerLogger.WithFields(logrus.Fields{
					"container_id": containerId,
					"identity_id":  request.IdentityId,
					"task_id":      msg.TaskID,
				}).Info(msg.Message)

				if msg.TaskID != nil {
					log.Printf("<%s>:<%s> - %s\n", containerId, *msg.TaskID, msg.Message)
				} else if msg.Message != "" {
					log.Printf("<%s> - %s\n", containerId, msg.Message)
				}
			}

			if !msgDecoded && o.Msg != "" {
				// Fallback in case the message was not JSON
				containerLogger.WithFields(logrus.Fields{
					"container_id": containerId,
					"identity_id":  request.IdentityId,
				}).Info(o.Msg)

				log.Printf("<%s> - %s\n", containerId, o.Msg)
			}

		}
	}()

	s.containerWg.Add(1)
	go s.spawn(request, bundlePath, spec, outputChan)

	return nil
}

func (s *Worker) stopContainer(event *common.Event) bool {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	containerId := event.Args["container_id"].(string)

	var err error = nil
	if _, containerExists := s.runningContainers[containerId]; containerExists {
		log.Printf("<%s> - received stop container event.\n", containerId)
		s.stopContainerChan <- containerId
	}

	return err == nil
}

func (s *Worker) processStopContainerEvents() {
	for containerId := range s.stopContainerChan {
		log.Printf("<%s> - stopping container.\n", containerId)

		if _, containerExists := s.runningContainers[containerId]; !containerExists {
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

	delete(s.runningContainers, containerId)

	err := s.containerRepo.DeleteContainerState(request)
	if err != nil {
		log.Printf("<%s> - failed to remove container state: %v\n", containerId, err)
	}

	s.completedRequests <- request
}

// Spawn a container using runc binary
func (s *Worker) spawn(request *types.ContainerRequest, bundlePath string, spec *specs.Spec, outputChan chan common.OutputMsg) {
	s.workerRepo.AddContainerRequestToWorker(s.workerId, request.ContainerId, request)
	defer s.workerRepo.RemoveContainerRequestFromWorker(s.workerId, request.ContainerId)
	defer s.deleteTemporaryDirectories(request)

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

	outputWriter := common.NewOutputWriter(func(s string) {
		outputChan <- common.OutputMsg{
			Msg:     strings.TrimSuffix(string(s), "\n"),
			Done:    false,
			Success: false,
		}
	})

	// Add the container instance to the runningContainers map
	containerInstance := &ContainerInstance{
		Id:         containerId,
		BundlePath: bundlePath,
		Overlay:    common.NewContainerOverlay(containerId, bundlePath, baseConfigPath, filepath.Join(bundlePath, "rootfs")),
		Spec:       spec,
		ExitCode:   -1,
	}
	s.runningContainers[containerId] = containerInstance

	go func() {
		time.Sleep(time.Second)

		s.containerLock.Lock()
		defer s.containerLock.Unlock()

		_, ok := s.runningContainers[containerId]
		if !ok {
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
	s.workerMetrics.ContainerStarted(containerId, request.IdentityId)
	defer s.workerMetrics.ContainerStopped(containerId, request.IdentityId)

	pidChan := make(chan int, 1)
	go s.workerMetrics.EmitResourceUsage(request, pidChan, done)

	// Invoke runc process (launch the container)
	exitCode, err = s.runcHandle.Run(s.ctx, containerId, bundlePath, &runc.CreateOpts{
		OutputWriter: outputWriter,
		ConfigPath:   configPath,
		Started:      pidChan,
	})

	if err != nil {
		containerErr = err
		return
	}

	if exitCode != 0 {
		containerErr = fmt.Errorf("unable to run container: %d", exitCode)
	}
}

func (s *Worker) deleteTemporaryDirectories(request *types.ContainerRequest) {
	for _, dir := range containerTmpDirs {
		containerTmpDir := filepath.Join(dir, request.ContainerId, dir) // NOTE: Cleanup this logic where the prefix and suffix are the same
		os.RemoveAll(containerTmpDir)
	}
}

func (s *Worker) getContainerConfig(request *types.ContainerRequest) (*ContainerConfigResponse, error) {
	response, err := s.apiClient.GetContainerConfig(&ContainerConfigRequest{
		ContainerId: request.ContainerId,
	})
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (s *Worker) getContainerEnvironment(request *types.ContainerRequest, containerConfig *ContainerConfigResponse, options *ContainerOptions) []string {
	env := []string{
		fmt.Sprintf("IDENTITY_ID=%s", containerConfig.IdentityId),
		fmt.Sprintf("S2S_TOKEN=%s", containerConfig.S2SToken),
		fmt.Sprintf("BIND_PORT=%d", options.BindPort),
		fmt.Sprintf("BEAM_NAMESPACE=%s", os.Getenv("BEAM_NAMESPACE")),
		fmt.Sprintf("BEAM_API_BASE_URL=%s", os.Getenv("BEAMAPI_BASE_URL")),
		fmt.Sprintf("BACKEND_BASE_URL=%s", os.Getenv("BACKEND_BASE_URL")),
		fmt.Sprintf("BEAM_WORKBUS_HOST=%s", os.Getenv("BEAM_WORKBUS_HOST")),
		fmt.Sprintf("BEAM_WORKBUS_PORT=%s", os.Getenv("BEAM_WORKBUS_PORT")),
		fmt.Sprintf("CONTAINER_HOSTNAME=%s", fmt.Sprintf("%s:%d", s.podIPAddr, options.BindPort)),
		fmt.Sprintf("CONTAINER_ID=%s", request.ContainerId),
		fmt.Sprintf("STATSD_HOST=%s", os.Getenv("STATSD_HOST")),
		fmt.Sprintf("STATSD_PORT=%s", os.Getenv("STATSD_PORT")),
	}
	env = append(env, containerConfig.Env...)
	env = append(env, request.Env...)
	return env
}

func (s *Worker) readBundleConfig(imageTag string) (*specs.Spec, error) {
	imageConfigPath := filepath.Join(s.userImagePath, imageTag, "initial_config.json")

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

	containerConfig, err := s.getContainerConfig(request)
	if err != nil {
		return nil, err
	}

	if containerConfig == nil {
		return nil, errors.New("invalid container config")
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

	// Create local workspace path so we can symlink volumes before the container starts
	os.MkdirAll(containerConfig.WorkspacePath, os.FileMode(0755))

	// Create tmp directories in host so that we can bind them to container
	for _, dir := range containerTmpDirs {
		containerTmpDir := filepath.Join(dir, request.ContainerId, dir) // NOTE: Cleanup this logic where the prefix and suffix are the same
		err = os.MkdirAll(containerTmpDir, 0755)
		if err != nil {
			log.Printf("<%s> - failed to create %s directory: %v\n", request.ContainerId, dir, err)
			return nil, err
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "bind",
			Source:      containerTmpDir,
			Destination: dir,
			Options:     []string{"bind", "mode=755", "nosuid", "strictatime", "rslave"},
		})
	}

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
				log.Printf("Symlink Error: %v", err)
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
	return s.redisClient.Close()
}
