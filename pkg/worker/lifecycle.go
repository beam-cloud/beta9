package worker

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"

	runc "github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	baseConfigPath            string        = "/tmp"
	defaultContainerDirectory string        = "/mnt/code"
	specBaseName              string        = "config.json"
	initialSpecBaseName       string        = "initial_config.json"
	runcEventsInterval        time.Duration = 5 * time.Second
	containerInnerPort        int           = 8001 // Use a fixed port inside the container
	exitCodeSigterm           int           = 143  // Means the container received a SIGTERM by the underlying operating system
	exitCodeSigkill           int           = 137  // Means the container received a SIGKILL by the underlying operating system
)

//go:embed base_runc_config.json
var baseRuncConfigRaw string

// These parameters are set when runcResourcesEnforced is enabled in the worker config
var cgroupV2Parameters map[string]string = map[string]string{
	// When an OOM occurs within a runc container, the kernel will kill the container and all procceses within it.
	"memory.oom.group": "1",
}

// handleStopContainerEvent used by the event bus to stop a container.
// Containers are stopped by sending a stop container event to stopContainerChan.
func (s *Worker) handleStopContainerEvent(event *common.Event) bool {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	stopArgs, err := types.ToStopContainerArgs(event.Args)
	if err != nil {
		log.Error().Str("worker_id", s.workerId).Msgf("failed to parse stop container args: %v", err)
		return false
	}

	if _, exists := s.containerInstances.Get(stopArgs.ContainerId); exists {
		log.Info().Str("container_id", stopArgs.ContainerId).Msg("received stop container event")
		s.stopContainerChan <- stopContainerEvent{ContainerId: stopArgs.ContainerId, Kill: stopArgs.Force}
	}

	return true
}

// stopContainer stops a runc container. When force is true, a SIGKILL signal is sent to the container.
func (s *Worker) stopContainer(containerId string, kill bool) error {
	log.Info().Str("container_id", containerId).Msg("stopping container")

	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		log.Info().Str("container_id", containerId).Msg("container not found")
		return nil
	}

	signal := int(syscall.SIGTERM)
	if kill {
		signal = int(syscall.SIGKILL)
	}

	err := s.runcHandle.Kill(context.Background(), instance.Id, signal, &runc.KillOpts{All: true})
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("error stopping container: %v", err)
		s.containerNetworkManager.TearDown(containerId)
		return nil
	}

	log.Info().Str("container_id", containerId).Msg("container stopped")
	return nil
}

func (s *Worker) finalizeContainer(containerId string, request *types.ContainerRequest, exitCode *int) {
	defer s.containerWg.Done()

	if *exitCode < 0 {
		*exitCode = 1
	} else if *exitCode == exitCodeSigterm {
		*exitCode = 0
	}

	_, err := handleGRPCResponse(s.containerRepoClient.SetContainerExitCode(context.Background(), &pb.SetContainerExitCodeRequest{
		ContainerId: containerId,
		ExitCode:    int32(*exitCode),
	}))
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to set exit code: %v", err)
	}

	s.clearContainer(containerId, request, *exitCode)
}

func (s *Worker) clearContainer(containerId string, request *types.ContainerRequest, exitCode int) {
	s.containerLock.Lock()

	// De-allocate GPU devices so they are available for new containers
	if request.Gpu != "" {
		s.containerCudaManager.UnassignGPUDevices(containerId)
	}

	// Tear down container network components
	err := s.containerNetworkManager.TearDown(request.ContainerId)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Msgf("failed to clean up container network: %v", err)
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
		select {
		case <-time.After(time.Duration(s.config.Worker.TerminationGracePeriod) * time.Second):
		case <-s.ctx.Done():
		}

		// If the container is still running, stop it. This happens when a sigterm is detected.
		container, err := s.runcHandle.State(context.TODO(), containerId)
		if err == nil && container.Status == types.RuncContainerStatusRunning {
			if err := s.stopContainer(containerId, true); err != nil {
				log.Error().Str("container_id", containerId).Msgf("failed to stop container: %v", err)
			}
		}

		s.deleteContainer(containerId, err)

		log.Info().Str("container_id", containerId).Msg("finalized container shutdown")
	}()
}

func (s *Worker) deleteContainer(containerId string, err error) {
	s.containerInstances.Delete(containerId)

	_, err = handleGRPCResponse(s.containerRepoClient.DeleteContainerState(context.Background(), &pb.DeleteContainerStateRequest{ContainerId: containerId}))
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to remove container state: %v", err)
	}
}

// Spawn a single container and stream output to stdout/stderr
func (s *Worker) RunContainer(ctx context.Context, request *types.ContainerRequest) error {
	containerId := request.ContainerId

	s.containerInstances.Set(containerId, &ContainerInstance{
		Id:        containerId,
		StubId:    request.StubId,
		LogBuffer: common.NewLogBuffer(),
		Request:   request,
	})

	bundlePath := filepath.Join(s.imageMountPath, request.ImageId)

	// Set worker hostname
	hostname := fmt.Sprintf("%s:%d", s.podAddr, s.runcServer.port)
	_, err := handleGRPCResponse(s.containerRepoClient.SetWorkerAddress(context.Background(), &pb.SetWorkerAddressRequest{
		ContainerId: containerId,
		Address:     hostname,
	}))
	if err != nil {
		return err
	}

	logChan := make(chan common.LogRecord)
	outputLogger := slog.New(common.NewChannelHandler(logChan))

	// Handle stdout/stderr
	go s.containerLogger.CaptureLogs(containerId, logChan)

	// Attempt to pull image
	log.Info().Str("container_id", containerId).Msgf("lazy-pulling image: %s", request.ImageId)
	if err := s.imageClient.PullLazy(request); err != nil {
		if !request.IsBuildRequest() {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
			if err := s.buildOrPullBaseImage(ctx, request, containerId, outputLogger); err != nil {
				return err
			}
		}
	}

	bindPort, err := getRandomFreePort()
	if err != nil {
		return err
	}
	log.Info().Str("container_id", containerId).Msgf("acquired port: %d", bindPort)

	// Read spec from bundle
	initialBundleSpec, _ := s.readBundleConfig(request.ImageId, request.IsBuildRequest())

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
	log.Info().Str("container_id", containerId).Msg("successfully created spec from request")

	// Set an address (ip:port) for the pod/container in Redis. Depending on the stub type,
	// gateway may need to directly interact with this pod/container.
	containerAddr := fmt.Sprintf("%s:%d", s.podAddr, bindPort)
	_, err = handleGRPCResponse(s.containerRepoClient.SetContainerAddress(context.Background(), &pb.SetContainerAddressRequest{
		ContainerId: request.ContainerId,
		Address:     containerAddr,
	}))
	if err != nil {
		return err
	}
	log.Info().Str("container_id", containerId).Msg("set container address")

	go s.containerWg.Add(1)

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Start the container
		go s.spawn(request, spec, outputLogger, opts)
	}

	log.Info().Str("container_id", containerId).Msg("spawned successfully")
	return nil
}

func (s *Worker) buildOrPullBaseImage(ctx context.Context, request *types.ContainerRequest, containerId string, outputLogger *slog.Logger) error {
	switch {
	case request.BuildOptions.Dockerfile != nil:
		log.Info().Str("container_id", containerId).Msg("lazy-pull failed, building image from Dockerfile")
		if err := s.imageClient.BuildAndArchiveImage(ctx, outputLogger, request); err != nil {
			return err
		}
	case request.BuildOptions.SourceImage != nil:
		log.Info().Str("container_id", containerId).Msgf("lazy-pull failed, pulling source image: %s", *request.BuildOptions.SourceImage)

		if err := s.imageClient.PullAndArchiveImage(ctx, outputLogger, request); err != nil {
			return err
		}
	}

	// Try pull again after building or pulling the source image
	return s.imageClient.PullLazy(request)
}

func (s *Worker) readBundleConfig(imageId string, isBuildRequest bool) (*specs.Spec, error) {
	imageConfigPath := filepath.Join(s.imageMountPath, imageId, initialSpecBaseName)
	if isBuildRequest {
		imageConfigPath = filepath.Join(s.imageMountPath, imageId, specBaseName)
	}

	data, err := os.ReadFile(imageConfigPath)
	if err != nil {
		log.Error().Str("image_id", imageId).Str("image_config_path", imageConfigPath).Err(err).Msg("failed to read bundle config")
		return nil, err
	}

	specTemplate := strings.TrimSpace(string(data))
	var spec specs.Spec

	err = json.Unmarshal([]byte(specTemplate), &spec)
	if err != nil {
		log.Error().Str("image_id", imageId).Str("image_config_path", imageConfigPath).Err(err).Msg("failed to unmarshal bundle config")
		return nil, err
	}

	return &spec, nil
}

// Generate a runc spec from a given request
func (s *Worker) specFromRequest(request *types.ContainerRequest, options *ContainerOptions) (*specs.Spec, error) {
	os.MkdirAll(filepath.Join(baseConfigPath, request.ContainerId), os.ModePerm)
	configPath := filepath.Join(baseConfigPath, request.ContainerId, specBaseName)

	spec, err := s.newSpecTemplate()
	if err != nil {
		return nil, err
	}

	spec.Process.Cwd = defaultContainerDirectory
	spec.Process.Args = request.EntryPoint
	spec.Process.Terminal = true // NOTE: This is since we are using a console writer for logging

	if s.config.Worker.RunCResourcesEnforced {
		spec.Linux.Resources.CPU = getLinuxCPU(request)
		spec.Linux.Resources.Memory = getLinuxMemory(request)
		spec.Linux.Resources.Unified = cgroupV2Parameters
	}

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

	// We need to modify the spec to support Cedana C/R if enabled
	if s.IsCRIUAvailable() && request.CheckpointEnabled {
		containerHostname := fmt.Sprintf("%s:%d", s.podAddr, options.BindPort)
		s.cedanaClient.PrepareContainerSpec(spec, request.ContainerId, containerHostname, request.RequiresGPU())
	}

	spec.Process.Env = append(spec.Process.Env, env...)
	spec.Root.Readonly = false

	var volumeCacheMap map[string]string = make(map[string]string)

	// Add bind mounts to runc spec
	for _, m := range request.Mounts {
		// Skip mountpoint storage if the local path does not exist (mounting failed)
		if m.MountType == storage.StorageModeMountPoint {
			if _, err := os.Stat(m.LocalPath); os.IsNotExist(err) {
				continue
			}
		} else {
			volumeCacheMap[filepath.Base(m.MountPath)] = m.LocalPath

			if _, err := os.Stat(m.LocalPath); os.IsNotExist(err) {
				err := os.MkdirAll(m.LocalPath, 0755)
				if err != nil {
					log.Error().Str("container_id", request.ContainerId).Msgf("failed to create mount directory: %v", err)
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
				log.Error().Str("container_id", request.ContainerId).Msgf("unable to symlink volume: %v", err)
			}
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "none",
			Source:      m.LocalPath,
			Destination: m.MountPath,
			Options:     []string{"rbind", mode},
		})
	}

	// If volume caching is enabled, set it up and add proper mounts to spec
	if s.fileCacheManager.CacheAvailable() && request.Workspace.VolumeCacheEnabled && !request.IsBuildRequest() {
		err = s.fileCacheManager.EnableVolumeCaching(request.Workspace.Name, volumeCacheMap, spec)
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to setup volume caching: %v", err)
		}
	}

	// Configure resolv.conf
	resolvMount := specs.Mount{
		Type:        "none",
		Source:      "/workspace/resolv.conf",
		Destination: "/etc/resolv.conf",
		Options: []string{
			"ro",
			"rbind",
			"rprivate",
			"nosuid",
			"noexec",
			"nodev",
		},
	}

	if s.config.Worker.UseHostResolvConf {
		resolvMount.Source = "/etc/resolv.conf"
	}

	spec.Mounts = append(spec.Mounts, resolvMount)
	return spec, nil
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

func (s *Worker) getContainerEnvironment(request *types.ContainerRequest, options *ContainerOptions) []string {
	// Most of these env vars are required to communicate with the gateway and vice versa
	env := []string{
		fmt.Sprintf("BIND_PORT=%d", containerInnerPort),
		fmt.Sprintf("CONTAINER_HOSTNAME=%s", fmt.Sprintf("%s:%d", s.podAddr, options.BindPort)),
		fmt.Sprintf("CONTAINER_ID=%s", request.ContainerId),
		fmt.Sprintf("BETA9_GATEWAY_HOST=%s", os.Getenv("BETA9_GATEWAY_HOST")),
		fmt.Sprintf("BETA9_GATEWAY_PORT=%s", os.Getenv("BETA9_GATEWAY_PORT")),
		fmt.Sprintf("CHECKPOINT_ENABLED=%t", request.CheckpointEnabled && s.IsCRIUAvailable()),
		"PYTHONUNBUFFERED=1",
	}

	// Add env vars from request
	env = append(request.Env, env...)

	// Add env vars from initial spec. This would be the case for regular workers, not build workers.
	if options.InitialSpec != nil {
		env = append(options.InitialSpec.Process.Env, env...)
	}

	return env
}

// spawn a container using runc binary
func (s *Worker) spawn(request *types.ContainerRequest, spec *specs.Spec, outputLogger *slog.Logger, opts *ContainerOptions) {
	ctx, cancel := context.WithCancel(s.ctx)

	s.workerRepoClient.AddContainerToWorker(ctx, &pb.AddContainerToWorkerRequest{
		WorkerId:    s.workerId,
		ContainerId: request.ContainerId,
	})
	defer s.workerRepoClient.RemoveContainerFromWorker(ctx, &pb.RemoveContainerFromWorkerRequest{
		WorkerId:    s.workerId,
		ContainerId: request.ContainerId,
	})
	defer cancel()

	exitCode := -1
	containerId := request.ContainerId

	// Unmount external s3 buckets
	defer s.containerMountManager.RemoveContainerMounts(containerId)

	// Cleanup container state and resources
	defer s.finalizeContainer(containerId, request, &exitCode)

	// Create overlayfs for container
	overlay := s.createOverlay(request, opts.BundlePath)

	containerInstance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return
	}
	containerInstance.BundlePath = opts.BundlePath
	containerInstance.Overlay = overlay
	containerInstance.Spec = spec
	containerInstance.ExitCode = -1
	containerInstance.OutputWriter = common.NewOutputWriter(func(s string) {
		outputLogger.Info(s, "done", false, "success", false)
	})
	s.containerInstances.Set(containerId, containerInstance)

	// Every 30 seconds, update container status
	go s.updateContainerStatus(request)

	go func() {
		time.Sleep(time.Second)

		s.containerLock.Lock()
		defer s.containerLock.Unlock()

		_, exists := s.containerInstances.Get(containerId)
		if !exists {
			return
		}

		_, err := handleGRPCResponse(s.containerRepoClient.GetContainerState(context.Background(), &pb.GetContainerStateRequest{ContainerId: containerId}))
		if err != nil {
			notFoundErr := &types.ErrContainerStateNotFound{}

			if notFoundErr.From(err) {
				log.Info().Str("container_id", containerId).Msg("container state not found, returning")
				return
			}
		}

		// Update container status to running
		_, err = handleGRPCResponse(s.containerRepoClient.UpdateContainerStatus(context.Background(), &pb.UpdateContainerStatusRequest{
			ContainerId:   containerId,
			Status:        string(types.ContainerStatusRunning),
			ExpirySeconds: int64(types.ContainerStateTtlS),
		}))
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to update container status to running: %v", err)
		}

		// TODO: add schedule time scheduling latency
		requestTime := request.Timestamp.Unix()
		now := time.Now().Unix()
		schedulingLatency := now - requestTime
		log.Info().Str("container_id", containerId).Msgf("scheduling latency: %d seconds", schedulingLatency)
	}()

	// Setup container overlay filesystem
	err := containerInstance.Overlay.Setup()
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to setup overlay: %v", err)
		return
	}
	defer containerInstance.Overlay.Cleanup()

	spec.Root.Path = containerInstance.Overlay.TopLayerPath()

	// Setup container network namespace / devices
	err = s.containerNetworkManager.Setup(containerId, spec)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to setup container network: %v", err)
		return
	}

	// Expose the bind port
	err = s.containerNetworkManager.ExposePort(containerId, opts.BindPort, containerInnerPort)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to expose container bind port: %v", err)
		return
	}

	// Write runc config spec to disk
	configContents, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return
	}

	configPath := filepath.Join(baseConfigPath, containerId, specBaseName)
	err = os.WriteFile(configPath, configContents, 0644)
	if err != nil {
		return
	}

	consoleWriter, err := NewConsoleWriter(containerInstance.OutputWriter)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to create console writer: %v", err)
		return
	}

	// Log metrics
	go s.workerMetrics.EmitContainerUsage(ctx, request)
	go s.eventRepo.PushContainerStartedEvent(containerId, s.workerId, request)
	defer func() { go s.eventRepo.PushContainerStoppedEvent(containerId, s.workerId, request) }()

	startedChan := make(chan int, 1)

	// Handle checkpoint creation & restore if applicable
	if s.IsCRIUAvailable() && request.CheckpointEnabled {
		restored, restoredContainerId, err := s.attemptCheckpointOrRestore(ctx, request, consoleWriter, startedChan, configPath)
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("C/R failed: %v", err)
		}

		if restored {
			// HOTFIX: If we restored from a checkpoint, we need to use the container ID of the restored container
			// instead of the original container ID
			containerInstance, exists := s.containerInstances.Get(request.ContainerId)
			if exists {
				containerInstance.Id = restoredContainerId
				s.containerInstances.Set(containerId, containerInstance)
				containerId = restoredContainerId
			}

			exitCode = s.waitForRestoredContainer(ctx, containerId, startedChan, outputLogger, request, spec)
			return
		}
	}

	// Invoke runc process (launch the container)
	_, err = s.runcHandle.Run(s.ctx, containerId, opts.BundlePath, &runc.CreateOpts{
		Detach:        true,
		ConsoleSocket: consoleWriter,
		ConfigPath:    configPath,
		Started:       startedChan,
	})
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to run container: %v", err)
		return
	}

	exitCode = s.wait(ctx, containerId, startedChan, outputLogger, request, spec)
}

// Wait for a container to exit and return the exit code
func (s *Worker) wait(ctx context.Context, containerId string, startedChan chan int, outputLogger *slog.Logger, request *types.ContainerRequest, spec *specs.Spec) int {
	<-startedChan

	// Clean up runc container state and send final output message
	cleanup := func(exitCode int, err error) int {
		log.Info().Str("container_id", containerId).Msgf("container has exited with code: %d", exitCode)

		outputLogger.Info("", "done", true, "success", exitCode == 0)

		err = s.runcHandle.Delete(s.ctx, containerId, &runc.DeleteOpts{Force: true})
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to delete container: %v", err)
		}

		return exitCode
	}

	// Look up the PID of the container
	state, err := s.runcHandle.State(ctx, containerId)
	if err != nil {
		return cleanup(-1, err)
	}
	pid := state.Pid

	// Start monitoring the container
	go s.collectAndSendContainerMetrics(ctx, request, spec, pid) // Capture resource usage (cpu/mem/gpu)
	go s.watchOOMEvents(ctx, request, outputLogger)              // Watch for OOM events

	process, err := os.FindProcess(pid)
	if err != nil {
		return cleanup(-1, err)
	}

	// Wait for the container to exit
	processState, err := process.Wait()
	if err != nil {
		return cleanup(-1, err)
	}

	return cleanup(processState.ExitCode(), nil)
}

func (s *Worker) createOverlay(request *types.ContainerRequest, bundlePath string) *common.ContainerOverlay {
	// For images that have a rootfs, set that as the root path
	// otherwise, assume runc config files are in the rootfs themselves
	rootPath := filepath.Join(bundlePath, "rootfs")
	if _, err := os.Stat(rootPath); os.IsNotExist(err) {
		rootPath = bundlePath
	}

	overlayPath := baseConfigPath
	if request.IsBuildRequest() {
		overlayPath = "/dev/shm"
	}

	return common.NewContainerOverlay(request.ContainerId, rootPath, overlayPath)
}

func (s *Worker) watchOOMEvents(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) {
	var (
		seenEvents  = make(map[string]struct{})
		ch          <-chan *runc.Event
		err         error
		ticker      = time.NewTicker(time.Second)
		containerId = request.ContainerId
	)

	defer ticker.Stop()

	select {
	case <-ctx.Done():
		return
	default:
		ch, err = s.runcHandle.Events(ctx, containerId, time.Second)
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to open runc events channel: %v", err)
			return
		}
	}

	maxTries, tries := 5, 0 // Used for re-opening the channel if it's closed
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			seenEvents = make(map[string]struct{})
		case event, ok := <-ch:
			if !ok { // If the channel is closed, try to re-open it
				if tries == maxTries-1 {
					log.Error().Str("container_id", containerId).Msg("failed to watch for OOM events")
					return
				}

				tries++
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					ch, err = s.runcHandle.Events(ctx, containerId, time.Second)
					if err != nil {
						log.Error().Str("container_id", containerId).Msgf("failed to open runc events channel: %v", err)
					}
				}
				continue
			}

			if _, ok := seenEvents[event.Type]; ok {
				continue
			}

			seenEvents[event.Type] = struct{}{}

			if event.Type == "oom" {
				outputLogger.Error("Container was killed due to out-of-memory conditions.")
				s.eventRepo.PushContainerOOMEvent(containerId, s.workerId, request)
			}
		}
	}
}
