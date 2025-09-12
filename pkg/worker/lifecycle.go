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
	"sync/atomic"
	"syscall"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"tags.cncf.io/container-device-interface/pkg/cdi"

	runc "github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	baseConfigPath            string        = "/tmp"
	defaultContainerDirectory string        = types.WorkerUserCodeVolume
	specBaseName              string        = "config.json"
	initialSpecBaseName       string        = "initial_config.json"
	runcEventsInterval        time.Duration = 5 * time.Second
	containerInnerPort        int           = 8001 // Use a fixed port inside the container
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

	if containerInstance, exists := s.containerInstances.Get(stopArgs.ContainerId); exists {
		log.Info().Str("container_id", stopArgs.ContainerId).Msg("received stop container event")
		containerInstance.StopReason = stopArgs.Reason
		s.containerInstances.Set(stopArgs.ContainerId, containerInstance)
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
	} else if *exitCode == int(types.ContainerExitCodeSigterm) {
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
		s.containerGPUManager.UnassignGPUDevices(containerId)
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

		s.deleteContainer(containerId)

		log.Info().Str("container_id", containerId).Msg("finalized container shutdown")
	}()
}

func (s *Worker) deleteContainer(containerId string) {
	s.containerInstances.Delete(containerId)

	_, err := handleGRPCResponse(s.containerRepoClient.DeleteContainerState(context.Background(), &pb.DeleteContainerStateRequest{ContainerId: containerId}))
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

	logChan := make(chan common.LogRecord, 1000)
	outputLogger := slog.New(common.NewChannelHandler(logChan))

	// Handle stdout/stderr
	go s.containerLogger.CaptureLogs(request, logChan)

	// Attempt to pull image
	outputLogger.Info(fmt.Sprintf("Loading image <%s>...\n", request.ImageId))
	elapsed, err := s.imageClient.PullLazy(ctx, request, outputLogger)
	if err != nil {
		if !request.IsBuildRequest() {
			log.Error().Str("container_id", containerId).Msgf("failed to pull image: %v", err)
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
			if err := s.buildOrPullBaseImage(ctx, request, containerId, outputLogger); err != nil {
				return err
			}
			elapsed, err = s.imageClient.PullLazy(ctx, request, outputLogger)
			if err != nil {
				return err
			}
		}
	}
	outputLogger.Info(fmt.Sprintf("Loaded image <%s>, took: %s\n", request.ImageId, elapsed))

	// Determine how many ports we need to expose
	portsToExpose := len(request.Ports)
	if portsToExpose == 0 {
		portsToExpose = 1
		request.Ports = []uint32{uint32(containerInnerPort)}
	}

	// Expose SSH port
	request.Ports = append(request.Ports, uint32(types.WorkerShellPort))
	portsToExpose++

	// Only expose checkpoint exposed ports if they are available
	if request.Checkpoint != nil {
		request.Ports = request.Checkpoint.ExposedPorts
		portsToExpose = len(request.Ports)
	}

	bindPorts := make([]int, 0, portsToExpose)
	for i := 0; i < portsToExpose; i++ {
		bindPort, err := getRandomFreePort()
		if err != nil {
			return err
		}
		bindPorts = append(bindPorts, bindPort)
	}

	log.Info().Str("container_id", containerId).Msgf("acquired ports: %v", bindPorts)

	// Read spec from bundle
	initialBundleSpec, _ := s.readBundleConfig(request.ImageId, request.IsBuildRequest())

	opts := &ContainerOptions{
		BundlePath:   bundlePath,
		HostBindPort: bindPorts[0],
		BindPorts:    bindPorts,
		InitialSpec:  initialBundleSpec,
	}

	err = s.containerMountManager.SetupContainerMounts(ctx, request, outputLogger)
	if err != nil {
		s.containerLogger.Log(request.ContainerId, request.StubId, "failed to setup container mounts: %v", err)
	}

	// Generate dynamic runc spec for this container
	spec, err := s.specFromRequest(request, opts)
	if err != nil {
		return err
	}
	log.Info().Str("container_id", containerId).Msg("successfully created spec from request")

	// Set an address (ip:port) for the pod/container in Redis. Depending on the stub type,
	// gateway may need to directly interact with this pod/container.
	containerAddr := fmt.Sprintf("%s:%d", s.podAddr, opts.BindPorts[0])
	_, err = handleGRPCResponse(s.containerRepoClient.SetContainerAddress(context.Background(), &pb.SetContainerAddressRequest{
		ContainerId: request.ContainerId,
		Address:     containerAddr,
	}))
	if err != nil {
		return err
	}
	log.Info().Str("container_id", containerId).Msgf("set container address: %s", containerAddr)

	addressMap := make(map[int32]string)
	for idx, containerPort := range request.Ports {
		addressMap[int32(containerPort)] = fmt.Sprintf("%s:%d", s.podAddr, opts.BindPorts[idx])
	}
	_, err = handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
		ContainerId: request.ContainerId,
		AddressMap:  addressMap,
	}))
	if err != nil {
		return err
	}

	log.Info().Str("container_id", containerId).Msgf("set container address map: %v", addressMap)

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

	return nil
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

	spec, err := s.newSpecTemplate()
	if err != nil {
		return nil, err
	}

	spec.Process.Cwd = defaultContainerDirectory
	spec.Process.Args = request.EntryPoint
	spec.Process.Terminal = false

	if request.Stub.Type.Kind() == types.StubTypePod {
		if len(request.EntryPoint) == 0 {
			log.Info().
				Str("container_id", request.ContainerId).
				Str("stub_id", request.StubId).
				Str("entrypoint", strings.Join(request.EntryPoint, " ")).
				Msg("no entrypoint provided, using initial spec entrypoint")

			spec.Process.Args = options.InitialSpec.Process.Args
		}

		spec.Process.Cwd = options.InitialSpec.Process.Cwd
		spec.Process.User.UID = options.InitialSpec.Process.User.UID
		spec.Process.User.GID = options.InitialSpec.Process.User.GID
	}

	throttlingEnabled := !request.IsBuildRequest() && !request.RequiresGPU()
	if throttlingEnabled && (s.config.Worker.ContainerResourceLimits.CPUEnforced || s.config.Worker.ContainerResourceLimits.MemoryEnforced) {
		spec.Linux.Resources.Unified = cgroupV2Parameters

		if s.config.Worker.ContainerResourceLimits.CPUEnforced {
			spec.Linux.Resources.CPU = getLinuxCPU(request)
		}

		if s.config.Worker.ContainerResourceLimits.MemoryEnforced {
			spec.Linux.Resources.Memory = getLinuxMemory(request)
		}
	}

	env := s.getContainerEnvironment(request, options)
	if request.Gpu != "" {
		env = s.containerGPUManager.InjectEnvVars(env)
	}
	spec.Process.Env = append(spec.Process.Env, env...)

	// We need to include the checkpoint signal files in the container spec
	if s.IsCRIUAvailable(request.GpuCount) && request.CheckpointEnabled {
		err = os.MkdirAll(checkpointSignalDir(request.ContainerId), os.ModePerm) // Add a mount point for the checkpoint signal file
		if err != nil {
			return nil, err
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "bind",
			Source:      checkpointSignalDir(request.ContainerId),
			Destination: "/criu",
			Options: []string{
				"rbind",
				"rprivate",
				"nosuid",
				"nodev",
			},
		})

		containerIdPath := filepath.Join(checkpointSignalDir(request.ContainerId), checkpointContainerIdFileName)
		err := os.WriteFile(containerIdPath, []byte(request.ContainerId), 0644)
		if err != nil {
			return nil, err
		}

		containerHostname := fmt.Sprintf("%s:%d", s.podAddr, options.HostBindPort)
		containerHostnamePath := filepath.Join(checkpointSignalDir(request.ContainerId), checkpointContainerHostnameFileName)
		err = os.WriteFile(containerHostnamePath, []byte(containerHostname), 0644)
		if err != nil {
			return nil, err
		}
	}

	var volumeCacheMap map[string]string = make(map[string]string)

	// Add bind mounts to runc spec
	for _, m := range request.Mounts {
		// Skip mountpoint storage if the local path does not exist (mounting failed)
		if m.MountType == storage.StorageModeMountPoint {
			if _, err := os.Stat(m.LocalPath); os.IsNotExist(err) {
				continue
			}
		} else {
			if strings.HasPrefix(m.MountPath, types.WorkerContainerVolumePath) {
				volumeCacheMap[filepath.Base(m.MountPath)] = m.LocalPath
			}

			err := os.MkdirAll(m.LocalPath, 0755)
			if err != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("failed to create mount directory: %v", err)
				continue
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
	if request.VolumeCacheCompatible() && s.fileCacheManager.CacheAvailable() {
		err = s.fileCacheManager.EnableVolumeCaching(request.Workspace.Name, volumeCacheMap, spec)
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to setup volume caching: %v", err)
		}
	}

	// Configure resolv.conf
	resolvMount := specs.Mount{
		Type:        "none",
		Source:      "/workspace/etc/resolv.conf",
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

	// Add back tmpfs pod/sandbox mounts from initial spec if they exist
	if request.Stub.Type.Kind() == types.StubTypePod || request.Stub.Type.Kind() == types.StubTypeSandbox {
		for _, m := range options.InitialSpec.Mounts {
			if m.Source == "none" && m.Type == "tmpfs" {
				spec.Mounts = append(spec.Mounts, m)
			}
		}
	}

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
		fmt.Sprintf("CONTAINER_HOSTNAME=%s", fmt.Sprintf("%s:%d", s.podAddr, options.BindPorts[0])),
		fmt.Sprintf("CONTAINER_ID=%s", request.ContainerId),
		fmt.Sprintf("BETA9_GATEWAY_HOST=%s", os.Getenv("BETA9_GATEWAY_HOST")),
		fmt.Sprintf("BETA9_GATEWAY_PORT=%s", os.Getenv("BETA9_GATEWAY_PORT")),
		fmt.Sprintf("BETA9_GATEWAY_HOST_HTTP=%s", os.Getenv("BETA9_GATEWAY_HOST_HTTP")),
		fmt.Sprintf("BETA9_GATEWAY_PORT_HTTP=%s", os.Getenv("BETA9_GATEWAY_PORT_HTTP")),
		fmt.Sprintf("STORAGE_AVAILABLE=%t", request.StorageAvailable()),
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
		PoolName:    s.poolName,
		PodHostname: s.podHostName,
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

		resp, err := handleGRPCResponse(s.containerRepoClient.GetContainerState(context.Background(), &pb.GetContainerStateRequest{ContainerId: containerId}))
		if err != nil {
			notFoundErr := &types.ErrContainerStateNotFound{}

			if notFoundErr.From(err) {
				log.Warn().Str("container_id", containerId).Msg("container state not found, returning")
				return
			}
		} else if resp.State.Status == string(types.ContainerStatusStopping) {
			log.Warn().Str("container_id", containerId).Msg("container should be stopping, force killing")
			s.stopContainer(containerId, true)
			return
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
	}()

	// Setup container overlay filesystem
	err := containerInstance.Overlay.Setup()
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to setup overlay: %v", err)
		return
	}
	defer containerInstance.Overlay.Cleanup()

	spec.Root.Readonly = false
	spec.Root.Path = containerInstance.Overlay.TopLayerPath()

	// Setup container network namespace / devices
	err = s.containerNetworkManager.Setup(containerId, spec, request)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to setup container network: %v", err)
		return
	}

	if request.RequiresGPU() {
		// Assign n-number of GPUs to a container
		assignedDevices, err := s.containerGPUManager.AssignGPUDevices(request.ContainerId, request.GpuCount)
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to assign GPUs: %v", err)
			return
		}

		cdiCache := cdi.GetDefaultCache()

		var devicesToInject []string
		for _, device := range assignedDevices {
			devicePath := fmt.Sprintf("%s=%d", nvidiaDeviceKindPrefix, device)
			devicesToInject = append(devicesToInject, devicePath)
		}

		unresolvable, err := cdiCache.InjectDevices(spec, devicesToInject...)
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to inject devices: %v", err)
			return
		}
		if len(unresolvable) > 0 {
			log.Error().Str("container_id", request.ContainerId).Msgf("unresolvable devices: %v", unresolvable)
			return
		}
	}

	// Expose the bind ports
	for idx, bindPort := range opts.BindPorts {
		err = s.containerNetworkManager.ExposePort(containerId, bindPort, int(request.Ports[idx]))
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to expose container bind port: %v", err)
			return
		}
	}

	// Write runc config spec to disk
	configContents, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return
	}

	configPath := filepath.Join(spec.Root.Path, specBaseName)
	err = os.WriteFile(configPath, configContents, 0644)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to write runc config: %v", err)
		return
	}
	request.ConfigPath = configPath

	outputWriter := containerInstance.OutputWriter

	// Log metrics
	go s.workerUsageMetrics.EmitContainerUsage(ctx, request)
	go s.eventRepo.PushContainerStartedEvent(containerId, s.workerId, request)
	defer func() { go s.eventRepo.PushContainerStoppedEvent(containerId, s.workerId, request, exitCode) }()

	startedChan := make(chan int, 1)
	checkpointPIDChan := make(chan int, 1)
	monitorPIDChan := make(chan int, 1)

	defer func() {
		// Close in reverse order of dependency
		close(checkpointPIDChan)
		close(monitorPIDChan)
		close(startedChan)
	}()

	go func() {
		// When the process starts monitor it and potentially checkpoint it
		pid, ok := <-startedChan
		if !ok {
			return
		}

		monitorPIDChan <- pid
		checkpointPIDChan <- pid
	}()

	isOOMKilled := atomic.Bool{}
	go func() {
		pid := <-monitorPIDChan
		go s.collectAndSendContainerMetrics(ctx, request, spec, pid)  // Capture resource usage (cpu/mem/gpu)
		go s.watchOOMEvents(ctx, request, outputLogger, &isOOMKilled) // Watch for OOM events
	}()

	exitCode, _ = s.runContainer(ctx, request, outputLogger, outputWriter, startedChan, checkpointPIDChan)

	stopReason := types.StopContainerReasonUnknown
	containerInstance, exists = s.containerInstances.Get(containerId)
	if exists {
		stopReason = types.StopContainerReason(containerInstance.StopReason)
	}

	switch stopReason {
	case types.StopContainerReasonScheduler:
		exitCode = int(types.ContainerExitCodeScheduler)
	case types.StopContainerReasonTtl:
		exitCode = int(types.ContainerExitCodeTtl)
	case types.StopContainerReasonUser:
		exitCode = int(types.ContainerExitCodeUser)
	case types.StopContainerReasonAdmin:
		exitCode = int(types.ContainerExitCodeAdmin)
	default:
		if isOOMKilled.Load() {
			exitCode = int(types.ContainerExitCodeOomKill)
		} else if exitCode == int(types.ContainerExitCodeOomKill) || exitCode == -1 {
			// Exit code will match OOM kill exit code, but container was not OOM killed so override it
			exitCode = 0
		}
	}

	log.Info().Str("container_id", containerId).Msgf("container has exited with code: %d, stop reason: %s", exitCode, stopReason)
	outputLogger.Info("", "done", true, "success", exitCode == 0)
	if containerId != "" {
		err = s.runcHandle.Delete(s.ctx, containerId, &runc.DeleteOpts{Force: true})
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to delete container: %v", err)
		}
	}
}

func (s *Worker) runContainer(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter *common.OutputWriter, startedChan chan int, checkpointPIDChan chan int) (int, error) {
	// Handle automatic checkpoint creation if applicable
	// (This occurs when checkpoint_enabled is true and an existing checkpoint is not available)
	if s.IsCRIUAvailable(request.GpuCount) && request.CheckpointEnabled {
		go s.attemptAutoCheckpoint(ctx, request, outputLogger, outputWriter, startedChan, checkpointPIDChan)
	}

	// Handle restore from checkpoint if available
	if s.IsCRIUAvailable(request.GpuCount) && request.Checkpoint != nil {
		if request.Checkpoint != nil && request.Checkpoint.Status == string(types.CheckpointStatusAvailable) {
			checkpointPath := s.checkpointPath(request.Checkpoint.CheckpointId)

			err := copyDirectory(filepath.Join(checkpointPath, checkpointFsDir), filepath.Dir(request.ConfigPath), []string{})
			if err != nil {
				log.Error().Str("container_id", request.ContainerId).Msgf("failed to copy checkpoint directory: %v", err)
			}
		}

		exitCode, restored, err := s.attemptRestoreCheckpoint(ctx, request, outputLogger, outputWriter, startedChan, checkpointPIDChan)
		if restored {
			return exitCode, err
		}

		// If this is not a deployment stub, don't fall back to running the container
		if !restored && !request.Stub.Type.IsDeployment() {
			return exitCode, err
		} else if !restored {
			// Disable checkpoing flag if the restore fails
			request.CheckpointEnabled = false
		}
	}

	if request.CheckpointEnabled {
		err := addEnvToSpec(request.ConfigPath, []string{fmt.Sprintf("CHECKPOINT_ENABLED=%t", request.CheckpointEnabled && s.IsCRIUAvailable(request.GpuCount))})
		if err != nil {
			log.Warn().Str("container_id", request.ContainerId).Msgf("failed to add checkpoint env var to spec: %v", err)
		}

	}

	bundlePath := filepath.Dir(request.ConfigPath)
	exitCode, err := s.runcHandle.Run(ctx, request.ContainerId, bundlePath, &runc.CreateOpts{
		OutputWriter: outputWriter,
		Started:      startedChan,
	})
	if err != nil {
		log.Warn().Str("container_id", request.ContainerId).Err(err).Msgf("error running container from bundle, exit code %d", exitCode)
	}

	return exitCode, err
}

func (s *Worker) createOverlay(request *types.ContainerRequest, bundlePath string) *common.ContainerOverlay {
	// For images that have a rootfs path, set that as the root path
	// otherwise, assume OCI spec files are in the root
	rootPath := filepath.Join(bundlePath, "rootfs")
	if _, err := os.Stat(rootPath); os.IsNotExist(err) {
		rootPath = bundlePath
	}

	overlayPath := baseConfigPath
	if request.IsBuildRequest() {
		overlayPath = "/dev/shm"
	}

	return common.NewContainerOverlay(request, rootPath, overlayPath)
}

func (s *Worker) watchOOMEvents(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, isOOMKilled *atomic.Bool) {
	var (
		seenEvents  = make(map[string]struct{})
		ch          <-chan *runc.Event
		err         error
		ticker      = time.NewTicker(time.Second)
		containerId = request.ContainerId
	)

	defer ticker.Stop()

	eventsCtx, cancelEvents := context.WithCancel(ctx)
	defer cancelEvents()

	select {
	case <-ctx.Done():
		return
	default:
		ch, err = s.runcHandle.Events(eventsCtx, containerId, time.Second)
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to open runc events channel: %v", err)
			return
		}
	}

	maxTries, tries := 10, 0
	baseBackoff := time.Second
	maxBackoff := time.Minute

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			seenEvents = make(map[string]struct{})
		case event, ok := <-ch:
			if !ok {
				if tries == maxTries {
					return
				}

				backoff := baseBackoff * time.Duration(1<<uint(tries))
				if backoff > maxBackoff {
					backoff = maxBackoff
				}

				tries++

				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					ch, err = s.runcHandle.Events(eventsCtx, containerId, time.Second)
					if err != nil {
						log.Error().Str("container_id", containerId).Msgf("failed to open runc events channel: %v", err)
					}
				}
				continue
			}

			if s.config.DebugMode {
				log.Info().Str("container_id", containerId).Msgf("received container event: %+v", event)
			}

			if event.Type == "error" && event.Err != nil && strings.Contains(event.Err.Error(), "file already closed") {
				log.Warn().Str("container_id", containerId).Msgf("received error event: %s, stopping OOM event monitoring", event.Err.Error())
				cancelEvents()
				return
			}

			if _, ok := seenEvents[event.Type]; ok {
				continue
			}

			seenEvents[event.Type] = struct{}{}

			if event.Type == "oom" {
				isOOMKilled.Store(true)
				outputLogger.Error("Container was killed due to out-of-memory conditions.")
				s.eventRepo.PushContainerOOMEvent(containerId, s.workerId, request)
			}
		}
	}
}
