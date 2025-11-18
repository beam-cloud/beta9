package worker

import (
	"context"
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
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	goproc "github.com/beam-cloud/goproc/pkg"
	"tags.cncf.io/container-device-interface/pkg/cdi"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	baseConfigPath            string = "/tmp"
	defaultContainerDirectory string = types.WorkerUserCodeVolume
	specBaseName              string = "config.json"
	initialSpecBaseName       string = "initial_config.json"
	containerInnerPort        int    = 8001 // Use a fixed port inside the container
)

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

// stopContainer stops a container. When force is true, a SIGKILL signal is sent to the container.
func (s *Worker) stopContainer(containerId string, kill bool) error {
	log.Info().Str("container_id", containerId).Msg("stopping container")

	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		log.Info().Str("container_id", containerId).Msg("container not found")
		return nil
	}

	signal := syscall.SIGTERM
	if kill {
		signal = syscall.SIGKILL
	}

	// Use the runtime that was selected for this container
	rt := instance.Runtime
	if rt == nil {
		rt = s.runtime
	}

	// Kill the container - this will cause the runtime.Run() call to exit
	// which triggers all the defers (overlay cleanup, mount cleanup, etc.)
	err := rt.Kill(context.Background(), instance.Id, signal, &runtime.KillOpts{All: true})
	if err != nil {
		log.Debug().Str("container_id", containerId).Err(err).Msg("error killing container (may already be stopped)")
	}

	log.Info().Str("container_id", containerId).Msg("container stop signal sent")
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

	// Tear down container network components - best effort
	if err := s.containerNetworkManager.TearDown(request.ContainerId); err != nil {
		log.Warn().Str("container_id", request.ContainerId).Err(err).Msg("failed to clean up container network")
	}

	// Clean up upload directory
	os.RemoveAll(filepath.Join(types.WorkerContainerUploadsHostPath, containerId))

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
		instance, exists := s.containerInstances.Get(containerId)
		if exists && instance.Runtime != nil {
			container, err := instance.Runtime.State(context.TODO(), containerId)
			if err == nil && container.Status == types.RuncContainerStatusRunning {
				if err := s.stopContainer(containerId, true); err != nil {
					log.Error().Str("container_id", containerId).Msgf("failed to stop container: %v", err)
				}
			}

			// Stop OOM watcher
			if instance.OOMWatcher != nil {
				instance.OOMWatcher.Stop()
			}
		}

		s.deleteContainer(containerId)

		log.Info().Str("container_id", containerId).Msg("finalized container shutdown")
	}()
}

func (s *Worker) deleteContainer(containerId string) {
	s.containerInstances.Delete(containerId)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := handleGRPCResponse(s.containerRepoClient.DeleteContainerState(ctx, &pb.DeleteContainerStateRequest{ContainerId: containerId}))
	if err != nil {
		log.Debug().Str("container_id", containerId).Err(err).Msg("failed to remove remote container state")
	}
}

// Spawn a single container and stream output to stdout/stderr
func (s *Worker) RunContainer(ctx context.Context, request *types.ContainerRequest) error {
	containerId := request.ContainerId

	caps := s.runtime.Capabilities()

	// Gate features based on runtime capabilities
	if request.CheckpointEnabled && !caps.CheckpointRestore {
		log.Info().Str("container_id", containerId).
			Str("runtime", s.runtime.Name()).
			Msg("disabling checkpoint for runtime without CRIU support")

		request.CheckpointEnabled = false
		request.Checkpoint = nil
	}

	if request.RequiresGPU() && !caps.GPU {
		return fmt.Errorf("runtime %s does not support GPU workloads", s.runtime.Name())
	}

	s.containerInstances.Set(containerId, &ContainerInstance{
		Id:        containerId,
		StubId:    request.StubId,
		LogBuffer: common.NewLogBuffer(),
		Request:   request,
		Runtime:   s.runtime,
	})

	bundlePath := filepath.Join(s.imageMountPath, request.ImageId)

	// Set worker hostname
	hostname := fmt.Sprintf("%s:%d", s.podAddr, s.containerServer.port)
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

	// Clip v2 build short-circuit: For v2 builds, the image was already built via buildah
	// (see buildOrPullBaseImage) and indexed as a .clip archive. We don't need to run a
	// runc container or execute any commands inside it. Mark the build as successful and exit.
	if request.IsBuildRequest() && s.config.ImageService.ClipVersion == uint32(types.ClipVersion2) {
		exitCode := 0
		_, _ = handleGRPCResponse(s.containerRepoClient.SetContainerExitCode(context.Background(), &pb.SetContainerExitCodeRequest{
			ContainerId: containerId,
			ExitCode:    int32(exitCode),
		}))
		s.containerWg.Add(1)
		go func() {
			s.finalizeContainer(containerId, request, &exitCode)
		}()
		return nil
	}

	// Determine how many ports we need to expose
	portsToExpose := len(request.Ports)
	if portsToExpose == 0 {
		portsToExpose = 1
		request.Ports = []uint32{uint32(containerInnerPort)}
	}

	// Expose SSH port
	request.Ports = append(request.Ports, uint32(types.WorkerShellPort))
	portsToExpose++

	// Expose sandbox process manager port
	if request.Stub.Type.Kind() == types.StubTypeSandbox {
		request.Ports = append(request.Ports, uint32(types.WorkerSandboxProcessManagerPort))
		portsToExpose++
	}

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

	// Read spec from bundle; guard against empty image IDs
	if request.ImageId == "" {
		return fmt.Errorf("empty image id in request")
	}
	initialBundleSpec, _ := s.readBundleConfig(request)

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
	// For Clip v2 builds, the Dockerfile is rendered by the builder with all build steps.
	// Build via buildah if a non-empty Dockerfile is present (contains RUN commands for actual builds).
	if request.BuildOptions.Dockerfile != nil && *request.BuildOptions.Dockerfile != "" {
		log.Info().Str("container_id", containerId).Msg("building image from Dockerfile")
		return s.imageClient.BuildAndArchiveImage(ctx, outputLogger, request)
	}

	// Fallback: pull the source image and archive it
	if request.BuildOptions.SourceImage != nil {
		log.Info().Str("container_id", containerId).Msgf("pulling source image: %s", *request.BuildOptions.SourceImage)
		return s.imageClient.PullAndArchiveImage(ctx, outputLogger, request)
	}

	return nil
}

func (s *Worker) readBundleConfig(request *types.ContainerRequest) (*specs.Spec, error) {
	imageConfigPath := filepath.Join(s.imageMountPath, request.ImageId, initialSpecBaseName)
	if request.IsBuildRequest() {
		imageConfigPath = filepath.Join(s.imageMountPath, request.ImageId, specBaseName)
	}

	data, err := os.ReadFile(imageConfigPath)
	if err != nil {
		// For v2 images, there's no pre-baked config.json in the mounted root.
		// Derive the spec from CLIP metadata embedded in the archive.
		if os.IsNotExist(err) {
			log.Info().Str("image_id", request.ImageId).Msg("no bundle config found, deriving from v2 image metadata")
			return s.deriveSpecFromV2Image(request)
		}
		log.Error().Str("image_id", request.ImageId).Str("image_config_path", imageConfigPath).Err(err).Msg("failed to read bundle config")
		return nil, err
	}

	specTemplate := strings.TrimSpace(string(data))
	var spec specs.Spec

	err = json.Unmarshal([]byte(specTemplate), &spec)
	if err != nil {
		log.Error().Str("image_id", request.ImageId).Str("image_config_path", imageConfigPath).Err(err).Msg("failed to unmarshal bundle config")
		return nil, err
	}

	return &spec, nil
}

// deriveSpecFromV2Image creates an OCI spec from v2 image metadata.
// This is ONLY used for v2 images where we don't have an unpacked bundle with config.json.
// V1 images always have a config.json, so if we're here, it's a v2 image.
func (s *Worker) deriveSpecFromV2Image(request *types.ContainerRequest) (*specs.Spec, error) {
	clipMeta, ok := s.imageClient.GetCLIPImageMetadata(request.ImageId)
	if !ok {
		log.Warn().
			Str("image_id", request.ImageId).
			Msg("no metadata found in v2 image archive, using base spec")
		return nil, nil
	}

	log.Info().
		Str("image_id", request.ImageId).
		Msg("using metadata from v2 clip archive")

	return s.buildSpecFromCLIPMetadata(clipMeta), nil
}

// buildSpecFromCLIPMetadata constructs an OCI spec from CLIP image metadata
// This is the primary path for v2 images with embedded metadata
func (s *Worker) buildSpecFromCLIPMetadata(clipMeta *clipCommon.ImageMetadata) *specs.Spec {
	spec := specs.Spec{
		Process: &specs.Process{
			Env: []string{},
		},
	}

	// CLIP metadata has a flat structure with all fields at the top level
	if len(clipMeta.Env) > 0 {
		spec.Process.Env = clipMeta.Env
	}
	if clipMeta.WorkingDir != "" {
		spec.Process.Cwd = clipMeta.WorkingDir
	}
	if clipMeta.User != "" {
		spec.Process.User.Username = clipMeta.User
	}
	// Combine Entrypoint and Cmd, or use Cmd alone
	if len(clipMeta.Entrypoint) > 0 {
		spec.Process.Args = append(clipMeta.Entrypoint, clipMeta.Cmd...)
	} else if len(clipMeta.Cmd) > 0 {
		spec.Process.Args = clipMeta.Cmd
	}

	return &spec
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

	if request.Stub.Type.Kind() == types.StubTypePod && options.InitialSpec != nil {
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

		if s.config.Worker.ContainerResourceLimits.CPUEnforced || s.config.Worker.ContainerResourceLimits.MemoryEnforced {
			resources, err := s.getContainerResources(request)
			if err != nil {
				return nil, err
			}
			if resources.CPU != nil {
				spec.Linux.Resources.CPU = resources.CPU
			}
			if resources.Memory != nil {
				spec.Linux.Resources.Memory = resources.Memory
			}
		}
	}

	env := s.getContainerEnvironment(request, options)
	// Only inject GPU env vars if using CDI (runc)
	// For gVisor, env vars are set manually in the GPU device injection logic
	if request.Gpu != "" && s.runtime.Capabilities().CDI {
		env = s.containerGPUManager.InjectEnvVars(env)
	}

	// Environment is already assembled in getContainerEnvironment (includes InitialSpec.Env if present)
	spec.Process.Env = env

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

	// External mount for gVisor file uploads (external mounts bypass directory caching)
	uploadsPath := filepath.Join(types.WorkerContainerUploadsHostPath, request.ContainerId)
	if err := os.MkdirAll(uploadsPath, 0755); err == nil {
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "none",
			Source:      uploadsPath,
			Destination: types.WorkerContainerUploadsMountPath,
			Options:     []string{"rbind", "rw"},
		})
	}

	// Add back tmpfs pod/sandbox mounts from initial spec if they exist
	if (request.Stub.Type.Kind() == types.StubTypePod || request.Stub.Type.Kind() == types.StubTypeSandbox) && options.InitialSpec != nil {
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
	// Get the appropriate base config for the runtime
	baseConfig := runtime.GetBaseConfig(s.runtime.Name())
	specTemplate := strings.TrimSpace(baseConfig)
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
	var err error
	err = containerInstance.Overlay.Setup()
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

	// Only inject GPU devices if runtime supports GPU
	if request.RequiresGPU() && s.runtime.Capabilities().GPU {
		// Assign n-number of GPUs to a container
		assignedDevices, err := s.containerGPUManager.AssignGPUDevices(request.ContainerId, request.GpuCount)
		if err != nil {
			log.Error().Str("container_id", request.ContainerId).Msgf("failed to assign GPUs: %v", err)
			return
		}

		// Only use CDI if runtime supports it
		if s.runtime.Capabilities().CDI {
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

			// Debug: Log what CDI injected for troubleshooting
			var devicePaths []string
			for _, dev := range spec.Linux.Devices {
				devicePaths = append(devicePaths, dev.Path)
			}
			var mountSources []string
			for _, mnt := range spec.Mounts {
				if strings.Contains(mnt.Source, "nvidia") || strings.Contains(mnt.Source, "cuda") {
					mountSources = append(mountSources, mnt.Source)
				}
			}
			var envVars []string
			for _, env := range spec.Process.Env {
				if strings.Contains(env, "NVIDIA") || strings.Contains(env, "CUDA") {
					envVars = append(envVars, env)
				}
			}
			log.Info().
				Str("container_id", request.ContainerId).
				Strs("devices_before_filtering", devicePaths).
				Strs("gpu_mounts", mountSources).
				Strs("gpu_env_vars", envVars).
				Interface("cdi_annotations", spec.Annotations).
				Msg("CDI injection complete - gVisor will filter unsupported devices")
		} else {
			// For gVisor: DO NOT inject device nodes - nvproxy creates them internally!
			// But we still need to mount nvidia-smi and CUDA libraries from the host
			
			// Mount nvidia-smi binary if it exists on the host
			nvidiaSmiBinary := "/usr/bin/nvidia-smi"
			if _, err := os.Stat(nvidiaSmiBinary); err == nil {
				spec.Mounts = append(spec.Mounts, specs.Mount{
					Type:        "bind",
					Source:      nvidiaSmiBinary,
					Destination: nvidiaSmiBinary,
					Options:     []string{"ro", "rbind", "rprivate", "nosuid", "nodev"},
				})
			}
			
			// CRITICAL: Set NVIDIA_VISIBLE_DEVICES to control which GPUs are visible
			// This tells gVisor's nvproxy which GPUs to expose inside the sandbox
			// Format: comma-separated list of GPU indices (e.g., "0" or "0,1")
			var gpuIDStrs []string
			for _, gpuID := range assignedDevices {
				gpuIDStrs = append(gpuIDStrs, fmt.Sprintf("%d", gpuID))
			}
			visibleDevices := strings.Join(gpuIDStrs, ",")
			
			// Set NVIDIA_VISIBLE_DEVICES environment variable
			spec.Process.Env = append(spec.Process.Env, fmt.Sprintf("NVIDIA_VISIBLE_DEVICES=%s", visibleDevices))
			
			// Also set NVIDIA_DRIVER_CAPABILITIES for completeness
			spec.Process.Env = append(spec.Process.Env, "NVIDIA_DRIVER_CAPABILITIES=compute,utility,graphics,video")
			
			log.Info().
				Str("container_id", request.ContainerId).
				Ints("gpu_ids", assignedDevices).
				Str("visible_devices", visibleDevices).
				Msg("Set GPU env vars for gVisor nvproxy (nvproxy creates devices internally)")
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

	// Modify sandbox entry point to point to process manager binary
	if request.Stub.Type.Kind() == types.StubTypeSandbox {
		instance, exists := s.containerInstances.Get(containerId)
		if !exists {
			log.Error().Str("container_id", containerId).Msg("instance not found")
			return
		}

		instance.SandboxProcessManager, err = goproc.NewGoProcClient(ctx, instance.ContainerIp, uint(types.WorkerSandboxProcessManagerPort))
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to create sandbox process manager client: %v", err)
			return
		}

		instance.SandboxProcessManagerReady = false
		s.containerInstances.Set(containerId, instance)

		spec.Process.Args = []string{types.WorkerSandboxProcessManagerContainerPath}
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Type:        "bind",
			Source:      types.WorkerSandboxProcessManagerWorkerPath,
			Destination: types.WorkerSandboxProcessManagerContainerPath,
			Options: []string{
				"ro",
				"rbind",
				"rprivate",
				"nosuid",
				"nodev",
			},
		})
	}

	// Add Docker capabilities if enabled for sandbox containers with gVisor
	if request.DockerEnabled && request.Stub.Type.Kind() == types.StubTypeSandbox && s.runtime.Name() == types.ContainerRuntimeGvisor.String() {
		if runscRuntime, ok := s.runtime.(*runtime.Runsc); ok {
			runscRuntime.AddDockerInDockerCapabilities(spec)
			log.Info().Str("container_id", containerId).Msg("added docker capabilities for sandbox container")
		}
	}

	// Prepare spec for the selected runtime
	log.Info().Str("container_id", containerId).Str("runtime", s.runtime.Name()).Msg("calling runtime.Prepare()")
	if err := s.runtime.Prepare(ctx, spec); err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to prepare spec for runtime: %v", err)
		return
	}
	log.Info().Str("container_id", containerId).Msg("runtime.Prepare() completed successfully")

	// Write container config spec to disk
	configContents, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return
	}

	configPath := filepath.Join(spec.Root.Path, specBaseName)
	err = os.WriteFile(configPath, configContents, 0644)
	if err != nil {
		log.Error().Str("container_id", containerId).Msgf("failed to write container config: %v", err)
		return
	}
	request.ConfigPath = configPath
	
	// Log the final spec for debugging (truncated to reasonable size)
	specPreview := string(configContents)
	maxLen := 3000
	if len(specPreview) > maxLen {
		specPreview = specPreview[:maxLen] + "... (truncated)"
	}
	log.Debug().
		Str("container_id", containerId).
		Str("config_path", configPath).
		Str("spec_preview", specPreview).
		Msg("Written OCI spec to disk")

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

		// For sandboxes, wait for process manager to be ready before allowing exec calls
		if request.Stub.Type.Kind() == types.StubTypeSandbox {
			instance, exists := s.containerInstances.Get(containerId)
			if exists && instance.SandboxProcessManager != nil {

				// Wait for process manager to be ready - this blocks until ready or timeout
				if s.waitForProcessManager(ctx, containerId, instance) {
					instance.SandboxProcessManagerReady = true
					s.containerInstances.Set(containerId, instance)

					// Now that process manager is ready, start Docker daemon if enabled
					if request.DockerEnabled {
						go s.startDockerDaemon(ctx, containerId, instance)
					}

				} else {
					log.Error().Str("container_id", containerId).Msg("failed to initialize process manager - sandbox may not be functional")
				}
			}
		}
	}()

	isOOMKilled := atomic.Bool{}
	go func() {
		pid := <-monitorPIDChan
		go s.collectAndSendContainerMetrics(ctx, request, spec, pid)
		s.setupOOMWatcher(ctx, containerId, pid, spec, request, outputLogger, &isOOMKilled)
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
		// Check for OOM kill and ensure exit code is 137 for both runc and gVisor
		if isOOMKilled.Load() {
			exitCode = int(types.ContainerExitCodeOomKill) // 137
			log.Info().Str("container_id", containerId).Int("exit_code", exitCode).Msg("overriding exit code to 137 for OOM kill")
		} else if exitCode == int(types.ContainerExitCodeOomKill) || exitCode == -1 {
			// Exit code will match OOM kill exit code, but container was not OOM killed so override it
			exitCode = 0
		}
	}

	log.Info().Str("container_id", containerId).Msgf("container has exited with code: %d, stop reason: %s", exitCode, stopReason)
	outputLogger.Info("", "done", true, "success", exitCode == 0)
	if containerId != "" {
		instance, exists := s.containerInstances.Get(containerId)
		rt := s.runtime
		if exists && instance.Runtime != nil {
			rt = instance.Runtime
		}

		err = rt.Delete(s.ctx, containerId, &runtime.DeleteOpts{Force: true})
		if err != nil {
			log.Error().Str("container_id", containerId).Msgf("failed to delete container: %v", err)
		}
	}
}

func (s *Worker) runContainer(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger, outputWriter *common.OutputWriter, startedChan chan int, checkpointPIDChan chan int) (int, error) {
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if !exists {
		return -1, fmt.Errorf("container instance not found")
	}

	supportsCheckpoint := instance.Runtime.Capabilities().CheckpointRestore && s.IsCRIUAvailable(request.GpuCount)

	// Handle automatic checkpoint creation if applicable
	// (This occurs when checkpoint_enabled is true and an existing checkpoint is not available)
	if supportsCheckpoint && request.CheckpointEnabled {
		go s.attemptAutoCheckpoint(ctx, request, outputLogger, outputWriter, startedChan, checkpointPIDChan)
	}

	// Handle restore from checkpoint if available
	if supportsCheckpoint && request.Checkpoint != nil {
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
	exitCode, err := instance.Runtime.Run(ctx, request.ContainerId, bundlePath, &runtime.RunOpts{
		OutputWriter:  outputWriter,
		Started:       startedChan,
		DockerEnabled: request.DockerEnabled,
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

func (s *Worker) getContainerResources(request *types.ContainerRequest) (*specs.LinuxResources, error) {
	var resources ContainerResources

	// Get runtime for this container
	instance, exists := s.containerInstances.Get(request.ContainerId)
	if exists && instance.Runtime != nil && instance.Runtime.Name() == types.ContainerRuntimeGvisor.String() {
		resources = NewGvisorResources()
	} else {
		resources = NewRuncResources()
	}

	return &specs.LinuxResources{
		CPU:    resources.GetCPU(request),
		Memory: resources.GetMemory(request),
	}, nil
}
