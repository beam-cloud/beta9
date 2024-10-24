package worker

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	types "github.com/beam-cloud/beta9/pkg/types"
	runc "github.com/beam-cloud/go-runc"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	baseConfigPath            string = "/tmp"
	defaultContainerDirectory string = "/mnt/code"

	exitCodeSigterm int = 143 // Means the container received a SIGTERM by the underlying operating system
	exitCodeSigkill int = 137 // Means the container received a SIGKILL by the underlying operating system
)

var (
	//go:embed base_runc_config.json
	baseRuncConfigRaw string
)

// handleStopContainerEvent used by the event bus to stop a container.
// Containers are stopped by sending a stop container event to stopContainerChan.
func (s *Worker) handleStopContainerEvent(event *common.Event) bool {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	containerId := event.Args["container_id"].(string)

	if _, exists := s.containerInstances.Get(containerId); exists {
		log.Printf("<%s> - received stop container event.\n", containerId)
		s.stopContainerChan <- stopContainerEvent{ContainerId: containerId, Kill: false}
	}

	return true
}

// stopContainer stops a runc container.
// It will remove the container state if the contaienr is forcefully killed.
// Otherwise, the container state will be removed after the termination grace period in clearContainer().
func (s *Worker) stopContainer(containerId string, kill bool) error {
	log.Printf("<%s> - stopping container.\n", containerId)

	if _, exists := s.containerInstances.Get(containerId); !exists {
		log.Printf("<%s> - container not found.\n", containerId)
		return nil
	}

	signal := int(syscall.SIGTERM)
	if kill {
		signal = int(syscall.SIGKILL)
		s.containerRepo.DeleteContainerState(containerId)
	}

	err := s.runcHandle.Kill(context.Background(), containerId, signal, &runc.KillOpts{All: true})
	if err != nil {
		log.Printf("<%s> - unable to stop container: %v\n", containerId, err)

		if strings.Contains(err.Error(), "container does not exist") {
			s.containerNetworkManager.TearDown(containerId)
			return nil
		}

		return err
	}

	log.Printf("<%s> - container stopped.\n", containerId)
	return nil
}

func (s *Worker) finalizeContainer(containerId string, request *types.ContainerRequest, exitCode *int) {
	defer s.containerWg.Done()

	if *exitCode < 0 {
		*exitCode = 1
	} else if *exitCode == exitCodeSigterm {
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
		err := s.containerRepo.DeleteContainerState(containerId)
		if err != nil {
			log.Printf("<%s> - failed to remove container state: %v\n", containerId, err)
		}
	}()
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

	if s.checkpointingAvailable {
		go s.createCheckpoint(request)
	}

	log.Printf("<%s> - spawned successfully.\n", containerId)
	return nil
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

	if s.config.Worker.RunCResourcesEnforced {
		spec.Linux.Resources.CPU = getLinuxCPU(request)
		spec.Linux.Resources.Memory = getLinuxMemory(request)
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

	// We need to modify the spec to support Cedana C/R
	if s.checkpointingAvailable {
		s.cedanaClient.prepareContainerSpec(spec, request.Gpu != "")
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
				log.Printf("<%s> - unable to symlink volume: %v", request.ContainerId, err)
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
	if s.fileCacheManager.CacheAvailable() && request.Workspace.VolumeCacheEnabled {
		err = s.fileCacheManager.EnableVolumeCaching(request.Workspace.Name, volumeCacheMap, spec)
		if err != nil {
			log.Printf("<%s> - failed to setup volume caching: %v", request.ContainerId, err)
		}
	}

	if s.checkpointingAvailable && request.CheckpointEnabled {
		os.MkdirAll(checkpointDir(request.ContainerId), os.ModePerm)

		checkpointMount := specs.Mount{
			Type:        "bind",
			Source:      checkpointDir(request.ContainerId),
			Destination: "/cedana",
			Options: []string{
				"rbind",
				"rprivate",
				"nosuid",
				"nodev"},
		}
		spec.Mounts = append(spec.Mounts, checkpointMount)
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

// spawn a container using runc binary
func (s *Worker) spawn(request *types.ContainerRequest, spec *specs.Spec, outputChan chan common.OutputMsg, opts *ContainerOptions) {
	ctx, cancel := context.WithCancel(s.ctx)

	s.workerRepo.AddContainerToWorker(s.workerId, request.ContainerId)
	defer s.workerRepo.RemoveContainerFromWorker(s.workerId, request.ContainerId)
	defer cancel()

	exitCode := -1
	containerId := request.ContainerId

	// Unmount external s3 buckets
	defer s.containerMountManager.RemoveContainerMounts(containerId)

	// Cleanup container state and resources
	defer s.finalizeContainer(containerId, request, &exitCode)

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
	s.containerRepo.SetWorkerAddress(containerId, hostname)

	// Handle stdout/stderr from spawned container
	go s.containerLogger.CaptureLogs(containerId, outputChan)

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
		return
	}
	defer containerInstance.Overlay.Cleanup()
	spec.Root.Path = containerInstance.Overlay.TopLayerPath()

	// Setup container network namespace / devices
	err = s.containerNetworkManager.Setup(containerId, spec)
	if err != nil {
		log.Printf("<%s> failed to setup container network: %v", containerId, err)
		return
	}

	// Expose the bind port
	err = s.containerNetworkManager.ExposePort(containerId, opts.BindPort, opts.BindPort)
	if err != nil {
		log.Printf("<%s> failed to expose container bind port: %v", containerId, err)
		return
	}

	// Write runc config spec to disk
	configContents, err := json.MarshalIndent(spec, "", " ")
	if err != nil {
		return
	}

	configPath := filepath.Join(baseConfigPath, containerId, "config.json")
	err = os.WriteFile(configPath, configContents, 0644)
	if err != nil {
		return
	}

	// Log metrics
	go s.workerMetrics.EmitContainerUsage(ctx, request)
	go s.eventRepo.PushContainerStartedEvent(containerId, s.workerId, request)
	defer func() { go s.eventRepo.PushContainerStoppedEvent(containerId, s.workerId, request) }()

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

// isBuildRequest checks if the sourceImage field is not-nil, which means the container request is for a build container
func (s *Worker) isBuildRequest(request *types.ContainerRequest) bool {
	return request.SourceImage != nil
}

// Waits for the endpoint to be ready to checkpoint at the desired point in execution, ie.
// after all endpoint workers have reached a checkpointable state. /health is configured to
// return 200 only when all workers are ready.
func (s *Worker) createCheckpoint(request *types.ContainerRequest) {
	log.Printf("<%s> - waiting for container to be ready for checkpoint\n", request.ContainerId)

	start := time.Now()
	timeout := time.Duration(containerCheckpointTimeout)
	ready := false
	managing := false
	gpuEnabled := request.Gpu != ""

	for time.Since(start) < timeout {
		instance, exists := s.containerInstances.Get(request.ContainerId)
		if exists {
			// Start managing the container with Cedana
			if !managing {
				err := s.cedanaClient.Manage(s.ctx, instance.Id, gpuEnabled)
				if err == nil {
					managing = true
				} else {
					log.Printf("<%s> - cedana manage failed, container may not be started yet: %+v\n", instance.Id, err)
				}
			} else {
				// Check if the endpoint is ready for checkpoint by verifying the existence of the file
				if _, err := os.Stat(fmt.Sprintf("/tmp/%s/cedana/READY_FOR_CHECKPOINT", instance.Id)); err == nil {
					log.Printf("<%s> - endpoint is ready for checkpoint.\n", instance.Id)
					ready = true
					break
				}

				log.Printf("<%s> - endpoint not ready for checkpoint.\n", instance.Id)
			}
		} else {
			log.Printf("<%s> - container instance not found yet\n", request.ContainerId)
		}

		time.Sleep(time.Second)
	}

	if !ready {
		log.Printf("<%s> - checkpoint timed out after %s\n", request.ContainerId, time.Since(start))
		return
	}

	err := s.cedanaClient.Checkpoint(s.ctx, request.ContainerId)
	if err != nil {
		log.Printf("<%s> - cedana checkpoint failed: %+v\n", request.ContainerId, err)
		return
	}

	log.Printf("<%s> - checkpoint done\n", request.ContainerId)
}
