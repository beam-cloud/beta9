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
	baseConfigPath            string        = "/tmp"
	defaultContainerDirectory string        = "/mnt/code"
	specBaseName              string        = "config.json"
	initialSpecBaseName       string        = "initial_config.json"
	runcEventsInterval        time.Duration = 5 * time.Second
	containerInnerPort        int           = 8001 // XXX: Use fixed port in the container namespace for restores
	exitCodeSigterm           int           = 143  // Means the container received a SIGTERM by the underlying operating system
	exitCodeSigkill           int           = 137  // Means the container received a SIGKILL by the underlying operating system
)

//go:embed base_runc_config.json
var baseRuncConfigRaw string

// handleStopContainerEvent used by the event bus to stop a container.
// Containers are stopped by sending a stop container event to stopContainerChan.
func (s *Worker) handleStopContainerEvent(event *common.Event) bool {
	s.containerLock.Lock()
	defer s.containerLock.Unlock()

	stopArgs, err := types.ToStopContainerArgs(event.Args)
	if err != nil {
		log.Printf("failed to parse stop container args: %v\n", err)
		return false
	}

	if _, exists := s.containerInstances.Get(stopArgs.ContainerId); exists {
		log.Printf("<%s> - received stop container event.\n", stopArgs.ContainerId)
		s.stopContainerChan <- stopContainerEvent{ContainerId: stopArgs.ContainerId, Kill: stopArgs.Force}
	}

	return true
}

// stopContainer stops a runc container. When force is true, a SIGKILL signal is sent to the container.
func (s *Worker) stopContainer(containerId string, kill bool) error {
	log.Printf("<%s> - stopping container.\n", containerId)

	_, exists := s.containerInstances.Get(containerId)
	if !exists {
		log.Printf("<%s> - container not found.\n", containerId)
		return nil
	}

	signal := int(syscall.SIGTERM)
	if kill {
		signal = int(syscall.SIGKILL)
	}

	err := s.runcHandle.Kill(context.Background(), containerId, signal, &runc.KillOpts{All: true})
	if err != nil {
		log.Printf("<%s> - error stopping container: %v\n", containerId, err)
		s.containerNetworkManager.TearDown(containerId)
		return nil
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
		select {
		case <-time.After(time.Duration(s.config.Worker.TerminationGracePeriod) * time.Second):
		case <-s.ctx.Done():
		}

		// If the container is still running, stop it. This happens when a sigterm is detected.
		container, err := s.runcHandle.State(context.TODO(), containerId)
		if err == nil && container.Status == types.RuncContainerStatusRunning {
			if err := s.stopContainer(containerId, true); err != nil {
				log.Printf("<%s> - failed to stop container: %v\n", containerId, err)
			}
		}

		s.containerInstances.Delete(containerId)
		err = s.containerRepo.DeleteContainerState(containerId)
		if err != nil {
			log.Printf("<%s> - failed to remove container state: %v\n", containerId, err)
		}

		log.Printf("<%s> - finalized container shutdown.\n", containerId)
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

	log.Printf("<%s> - spawned successfully.\n", containerId)
	return nil
}

func (s *Worker) readBundleConfig(imageId string) (*specs.Spec, error) {
	imageConfigPath := filepath.Join(s.imageMountPath, imageId, initialSpecBaseName)

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

	// We need to modify the spec to support Cedana C/R if checkpointing is enabled
	if s.checkpointingAvailable && request.CheckpointEnabled {
		s.cedanaClient.PrepareContainerSpec(spec, request.ContainerId, request.Gpu != "")
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
				"nodev",
			},
		}
		spec.Mounts = append(spec.Mounts, checkpointMount)
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
		fmt.Sprintf("CHECKPOINT_ENABLED=%t", request.CheckpointEnabled && s.checkpointingAvailable),
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
		Request:   request,
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
	err = s.containerNetworkManager.ExposePort(containerId, opts.BindPort, containerInnerPort)
	if err != nil {
		log.Printf("<%s> failed to expose container bind port: %v", containerId, err)
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

	// TMP: Write last config to disk
	err = os.WriteFile("/tmp/last_config.json", configContents, 0644)
	if err != nil {
		return
	}

	consoleWriter, err := NewConsoleWriter(containerInstance.OutputWriter)
	if err != nil {
		log.Printf("<%s> - failed to create console writer: %v\n", containerId, err)
		return
	}

	// Log metrics
	go s.workerMetrics.EmitContainerUsage(ctx, request)
	go s.eventRepo.PushContainerStartedEvent(containerId, s.workerId, request)
	defer func() { go s.eventRepo.PushContainerStoppedEvent(containerId, s.workerId, request) }()

	pid := 0
	pidChan := make(chan int, 1)

	go func() {
		// Wait for runc to start the container
		pid = <-pidChan

		// Capture resource usage (cpu/mem/gpu)
		go s.collectAndSendContainerMetrics(ctx, request, spec, pid)

		// Watch for OOM events
		go s.watchOOMEvents(ctx, containerId, outputChan)
	}()

	// Handle checkpoint creation & restore	if applicable
	restored := false
	if request.CheckpointEnabled && s.checkpointingAvailable {
		state, createCheckpoint := s.shouldCreateCheckpoint(request)

		// If checkpointing is enabled, attempt to create a checkpoint
		if createCheckpoint {
			go s.createCheckpoint(ctx, request)
		} else if state.Status == types.CheckpointStatusAvailable {
			checkpointPath := filepath.Join(s.config.Worker.Checkpointing.Storage.MountPath, state.RemoteKey)
			processState, err := s.cedanaClient.Restore(ctx, state.ContainerId, checkpointPath, &runc.CreateOpts{
				Detach:        true,
				ConsoleSocket: consoleWriter,
				ConfigPath:    configPath,
				Started:       pidChan,
			})
			if err != nil {
				log.Printf("<%s> - failed to restore checkpoint: %v\n", request.ContainerId, err)
				// TODO: if restore fails, update checkpoint state to restore_failed
			} else {
				restored = true
				log.Printf("<%s> - checkpoint found and restored, process state: %+v\n", request.ContainerId, processState)
				err = s.wait(state.ContainerId)
				if err != nil {
					log.Printf("<%s> - failed to wait for container exit: %v\n", containerId, err)
				}
			}
		}
	}

	// Invoke runc process (launch the container), if not restored from checkpoint
	if !restored {
		exitCode, err = s.runcHandle.Run(s.ctx, containerId, opts.BundlePath, &runc.CreateOpts{
			Detach:        true,
			ConsoleSocket: consoleWriter,
			ConfigPath:    configPath,
			Started:       pidChan,
		})
		if err != nil {
			log.Printf("<%s> - failed to run container: %v\n", containerId, err)
		} else {
			err = s.wait(containerId)
			if err != nil {
				log.Printf("<%s> - failed to wait for container exit: %v\n", containerId, err)
			}
		}
	}

	// Send last log message since the container has exited
	outputChan <- common.OutputMsg{
		Msg:     "",
		Done:    true,
		Success: err == nil,
	}
}

// Wait for container to exit
func (s *Worker) wait(containerId string) error {
	events, err := s.runcHandle.Events(s.ctx, containerId, runcEventsInterval)
	if err != nil {
		return err
	}

	// wait until closure
	for range events {
		// can do something with events
	}

	err = s.runcHandle.Delete(s.ctx, containerId, &runc.DeleteOpts{Force: true})
	if err != nil {
		log.Printf("<%s> - failed to delete container: %v\n", containerId, err)
		return err
	}

	return nil
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

func (s *Worker) watchOOMEvents(ctx context.Context, containerId string, output chan common.OutputMsg) {
	seenEvents := make(map[string]struct{})

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	ch, err := s.runcHandle.Events(ctx, containerId, time.Second)
	if err != nil {
		log.Printf("<%s> failed to open runc events channel: %v", containerId, err)
		return
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
					log.Printf("<%s> failed to watch for OOM events.", containerId)
					return
				}

				tries++
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
					ch, err = s.runcHandle.Events(ctx, containerId, time.Second)
					if err != nil {
						log.Printf("<%s> failed to open runc events channel: %v", containerId, err)
					}
				}
				continue
			}

			if _, ok := seenEvents[event.Type]; ok {
				continue
			}

			seenEvents[event.Type] = struct{}{}

			if event.Type == "oom" {
				output <- common.OutputMsg{
					Msg: fmt.Sprintln("[ERROR] A process in the container was killed due to out-of-memory conditions."),
				}
			}
		}
	}
}

// Waits for the container to be ready to checkpoint at the desired point in execution, ie.
// after all processes within a container have reached a checkpointable state
func (s *Worker) createCheckpoint(ctx context.Context, request *types.ContainerRequest) error {
	os.MkdirAll(filepath.Join(s.config.Worker.Checkpointing.Storage.MountPath, request.Workspace.Name), os.ModePerm)

	timeout := defaultCheckpointDeadline
	managing := false
	gpuEnabled := request.Gpu != ""

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

waitForReady:
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("checkpoint deadline exceeded or container exited")
		case <-ticker.C:
			instance, exists := s.containerInstances.Get(request.ContainerId)
			if !exists {
				log.Printf("<%s> - container instance not found yet\n", request.ContainerId)
				continue
			}

			// Start managing the container with Cedana
			if !managing {
				err := s.cedanaClient.Manage(ctx, instance.Id, gpuEnabled)
				if err == nil {
					managing = true
				} else {
					log.Printf("<%s> - cedana manage failed, container may not be started yet: %+v\n", instance.Id, err)
				}
				continue
			}

			// Check if the endpoint is ready for checkpoint by verifying the existence of the file
			readyFilePath := fmt.Sprintf("/tmp/%s/cedana/READY_FOR_CHECKPOINT", instance.Id)
			if _, err := os.Stat(readyFilePath); err == nil {
				log.Printf("<%s> - endpoint is ready for checkpoint.\n", instance.Id)
				break waitForReady
			} else {
				log.Printf("<%s> - endpoint not ready for checkpoint.\n", instance.Id)
			}

		}
	}

	// Proceed to create the checkpoint
	checkpointPath, err := s.cedanaClient.Checkpoint(ctx, request.ContainerId)
	if err != nil {
		log.Printf("<%s> - cedana checkpoint failed: %+v\n", request.ContainerId, err)

		s.containerRepo.UpdateCheckpointState(request.Workspace.Name, request.StubId, &types.CheckpointState{
			Status:      types.CheckpointStatusCheckpointFailed,
			ContainerId: request.ContainerId,
			StubId:      request.StubId,
		})
		return err
	}

	// Move compressed checkpoint file to long-term storage directory
	remoteKey := filepath.Join(request.Workspace.Name, filepath.Base(checkpointPath))
	err = copyFile(checkpointPath, filepath.Join(s.config.Worker.Checkpointing.Storage.MountPath, remoteKey))
	if err != nil {
		log.Printf("<%s> - failed to copy checkpoint to storage: %v\n", request.ContainerId, err)
		return err
	}

	// TODO: Delete checkpoint files from local disk in /tmp

	log.Printf("<%s> - checkpoint created successfully\n", request.ContainerId)
	return s.containerRepo.UpdateCheckpointState(request.Workspace.Name, request.StubId, &types.CheckpointState{
		Status:      types.CheckpointStatusAvailable,
		ContainerId: request.ContainerId, // We store this as a reference to the container that we initially checkpointed
		StubId:      request.StubId,
		RemoteKey:   remoteKey,
	})
}

// shouldCreateCheckpoint checks if a checkpoint should be created for a given container
// NOTE: this currently only works for deployments since functions can run multiple containers
func (s *Worker) shouldCreateCheckpoint(request *types.ContainerRequest) (types.CheckpointState, bool) {
	if !s.checkpointingAvailable || !request.CheckpointEnabled {
		return types.CheckpointState{}, false
	}

	state, err := s.containerRepo.GetCheckpointState(request.Workspace.Name, request.StubId)
	if err != nil {
		if _, ok := err.(*types.ErrCheckpointNotFound); !ok {
			return types.CheckpointState{}, false
		}

		// If checkpoint not found, we can proceed to create one
		return types.CheckpointState{Status: types.CheckpointStatusNotFound}, true
	}

	return *state, false
}
