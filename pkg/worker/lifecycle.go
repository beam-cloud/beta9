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

	"github.com/containerd/cgroups/v3"
	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/containerd/cgroups/v3/cgroup2"
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

	memoryEvents MemoryEvents
}

type MemoryEvents struct {
	// This counter increments whenever a process in the cgroup
	// is terminated by the OOM killer due to memory exhaustion.
	oomKill uint
	// This counter increments when the OOM killer terminates
	// all processes in the cgroup as a group due to memory exhaustion.
	// TODO: Use this field once this issue is addressed https://github.com/containerd/cgroups/issues/274
	oomGroupKill uint
}

func (i *ContainerInstance) isOOMKilled() bool {
	memoryEvents, err := getMemoryEvents(i)
	if err != nil {
		log.Printf("<%s> - failed to get memory events: %v\n", i.Id, err)
		return false
	}

	if memoryEvents.oomKill > i.memoryEvents.oomKill || memoryEvents.oomGroupKill > i.memoryEvents.oomGroupKill {
		i.memoryEvents = memoryEvents
		return true
	}

	return false
}

func getMemoryEvents(i *ContainerInstance) (MemoryEvents, error) {
	me := MemoryEvents{}

	switch cgroups.Mode() {
	case cgroups.Legacy:
		c, err := cgroup1.Load(cgroup1.StaticPath(i.Spec.Linux.CgroupsPath))
		if err != nil {
			return me, err
		}

		stat, err := c.Stat()
		if err != nil {
			return me, err
		}

		me.oomKill = uint(stat.MemoryOomControl.GetOomKill())

	case cgroups.Unified:
		c, err := cgroup2.Load(i.Spec.Linux.CgroupsPath)
		if err != nil {
			return me, err
		}

		stat, err := c.Stat()
		if err != nil {
			return me, err
		}

		me.oomKill = uint(stat.GetMemoryEvents().GetOomKill())

	default:
		return me, fmt.Errorf("unknown cgroups mode")
	}

	return me, nil
}

type ContainerOptions struct {
	BundlePath  string
	BindPort    int
	InitialSpec *specs.Spec
}

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

	if _, exists := s.containerInstances.Get(containerId); !exists {
		log.Printf("<%s> - container not found.\n", containerId)
		return nil
	}

	signal := int(syscall.SIGTERM)
	if kill {
		signal = int(syscall.SIGKILL)
		s.containerRepo.DeleteContainerState(containerId)
		defer s.containerInstances.Delete(containerId)
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

	// Standardize cgroup path. Since it starts with a slash,
	// it will be appended to the cgroup root path /sys/fs/cgroup.
	spec.Linux.CgroupsPath = fmt.Sprintf("/%s/%s", s.workerId, request.ContainerId)

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

	pid := 0
	pidChan := make(chan int, 1)

	go func() {
		// Wait for runc to start the container
		pid = <-pidChan

		// Capture resource usage (cpu/mem/gpu)
		go s.collectAndSendContainerMetrics(ctx, request, spec, pid)

		// Check for OOM kills
		go s.watchForOOMKills(ctx, containerId, outputChan)

		<-ctx.Done()
	}()

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

func (s *Worker) watchForOOMKills(ctx context.Context, containerId string, output chan common.OutputMsg) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c, ok := s.containerInstances.Get(containerId); ok && c.isOOMKilled() {
				output <- common.OutputMsg{
					Msg: fmt.Sprintf("[WARNING] A process in the container was killed due to out-of-memory conditions.\n"),
				}
			}
		}
	}
}
