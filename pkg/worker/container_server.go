package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// ContainerRuntimeServer is a runtime-agnostic container server that works with any OCI runtime
type ContainerRuntimeServer struct {
	baseConfigSpec specs.Spec
	pb.UnimplementedContainerServiceServer
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerRepoClient     pb.ContainerRepositoryServiceClient
	containerNetworkManager *ContainerNetworkManager
	imageClient             *ImageClient
	runtime                 runtime.Runtime // The worker's configured runtime (from pool config)
	port                    int
	podAddr                 string
	createCheckpoint        func(ctx context.Context, opts *CreateCheckpointOpts) error
	grpcServer              *grpc.Server
	mu                      sync.Mutex
}

type ContainerRuntimeServerOpts struct {
	PodAddr                 string
	Runtime                 runtime.Runtime // The runtime configured for this worker pool
	ContainerInstances      *common.SafeMap[*ContainerInstance]
	ImageClient             *ImageClient
	ContainerRepoClient     pb.ContainerRepositoryServiceClient
	ContainerNetworkManager *ContainerNetworkManager
	CreateCheckpoint        func(ctx context.Context, opts *CreateCheckpointOpts) error
}

// NewContainerRuntimeServer creates a new runtime-agnostic container server
func NewContainerRuntimeServer(opts *ContainerRuntimeServerOpts) (*ContainerRuntimeServer, error) {
	var baseConfigSpec specs.Spec

	// Get the appropriate base config for the runtime
	baseConfig := runtime.GetBaseConfig(opts.Runtime.Name())
	specTemplate := strings.TrimSpace(baseConfig)
	err := json.Unmarshal([]byte(specTemplate), &baseConfigSpec)
	if err != nil {
		return nil, err
	}

	return &ContainerRuntimeServer{
		podAddr:                 opts.PodAddr,
		runtime:                 opts.Runtime,
		baseConfigSpec:          baseConfigSpec,
		containerInstances:      opts.ContainerInstances,
		imageClient:             opts.ImageClient,
		containerRepoClient:     opts.ContainerRepoClient,
		containerNetworkManager: opts.ContainerNetworkManager,
		createCheckpoint:        opts.CreateCheckpoint,
	}, nil
}

func (s *ContainerRuntimeServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.grpcServer != nil {
		return errors.New("server already started")
	}

	listener, err := net.Listen("tcp", ":0") // Random free port
	if err != nil {
		log.Error().Err(err).Msg("failed to listen")
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.port = listener.Addr().(*net.TCPAddr).Port
	log.Info().Int("port", s.port).Msg("container runtime server started")

	s.grpcServer = grpc.NewServer()
	pb.RegisterContainerServiceServer(s.grpcServer, s)

	go func() {
		err := s.grpcServer.Serve(listener)
		if err != nil {
			log.Error().Err(err).Msg("failed to start grpc server")
			os.Exit(1)
		}
	}()

	return nil
}

func (s *ContainerRuntimeServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
		s.grpcServer = nil
	}
	return nil
}

// getRuntime returns the worker's configured runtime
func (s *ContainerRuntimeServer) getRuntime() runtime.Runtime {
	return s.runtime
}

// ContainerKill kills and removes a container using the worker's configured runtime
func (s *ContainerRuntimeServer) ContainerKill(ctx context.Context, in *pb.ContainerKillRequest) (*pb.ContainerKillResponse, error) {
	rt := s.getRuntime()

	// Send SIGTERM first
	_ = rt.Kill(ctx, in.ContainerId, syscall.SIGTERM, &runtime.KillOpts{All: true})

	// Force delete
	err := rt.Delete(ctx, in.ContainerId, &runtime.DeleteOpts{Force: true})

	return &pb.ContainerKillResponse{
		Ok: err == nil,
	}, nil
}

// ContainerExec executes a command inside a running container
func (s *ContainerRuntimeServer) ContainerExec(ctx context.Context, in *pb.ContainerExecRequest) (*pb.ContainerExecResponse, error) {
	cmd := fmt.Sprintf("sh -c '%s'", in.Cmd)
	parsedCmd, err := shlex.Split(cmd)
	if err != nil {
		return &pb.ContainerExecResponse{}, err
	}

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerExecResponse{Ok: false}, nil
	}

	process := s.baseConfigSpec.Process
	process.Args = parsedCmd
	process.Cwd = instance.Spec.Process.Cwd

	instanceSpec := instance.Spec.Process
	process.Env = append(instanceSpec.Env, in.Env...)

	if instance.Request.IsBuildRequest() {
		// For build containers, use background context to prevent cancellation issues
		ctx = context.Background()
		process.Env = append(process.Env, instance.Request.BuildOptions.BuildSecrets...)
	}

	// Use the worker's configured runtime for exec
	rt := s.getRuntime()
	err = rt.Exec(ctx, in.ContainerId, *process, &runtime.ExecOpts{
		OutputWriter: instance.OutputWriter,
	})

	return &pb.ContainerExecResponse{
		Ok: err == nil,
	}, nil
}

// ContainerStatus returns the status of a container
func (s *ContainerRuntimeServer) ContainerStatus(ctx context.Context, in *pb.ContainerStatusRequest) (*pb.ContainerStatusResponse, error) {
	rt := s.getRuntime()

	state, err := rt.State(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerStatusResponse{Running: false}, nil
	}

	return &pb.ContainerStatusResponse{
		Running: state.Status == types.RuncContainerStatusRunning,
	}, nil
}

// ContainerStreamLogs streams container logs
func (s *ContainerRuntimeServer) ContainerStreamLogs(req *pb.ContainerStreamLogsRequest, stream pb.ContainerService_ContainerStreamLogsServer) error {
	instance, exists := s.containerInstances.Get(req.ContainerId)
	if !exists {
		return errors.New("container not found")
	}

	buffer := make([]byte, 4096)
	logEntry := &pb.ContainerLogEntry{}

	for {
		select {
		case <-stream.Context().Done():
			return errors.New("context cancelled")
		default:
		}

		n, err := instance.LogBuffer.Read(buffer)
		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if n > 0 {
			logEntry.Msg = string(buffer[:n])
			if err := stream.Send(logEntry); err != nil {
				return err
			}
			continue
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}

	return nil
}

// ContainerCheckpoint creates a checkpoint of a running container
func (s *ContainerRuntimeServer) ContainerCheckpoint(ctx context.Context, in *pb.ContainerCheckpointRequest) (*pb.ContainerCheckpointResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerCheckpointResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	// Check if runtime supports checkpointing
	rt := s.getRuntime()
	if !rt.Capabilities().CheckpointRestore {
		return &pb.ContainerCheckpointResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Runtime %s does not support checkpoint/restore", rt.Name()),
		}, nil
	}

	checkpointId := uuid.New().String()
	err := s.createCheckpoint(ctx, &CreateCheckpointOpts{
		Request:      instance.Request,
		CheckpointId: checkpointId,
		ContainerIp:  instance.ContainerIp,
	})
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to create checkpoint: %v", err)
		return &pb.ContainerCheckpointResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerCheckpointResponse{Ok: true, CheckpointId: checkpointId}, nil
}

// ContainerArchive archives a container's filesystem
func (s *ContainerRuntimeServer) ContainerArchive(req *pb.ContainerArchiveRequest, stream pb.ContainerService_ContainerArchiveServer) error {
	ctx := stream.Context()

	instance, exists := s.containerInstances.Get(req.ContainerId)
	if !exists {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	// Check container state using the worker's runtime
	rt := s.getRuntime()
	state, err := rt.State(ctx, req.ContainerId)
	if err != nil {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	if state.Status != types.RuncContainerStatusRunning {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not running"})
	}

	// If it's not a build request, use initial_config.json from the base image bundle
	initialConfigPath := filepath.Join(instance.BundlePath, specBaseName)
	if !instance.Request.IsBuildRequest() {
		initialConfigPath = filepath.Join(instance.BundlePath, initialSpecBaseName)
	}

	// Ensure initial config exists; for v2 (no unpack), derive from image if missing
	destInitial := filepath.Join(instance.Overlay.TopLayerPath(), initialSpecBaseName)
	if _, statErr := os.Stat(initialConfigPath); statErr == nil {
		if err = copyFile(initialConfigPath, destInitial); err != nil {
			return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
		}
	} else {
		// Derive initial spec from source image metadata via skopeo inspect
		if err = s.writeInitialSpecFromImage(ctx, instance, destInitial); err != nil {
			return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
		}
	}

	if err := s.addRequestEnvToInitialSpec(instance); err != nil {
		return err
	}

	tempConfig := s.baseConfigSpec
	tempConfig.Hooks.Prestart = nil
	tempConfig.Process.Terminal = false
	tempConfig.Process.Args = []string{"tail", "-f", "/dev/null"}
	tempConfig.Root.Readonly = false

	file, err := json.MarshalIndent(tempConfig, "", "  ")
	if err != nil {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	configPath := filepath.Join(instance.Overlay.TopLayerPath(), specBaseName)
	err = os.WriteFile(configPath, file, 0644)
	if err != nil {
		return stream.Send(&pb.ContainerArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	progressChan := make(chan int)
	doneChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		lastProgress := -1
		keepaliveTicker := time.NewTicker(10 * time.Second)
		defer keepaliveTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case progress, ok := <-progressChan:
				if !ok {
					return
				}
				if progress > lastProgress && progress != lastProgress {
					lastProgress = progress
					log.Info().Int("progress", progress).Msg("image upload progress")
					err := stream.Send(&pb.ContainerArchiveResponse{Done: false, Success: false, Progress: int32(progress), ErrorMsg: ""})
					if err != nil {
						return
					}
				}
			case <-keepaliveTicker.C:
				// Send a keepalive message to prevent connection timeout during long operations
				err := stream.Send(&pb.ContainerArchiveResponse{Done: false, Success: false, Progress: 0, ErrorMsg: ""})
				if err != nil {
					log.Warn().Err(err).Msg("failed to send keepalive message")
					return
				}
			case <-doneChan:
				return
			}
		}
	}()

	defer func() {
		close(progressChan)
		wg.Wait()
	}()

	topLayerPath := NewPathInfo(instance.Overlay.TopLayerPath())
	err = stream.Send(&pb.ContainerArchiveResponse{
		Done: true, Success: s.imageClient.Archive(ctx, topLayerPath, req.ImageId, progressChan) == nil,
	})

	close(doneChan)
	return err
}

// writeInitialSpecFromImage builds an initial_config.json using the base runc config
// plus full configuration (env, workdir, user, cmd, entrypoint) from v2 CLIP metadata if available.
// V1 images always have a config.json so this is only called for v2 images.
// The base spec is designed to be the fallback when CLIP metadata is not available.
func (s *ContainerRuntimeServer) writeInitialSpecFromImage(ctx context.Context, instance *ContainerInstance, destPath string) error {
	// Start from the base config (this is the designed fallback for v1 images)
	spec := s.baseConfigSpec

	// Try to get CLIP metadata from archive (v2 images only)
	clipMeta, ok := s.imageClient.GetCLIPImageMetadata(instance.Request.ImageId)
	if ok {
		log.Info().Str("image_id", instance.Request.ImageId).Msg("using v2 image metadata from clip archive for initial spec")

		// CLIP metadata has a flat structure with all fields at the top level
		if len(clipMeta.Env) > 0 {
			spec.Process.Env = append(spec.Process.Env, clipMeta.Env...)
		}
		if clipMeta.WorkingDir != "" {
			spec.Process.Cwd = clipMeta.WorkingDir
		}
		if clipMeta.User != "" {
			spec.Process.User.Username = clipMeta.User
		}
		// Set default args from Cmd if Entrypoint is not set, or combine them
		if len(clipMeta.Entrypoint) > 0 {
			spec.Process.Args = append(clipMeta.Entrypoint, clipMeta.Cmd...)
		} else if len(clipMeta.Cmd) > 0 {
			spec.Process.Args = clipMeta.Cmd
		}
	}
	// If no CLIP metadata, use base spec as-is (designed for v1 images)

	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(destPath, b, 0644)
}

func (s *ContainerRuntimeServer) addRequestEnvToInitialSpec(instance *ContainerInstance) error {
	if len(instance.Request.Env) == 0 {
		return nil
	}

	specPath := filepath.Join(instance.Overlay.TopLayerPath(), initialSpecBaseName)

	bytes, err := os.ReadFile(specPath)
	if err != nil {
		return err
	}

	var spec specs.Spec
	if err = json.Unmarshal(bytes, &spec); err != nil {
		return err
	}

	spec.Process.Env = append(instance.Request.Env, spec.Process.Env...)

	bytes, err = json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}

	if err = os.WriteFile(specPath, bytes, 0644); err != nil {
		return err
	}

	return nil
}

// ContainerSyncWorkspace syncs workspace files
func (s *ContainerRuntimeServer) ContainerSyncWorkspace(ctx context.Context, in *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
	_, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
	}

	workspacePath := types.TempContainerWorkspace(in.ContainerId)
	destPath := path.Join(workspacePath, in.Path)
	destNewPath := path.Join(workspacePath, in.NewPath)

	switch in.Op {
	case pb.SyncContainerWorkspaceOperation_DELETE:
		if err := os.RemoveAll(destPath); err != nil {
			return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
		}
	case pb.SyncContainerWorkspaceOperation_WRITE:
		if in.IsDir {
			os.MkdirAll(destPath, 0755)
		} else {
			os.MkdirAll(path.Dir(destPath), 0755)
			if err := os.WriteFile(destPath, in.Data, 0644); err != nil {
				return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
			}
		}
	case pb.SyncContainerWorkspaceOperation_MOVED:
		os.MkdirAll(path.Dir(destNewPath), 0755)
		if err := os.Rename(destPath, destNewPath); err != nil {
			return &pb.SyncContainerWorkspaceResponse{Ok: false}, nil
		}
	}

	return &pb.SyncContainerWorkspaceResponse{Ok: true}, nil
}

// waitForContainer waits for a container to be running
func (s *ContainerRuntimeServer) waitForContainer(ctx context.Context, containerId string) error {
	rt := s.getRuntime()

	for {
		instance, exists := s.containerInstances.Get(containerId)
		if !exists {
			return errors.New("container not found")
		}

		if instance.Spec == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		state, err := rt.State(ctx, containerId)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if state.Pid != 0 && state.Status == types.RuncContainerStatusRunning {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// getHostPathFromContainerPath maps a container path to its corresponding host path
func (s *ContainerRuntimeServer) getHostPathFromContainerPath(containerPath string, instance *ContainerInstance) string {
	// Check if the path matches any of the container's mounts
	for _, mount := range instance.Spec.Mounts {
		if containerPath == mount.Destination || strings.HasPrefix(containerPath, mount.Destination+"/") {
			relativePath := strings.TrimPrefix(containerPath, mount.Destination)
			return filepath.Join(mount.Source, strings.TrimPrefix(relativePath, "/"))
		}
	}

	// For overlay filesystems, write to the merged directory (not upper directly)
	// The overlay filesystem driver automatically stores changes in the upper layer
	// and makes them visible in both merged (what container sees) and upper
	if instance.Overlay != nil {
		mergedPath := instance.Overlay.TopLayerPath()
		if mergedPath != "" {
			return filepath.Join(mergedPath, strings.TrimPrefix(filepath.Clean(containerPath), "/"))
		}
	}

	// Fallback: use the root path (this shouldn't happen with overlay but keep for safety)
	return filepath.Join(instance.Spec.Root.Path, strings.TrimPrefix(filepath.Clean(containerPath), "/"))
}

// Sandbox methods follow (these are runtime-agnostic and work with the sandbox process manager)

func (s *ContainerRuntimeServer) ContainerSandboxExec(ctx context.Context, in *pb.ContainerSandboxExecRequest) (*pb.ContainerSandboxExecResponse, error) {
	log.Info().Str("container_id", in.ContainerId).Str("cmd", in.Cmd).Msg("running sandbox command")

	parsedCmd, err := shlex.Split(in.Cmd)
	if err != nil {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err = s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	env := instance.Spec.Process.Env
	formattedEnv := []string{}
	for key, value := range in.Env {
		formattedEnv = append(formattedEnv, fmt.Sprintf("%s=%s", key, value))
	}

	env = append(env, formattedEnv...)

	return s.handleSandboxExec(ctx, in, instance, env, parsedCmd, in.Cwd)
}

func (s *ContainerRuntimeServer) handleSandboxExec(ctx context.Context, in *pb.ContainerSandboxExecRequest, instance *ContainerInstance, env, cmd []string, cwd string) (*pb.ContainerSandboxExecResponse, error) {
	// Wait for process manager to be ready (polls the flag set by startup initialization)
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for !instance.SandboxProcessManagerReady {
		select {
		case <-timeout:
			return &pb.ContainerSandboxExecResponse{Ok: false, Pid: -1, ErrorMsg: "Process manager not ready within timeout"}, nil
		case <-ctx.Done():
			return &pb.ContainerSandboxExecResponse{Ok: false, Pid: -1, ErrorMsg: "Request cancelled"}, nil
		case <-ticker.C:
			// Refresh instance to get latest SandboxProcessManagerReady state
			if fresh, exists := s.containerInstances.Get(in.ContainerId); exists {
				instance = fresh
			}
		}
	}

	// Process manager is ready, execute the command
	pid, err := instance.SandboxProcessManager.Exec(cmd, cwd, env, false)
	if err != nil {
		return &pb.ContainerSandboxExecResponse{Ok: false, Pid: -1, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxExecResponse{Ok: true, Pid: int32(pid)}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxStatus(ctx context.Context, in *pb.ContainerSandboxStatusRequest) (*pb.ContainerSandboxStatusResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	exitCode, err := instance.SandboxProcessManager.Status(int(in.Pid))
	if err != nil {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	status := "running"
	if exitCode >= 0 {
		status = "exited"
	}

	return &pb.ContainerSandboxStatusResponse{
		Ok:       true,
		Status:   status,
		ExitCode: int32(exitCode),
	}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxStdout(ctx context.Context, in *pb.ContainerSandboxStdoutRequest) (*pb.ContainerSandboxStdoutResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	stdout, err := instance.SandboxProcessManager.Stdout(int(in.Pid))
	if err != nil {
		return &pb.ContainerSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.ContainerSandboxStdoutResponse{
		Ok:     true,
		Stdout: stdout,
	}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxStderr(ctx context.Context, in *pb.ContainerSandboxStderrRequest) (*pb.ContainerSandboxStderrResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	stderr, err := instance.SandboxProcessManager.Stderr(int(in.Pid))
	if err != nil {
		return &pb.ContainerSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.ContainerSandboxStderrResponse{
		Ok:     true,
		Stderr: stderr,
	}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxKill(ctx context.Context, in *pb.ContainerSandboxKillRequest) (*pb.ContainerSandboxKillResponse, error) {
	log.Info().Str("container_id", in.ContainerId).Int32("pid", in.Pid).Msg("killing sandbox process")

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxKillResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := instance.SandboxProcessManager.Kill(int(in.Pid))
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Int32("pid", in.Pid).Msgf("failed to kill sandbox process: %v", err)
		return &pb.ContainerSandboxKillResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxKillResponse{Ok: true}, err
}

func (s *ContainerRuntimeServer) ContainerSandboxListExposedPorts(ctx context.Context, in *pb.ContainerSandboxListExposedPortsRequest) (*pb.ContainerSandboxListExposedPortsResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxListExposedPortsResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	excludedPorts := []int32{types.WorkerSandboxProcessManagerPort, types.WorkerShellPort, int32(containerInnerPort)}
	ports := make([]int32, 0)
	for _, port := range instance.Request.Ports {
		if slices.Contains(excludedPorts, int32(port)) {
			continue
		}
		ports = append(ports, int32(port))
	}

	if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
		return &pb.ContainerSandboxListExposedPortsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxListExposedPortsResponse{Ok: true, ExposedPorts: ports}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxListProcesses(ctx context.Context, in *pb.ContainerSandboxListProcessesRequest) (*pb.ContainerSandboxListProcessesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxListProcessesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
		return &pb.ContainerSandboxListProcessesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	processes := make([]*pb.ProcessInfo, 0)
	ps, err := instance.SandboxProcessManager.ListProcesses()
	if err != nil {
		return &pb.ContainerSandboxListProcessesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	for _, process := range ps {
		processes = append(processes, &pb.ProcessInfo{Pid: int32(process.Pid), ExitCode: int32(process.ExitCode), Cwd: process.Cwd, Cmd: process.Cmd, Env: process.Env, Running: process.Running})
	}

	return &pb.ContainerSandboxListProcessesResponse{Ok: true, Processes: processes}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxUploadFile(ctx context.Context, in *pb.ContainerSandboxUploadFileRequest) (*pb.ContainerSandboxUploadFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	// For gVisor: write to external mount, then mv inside container to avoid caching issues
	// External mounts are always shared (no caching) per gVisor docs
	if instance.Runtime != nil && instance.Runtime.Name() == types.ContainerRuntimeGvisor.String() {
		if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
			return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}

		// Write to external bind mount
		tempFile := fmt.Sprintf("upload_%d", time.Now().UnixNano())
		tempHostPath := filepath.Join("/tmp/container-uploads", in.ContainerId, tempFile)
		if err := os.WriteFile(tempHostPath, in.Data, os.FileMode(in.Mode)); err != nil {
			return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}

		// Move to target location inside container
		quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
		cmd := fmt.Sprintf("mkdir -p %s && mv /tmp/.beta9/%s %s && chmod %o %s",
			quote(filepath.Dir(containerPath)), tempFile, quote(containerPath), in.Mode, quote(containerPath))

		if resp, err := s.ContainerExec(ctx, &pb.ContainerExecRequest{
			ContainerId: in.ContainerId,
			Cmd:         cmd,
			Env:         instance.Spec.Process.Env,
		}); err != nil || !resp.Ok {
			os.Remove(tempHostPath)
			return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: "mv failed"}, nil
		}

		return &pb.ContainerSandboxUploadFileResponse{Ok: true}, nil
	}

	// For runc: direct write to overlay
	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	if err := os.MkdirAll(filepath.Dir(hostPath), 0755); err != nil {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if err := os.WriteFile(hostPath, in.Data, os.FileMode(in.Mode)); err != nil {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxUploadFileResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxCreateDirectory(ctx context.Context, in *pb.ContainerSandboxCreateDirectoryRequest) (*pb.ContainerSandboxCreateDirectoryResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
		return &pb.ContainerSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(filepath.Clean(containerPath), instance)
	if err := os.MkdirAll(hostPath, os.FileMode(in.Mode)); err != nil {
		return &pb.ContainerSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxCreateDirectoryResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxDeleteDirectory(ctx context.Context, in *pb.ContainerSandboxDeleteDirectoryRequest) (*pb.ContainerSandboxDeleteDirectoryResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
		return &pb.ContainerSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(filepath.Clean(containerPath), instance)
	if err := os.RemoveAll(hostPath); err != nil {
		return &pb.ContainerSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxDeleteDirectoryResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxDownloadFile(ctx context.Context, in *pb.ContainerSandboxDownloadFileRequest) (*pb.ContainerSandboxDownloadFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	data, err := os.ReadFile(hostPath)
	if err != nil {
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxDownloadFileResponse{Ok: true, Data: data}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxDeleteFile(ctx context.Context, in *pb.ContainerSandboxDeleteFileRequest) (*pb.ContainerSandboxDeleteFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	err = os.RemoveAll(hostPath)
	if err != nil {
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxDeleteFileResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxStatFile(ctx context.Context, in *pb.ContainerSandboxStatFileRequest) (*pb.ContainerSandboxStatFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	stat, err := os.Stat(hostPath)
	if err != nil {
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.ContainerSandboxStatFileResponse{Ok: true, FileInfo: &pb.FileInfo{
		Mode:        int32(stat.Mode()),
		Size:        stat.Size(),
		ModTime:     stat.ModTime().Unix(),
		Permissions: uint32(stat.Mode()),
		Owner:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Uid)),
		Group:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Gid)),
		IsDir:       stat.IsDir(),
		Name:        stat.Name(),
	}}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxListFiles(ctx context.Context, in *pb.ContainerSandboxListFilesRequest) (*pb.ContainerSandboxListFilesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	files, err := os.ReadDir(hostPath)
	if err != nil {
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	responseFiles := make([]*pb.FileInfo, 0)
	for _, file := range files {
		stat, err := file.Info()
		if err != nil {
			return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}

		responseFiles = append(responseFiles, &pb.FileInfo{
			Mode:        int32(stat.Mode()),
			Size:        stat.Size(),
			ModTime:     stat.ModTime().Unix(),
			Permissions: uint32(stat.Mode()),
			Owner:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Uid)),
			Group:       strconv.Itoa(int(stat.Sys().(*syscall.Stat_t).Gid)),
			IsDir:       stat.IsDir(),
			Name:        file.Name(),
		})
	}

	return &pb.ContainerSandboxListFilesResponse{Ok: true, Files: responseFiles}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxExposePort(ctx context.Context, in *pb.ContainerSandboxExposePortRequest) (*pb.ContainerSandboxExposePortResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	getAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.GetContainerAddressMap(context.Background(), &pb.GetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
	}))
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	addressMap := getAddressMapResponse.AddressMap
	if _, exists := addressMap[int32(in.Port)]; exists {
		return &pb.ContainerSandboxExposePortResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Port %d is already exposed", in.Port),
		}, nil
	}

	bindPort, err := getRandomFreePort()
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	err = s.containerNetworkManager.ExposePort(in.ContainerId, bindPort, int(in.Port))
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to expose container bind port: %v", err)
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	addressMap[int32(in.Port)] = fmt.Sprintf("%s:%d", s.podAddr, bindPort)
	setAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
		AddressMap:  addressMap,
	}))
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	instance.Request.Ports = append(instance.Request.Ports, uint32(in.Port))
	s.containerInstances.Set(in.ContainerId, instance)

	log.Info().Str("container_id", in.ContainerId).Msgf("exposed sandbox port %d to %s", in.Port, addressMap[int32(in.Port)])

	return &pb.ContainerSandboxExposePortResponse{Ok: setAddressMapResponse.Ok}, err
}

func (s *ContainerRuntimeServer) ContainerSandboxReplaceInFiles(ctx context.Context, in *pb.ContainerSandboxReplaceInFilesRequest) (*pb.ContainerSandboxReplaceInFilesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	stagedFiles, err := stageFilesForReplacement(hostPath, in.Pattern, in.NewString)
	if err != nil {
		return &pb.ContainerSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	for _, stagedFile := range stagedFiles {
		err = os.WriteFile(stagedFile.Path, []byte(stagedFile.Content), 0644)
		if err != nil {
			return &pb.ContainerSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	return &pb.ContainerSandboxReplaceInFilesResponse{Ok: true}, nil
}

func (s *ContainerRuntimeServer) ContainerSandboxFindInFiles(ctx context.Context, in *pb.ContainerSandboxFindInFilesRequest) (*pb.ContainerSandboxFindInFilesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxFindInFilesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxFindInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)

	// Build ripgrep command with JSON output format
	args := []string{
		"--json",
		"--line-number",
		"--column",
		"--with-filename",
		"--no-heading",
		"--no-messages",
		"--no-ignore",
		"--hidden",
		"--binary",
		"--regexp", in.Pattern,
		hostPath,
	}

	cmd := exec.CommandContext(ctx, "rg", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		// ripgrep returns exit code 1 when no matches are found
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return &pb.ContainerSandboxFindInFilesResponse{Ok: true, Results: []*pb.FileSearchResult{}}, nil
		}
		return &pb.ContainerSandboxFindInFilesResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("ripgrep failed: %v, stderr: %s", err, stderr.String()),
		}, nil
	}

	// Parse ripgrep JSON output
	results := []*pb.FileSearchResult{}
	fileMatches := make(map[string][]*pb.FileSearchMatch)

	lines := strings.Split(stdout.String(), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var rgResult struct {
			Type string `json:"type"`
			Data struct {
				Path struct {
					Text string `json:"text"`
				} `json:"path"`
				Lines struct {
					Text string `json:"text"`
				} `json:"lines"`
				LineNumber     int `json:"line_number"`
				AbsoluteOffset int `json:"absolute_offset"`
				Submatches     []struct {
					Match struct {
						Text string `json:"text"`
					} `json:"match"`
					Start int `json:"start"`
					End   int `json:"end"`
				} `json:"submatches"`
			} `json:"data"`
		}

		if err := json.Unmarshal([]byte(line), &rgResult); err != nil {
			log.Warn().Str("line", line).Err(err).Msg("failed to parse ripgrep JSON output")
			continue
		}

		if rgResult.Type != "match" {
			continue
		}

		filePath := rgResult.Data.Path.Text
		lineNum := rgResult.Data.LineNumber

		for _, submatch := range rgResult.Data.Submatches {
			startCol := submatch.Start + 1
			endCol := submatch.End

			match := &pb.FileSearchMatch{
				Range: &pb.FileSearchRange{
					Start: &pb.FileSearchPosition{
						Line:   int32(lineNum),
						Column: int32(startCol),
					},
					End: &pb.FileSearchPosition{
						Line:   int32(lineNum),
						Column: int32(endCol),
					},
				},
				Content: submatch.Match.Text,
			}

			cleanedPath := filepath.Clean(filepath.Join(containerPath, strings.TrimPrefix(filePath, hostPath+string(os.PathSeparator))))
			fileMatches[cleanedPath] = append(fileMatches[cleanedPath], match)
		}
	}

	for filePath, matches := range fileMatches {
		results = append(results, &pb.FileSearchResult{
			Path:    filePath,
			Matches: matches,
		})
	}

	return &pb.ContainerSandboxFindInFilesResponse{Ok: true, Results: results}, nil
}

// Helper types and functions

type StagedFile struct {
	Path    string
	Content string
}

func stageFilesForReplacement(basePath string, stringToReplace string, stringToReplaceWith string) ([]StagedFile, error) {
	stagedFiles := []StagedFile{}
	regex, err := regexp.Compile(stringToReplace)
	if err != nil {
		return nil, err
	}

	filepath.WalkDir(basePath, func(path string, d os.DirEntry, err error) error {
		if err != nil && os.IsNotExist(err) {
			return nil
		}

		if !d.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			content, err := io.ReadAll(file)
			if err != nil {
				return err
			}

			if regex.Match(content) {
				content = regex.ReplaceAll(content, []byte(stringToReplaceWith))
			}

			stagedFiles = append(stagedFiles, StagedFile{
				Path:    path,
				Content: string(content),
			})
		}

		return nil
	})

	return stagedFiles, nil
}
