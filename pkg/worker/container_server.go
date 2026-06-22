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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	goproc "github.com/beam-cloud/goproc/pkg"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	gRPCMaxRecvMsgSize                  = 1024 * 1024 * 16
	gRPCMaxSendMsgSize                  = 1024 * 1024 * 16
	sandboxProcessManagerClientRetry    = 25 * time.Millisecond
	sandboxProcessManagerClientTimeout  = 10 * time.Second
	sandboxProcessManagerReadyTimeout   = 10 * time.Second
	sandboxProcessManagerReadyPollDelay = 25 * time.Millisecond
)

// ContainerRuntimeServer is a runtime-agnostic container server that works with any OCI runtime
type processLogEventRepository interface {
	PushContainerLogEventQueued(entry types.EventContainerLogSchema) error
}

type ContainerRuntimeServer struct {
	baseConfigSpec specs.Spec
	pb.UnimplementedContainerServiceServer
	containerInstances      *common.SafeMap[*ContainerInstance]
	killedSandboxProcesses  sync.Map
	containerRepoClient     pb.ContainerRepositoryServiceClient
	containerNetworkManager ContainerNetwork
	imageClient             *ImageClient
	runtime                 runtime.Runtime // The worker's configured runtime (from pool config)
	eventRepo               processLogEventRepository
	workerID                string
	port                    int
	podAddr                 string
	backendRoute            backendRouteFunc
	createCheckpoint        func(ctx context.Context, opts *CreateCheckpointOpts) error
	grpcServer              *grpc.Server
	mu                      sync.Mutex
	exposePortMu            sync.Mutex
}

type backendRouteFunc func(request *types.ContainerRequest, kind string, port int32, localTarget string) *pb.BackendRoute

type ContainerRuntimeServerOpts struct {
	PodAddr                 string
	Runtime                 runtime.Runtime // The runtime configured for this worker pool
	ContainerInstances      *common.SafeMap[*ContainerInstance]
	ImageClient             *ImageClient
	ContainerRepoClient     pb.ContainerRepositoryServiceClient
	ContainerNetworkManager ContainerNetwork
	EventRepo               repository.EventRepository
	WorkerID                string
	BackendRoute            backendRouteFunc
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
		eventRepo:               opts.EventRepo,
		workerID:                opts.WorkerID,
		backendRoute:            opts.BackendRoute,
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

	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(gRPCMaxRecvMsgSize),
		grpc.MaxSendMsgSize(gRPCMaxSendMsgSize),
	)

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
	spec, err := cloneSpec(s.baseConfigSpec)
	if err != nil {
		return err
	}

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
	if spec.Process.Cwd == "" {
		spec.Process.Cwd = "/"
	}

	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(destPath, b, 0644)
}

func cloneSpec(spec specs.Spec) (specs.Spec, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return specs.Spec{}, err
	}

	var cloned specs.Spec
	if err := json.Unmarshal(b, &cloned); err != nil {
		return specs.Spec{}, err
	}
	return cloned, nil
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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		instance, exists := s.containerInstances.Get(containerId)
		if !exists {
			return errors.New("container not found")
		}

		if instance.Spec == nil {
			if err := waitForContainerRetry(ctx); err != nil {
				return err
			}
			continue
		}

		state, err := rt.State(ctx, containerId)
		if err != nil {
			if err := waitForContainerRetry(ctx); err != nil {
				return err
			}
			continue
		}

		if state.Pid != 0 && state.Status == types.RuncContainerStatusRunning {
			break
		}

		if err := waitForContainerRetry(ctx); err != nil {
			return err
		}
	}

	return nil
}

func waitForContainerRetry(ctx context.Context) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *ContainerRuntimeServer) getHostPathFromContainerPath(containerPath string, instance *ContainerInstance) string {
	// Check mounts first
	for _, mount := range instance.Spec.Mounts {
		if containerPath == mount.Destination || strings.HasPrefix(containerPath, mount.Destination+"/") {
			relativePath := strings.TrimPrefix(containerPath, mount.Destination)
			return filepath.Join(mount.Source, strings.TrimPrefix(relativePath, "/"))
		}
	}

	// Use root path (already set to overlay merged path in lifecycle.go)
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

	instance, err = s.waitForSandboxProcessManager(ctx, in.ContainerId, instance)
	if err != nil {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	if instance.Spec == nil || instance.Spec.Process == nil {
		return &pb.ContainerSandboxExecResponse{Ok: false, ErrorMsg: "Container spec not ready"}, nil
	}

	env := append([]string{}, instance.Spec.Process.Env...)
	formattedEnv := []string{}
	for key, value := range in.Env {
		formattedEnv = append(formattedEnv, fmt.Sprintf("%s=%s", key, value))
	}

	env = append(env, formattedEnv...)

	return s.handleSandboxExec(ctx, in, instance, env, parsedCmd, in.Cwd)
}

func (s *ContainerRuntimeServer) handleSandboxExec(ctx context.Context, in *pb.ContainerSandboxExecRequest, instance *ContainerInstance, env, cmd []string, cwd string) (*pb.ContainerSandboxExecResponse, error) {
	pid, err := s.execSandboxProcess(ctx, in.ContainerId, instance, cmd, cwd, env)
	if err != nil {
		log.Warn().
			Str("container_id", in.ContainerId).
			Str("container_ip", instance.ContainerIp).
			Str("cmd", in.Cmd).
			Str("grpc_code", status.Code(err).String()).
			Err(err).
			Msg("sandbox process manager exec failed")
		return &pb.ContainerSandboxExecResponse{Ok: false, Pid: -1, ErrorMsg: err.Error()}, nil
	}

	if !instance.SandboxProcessManagerReady {
		instance.signalProcessManagerReadiness(true)
		s.containerInstances.Set(in.ContainerId, instance)
	}

	return &pb.ContainerSandboxExecResponse{Ok: true, Pid: int32(pid)}, nil
}

func (s *ContainerRuntimeServer) execSandboxProcess(ctx context.Context, containerId string, instance *ContainerInstance, cmd []string, cwd string, env []string) (int, error) {
	client, cleanup, err := s.sandboxProcessManagerClient(ctx, containerId, instance)
	if err != nil {
		return -1, err
	}
	if cleanup {
		defer client.Cleanup()
	}

	return client.Exec(cmd, cwd, env, false)
}

func (s *ContainerRuntimeServer) sandboxProcessManagerClient(ctx context.Context, containerId string, instance *ContainerInstance) (*goproc.GoProcClient, bool, error) {
	instance = s.refreshContainerInstance(containerId, instance)
	if instance != nil && instance.SandboxProcessManagerReady && instance.SandboxProcessManager != nil {
		return instance.SandboxProcessManager, false, nil
	}

	client, err := s.newSandboxProcessManagerClient(ctx, containerId, instance)
	return client, client != nil, err
}

func (s *ContainerRuntimeServer) newSandboxProcessManagerClient(ctx context.Context, containerId string, instance *ContainerInstance) (*goproc.GoProcClient, error) {
	var lastErr error
	loggedRetry := false
	deadline := time.Now().Add(sandboxProcessManagerClientTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return nil, sandboxProcessManagerClientError(err, lastErr)
		}
		if time.Now().After(deadline) {
			return nil, sandboxProcessManagerClientTimeoutError(lastErr)
		}

		instance = s.refreshContainerInstance(containerId, instance)
		client, err := newProcessManagerClient(ctx, instance)
		if err == nil {
			return client, nil
		}

		lastErr = err
		retryable := isProcessManagerDialFailure(err)
		if !retryable {
			return nil, err
		}
		if !loggedRetry {
			log.Debug().
				Str("container_id", containerId).
				Str("container_ip", instance.ContainerIp).
				Err(err).
				Msg("waiting for sandbox process manager client after dial failure")
			loggedRetry = true
		}

		timer := time.NewTimer(sandboxProcessManagerClientRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, sandboxProcessManagerClientError(ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}

func sandboxProcessManagerClientError(ctxErr, lastErr error) error {
	if errors.Is(ctxErr, context.Canceled) {
		return errors.New("Request cancelled")
	}
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		return sandboxProcessManagerClientTimeoutError(lastErr)
	}
	if lastErr != nil {
		return lastErr
	}
	return ctxErr
}

func sandboxProcessManagerClientTimeoutError(lastErr error) error {
	if lastErr != nil {
		return fmt.Errorf("sandbox process manager client not ready within timeout: %w", lastErr)
	}
	return errors.New("sandbox process manager client not ready within timeout")
}

func isProcessManagerDialFailure(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.DeadlineExceeded {
		return true
	}
	if status.Code(err) != codes.Unavailable {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "error while dialing") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "transport is closing")
}

func (s *ContainerRuntimeServer) waitForSandboxProcessManager(ctx context.Context, containerId string, instance *ContainerInstance) (*ContainerInstance, error) {
	ctx, cancel := context.WithTimeout(ctx, sandboxProcessManagerReadyTimeout)
	defer cancel()

	ticker := time.NewTicker(sandboxProcessManagerReadyPollDelay)
	defer ticker.Stop()

	for {
		instance = s.refreshContainerInstance(containerId, instance)
		if instance.SandboxProcessManagerReady {
			return instance, nil
		}

		select {
		case <-instance.ProcessManagerReadyChan:
			instance = s.refreshContainerInstance(containerId, instance)
			if instance.SandboxProcessManagerReady {
				return instance, nil
			}
			return instance, errors.New("Process manager failed to become ready")
		case <-ticker.C:
		case <-ctx.Done():
			return instance, sandboxProcessManagerWaitError(ctx.Err())
		}
	}
}

func sandboxProcessManagerWaitError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.New("Process manager not ready within timeout")
	}
	if errors.Is(err, context.Canceled) {
		return errors.New("Request cancelled")
	}
	return err
}

func (s *ContainerRuntimeServer) refreshContainerInstance(containerId string, fallback *ContainerInstance) *ContainerInstance {
	if fresh, exists := s.containerInstances.Get(containerId); exists {
		return fresh
	}
	return fallback
}

func sandboxProcessMarkKey(containerId string, pid int32) string {
	return fmt.Sprintf("%s:%d", containerId, pid)
}

func (s *ContainerRuntimeServer) markSandboxProcessExited(containerId string, pid int32) {
	s.killedSandboxProcesses.Store(sandboxProcessMarkKey(containerId, pid), time.Now())
}

func (s *ContainerRuntimeServer) clearSandboxProcessExited(containerId string, pid int32) {
	s.killedSandboxProcesses.Delete(sandboxProcessMarkKey(containerId, pid))
}

func (s *ContainerRuntimeServer) sandboxProcessMarkedExited(containerId string, pid int32) bool {
	key := sandboxProcessMarkKey(containerId, pid)
	value, ok := s.killedSandboxProcesses.Load(key)
	if !ok {
		return false
	}

	markedAt, ok := value.(time.Time)
	if !ok || time.Since(markedAt) > 10*time.Minute {
		s.killedSandboxProcesses.Delete(key)
		return false
	}

	return true
}

func (s *ContainerRuntimeServer) ContainerSandboxStatus(ctx context.Context, in *pb.ContainerSandboxStatusRequest) (*pb.ContainerSandboxStatusResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	if in.Pid == 0 {
		status := "pending"
		if instance.SandboxProcessManagerReady {
			status = "running"
		}

		return &pb.ContainerSandboxStatusResponse{
			Ok:       true,
			Status:   status,
			ExitCode: -1,
		}, nil
	}

	if !instance.SandboxProcessManagerReady {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Sandbox process manager is not ready",
		}, nil
	}

	client, cleanup, err := s.sandboxProcessManagerClient(ctx, in.ContainerId, instance)
	if err != nil {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	if cleanup {
		defer client.Cleanup()
	}

	exitCode, err := client.Status(int(in.Pid))
	if err != nil {
		return &pb.ContainerSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	if exitCode >= 0 {
		s.clearSandboxProcessExited(in.ContainerId, in.Pid)
	} else if s.sandboxProcessMarkedExited(in.ContainerId, in.Pid) {
		exitCode = sandboxMissingProcessExitCode
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

	if !instance.SandboxProcessManagerReady {
		return &pb.ContainerSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: "Sandbox process manager is not ready",
		}, nil
	}

	client, cleanup, err := s.sandboxProcessManagerClient(ctx, in.ContainerId, instance)
	if err != nil {
		return &pb.ContainerSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	if cleanup {
		defer client.Cleanup()
	}

	stdout, err := client.Stdout(int(in.Pid))
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

	if !instance.SandboxProcessManagerReady {
		return &pb.ContainerSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: "Sandbox process manager is not ready",
		}, nil
	}

	client, cleanup, err := s.sandboxProcessManagerClient(ctx, in.ContainerId, instance)
	if err != nil {
		return &pb.ContainerSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	if cleanup {
		defer client.Cleanup()
	}

	stderr, err := client.Stderr(int(in.Pid))
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

	if !instance.SandboxProcessManagerReady {
		return &pb.ContainerSandboxKillResponse{Ok: false, ErrorMsg: "Sandbox process manager is not ready"}, nil
	}

	client, cleanup, err := s.sandboxProcessManagerClient(ctx, in.ContainerId, instance)
	if err != nil {
		return &pb.ContainerSandboxKillResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if cleanup {
		defer client.Cleanup()
	}

	err = client.Kill(int(in.Pid))
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Int32("pid", in.Pid).Msgf("failed to kill sandbox process: %v", err)
		return &pb.ContainerSandboxKillResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	missing, err := waitForSandboxProcessMissing(ctx, client, in.Pid, 3*time.Second)
	if err != nil {
		log.Debug().
			Str("container_id", in.ContainerId).
			Int32("pid", in.Pid).
			Err(err).
			Msg("failed to confirm killed sandbox process exit")
	} else if missing {
		s.markSandboxProcessExited(in.ContainerId, in.Pid)
	}

	return &pb.ContainerSandboxKillResponse{Ok: true}, nil
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

	instance, err := s.waitForSandboxProcessManager(ctx, in.ContainerId, instance)
	if err != nil {
		return &pb.ContainerSandboxListProcessesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	processes := make([]*pb.ProcessInfo, 0)
	client, cleanup, err := s.sandboxProcessManagerClient(ctx, in.ContainerId, instance)
	if err != nil {
		return &pb.ContainerSandboxListProcessesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	if cleanup {
		defer client.Cleanup()
	}

	ps, err := client.ListProcesses()
	if err != nil {
		return &pb.ContainerSandboxListProcessesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	for _, process := range ps {
		exitCode := int32(process.ExitCode)
		running := process.Running
		if exitCode >= 0 {
			s.clearSandboxProcessExited(in.ContainerId, int32(process.Pid))
		} else if running && s.sandboxProcessMarkedExited(in.ContainerId, int32(process.Pid)) {
			running = false
			exitCode = sandboxMissingProcessExitCode
		}
		pid := int32(process.Pid)
		processes = append(processes, &pb.ProcessInfo{Pid: pid, ExitCode: exitCode, Cwd: process.Cwd, Cmd: process.Cmd, Env: process.Env, Running: running})
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
		tempHostPath := filepath.Join(types.WorkerContainerUploadsHostPath, in.ContainerId, tempFile)
		if err := os.WriteFile(tempHostPath, in.Data, os.FileMode(in.Mode)); err != nil {
			return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}

		tempContainerPath := filepath.Join(types.WorkerContainerUploadsMountPath, tempFile)
		cmd := fmt.Sprintf("mkdir -p %s && mv %s %s && chmod %o %s",
			common.ShellQuote(filepath.Dir(containerPath)),
			common.ShellQuote(tempContainerPath),
			common.ShellQuote(containerPath),
			in.Mode,
			common.ShellQuote(containerPath),
		)

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
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to create directory for %s: %s", containerPath, err.Error())}, nil
	}
	if err := os.WriteFile(hostPath, in.Data, os.FileMode(in.Mode)); err != nil {
		return &pb.ContainerSandboxUploadFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to write file to %s: %s", containerPath, err.Error())}, nil
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
		return &pb.ContainerSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to create directory %s: %s", containerPath, err.Error())}, nil
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
		return &pb.ContainerSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to delete directory %s: %s", containerPath, err.Error())}, nil
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
		return &pb.ContainerSandboxDownloadFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to read file from %s: %s", containerPath, err.Error())}, nil
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
		return &pb.ContainerSandboxDeleteFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to delete file %s: %s", containerPath, err.Error())}, nil
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
		return &pb.ContainerSandboxStatFileResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to stat file %s: %s", containerPath, err.Error())}, nil
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
		return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to list files in %s: %s", containerPath, err.Error())}, nil
	}

	responseFiles := make([]*pb.FileInfo, 0)
	for _, file := range files {
		stat, err := file.Info()
		if err != nil {
			return &pb.ContainerSandboxListFilesResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to get file info for %s: %s", file.Name(), err.Error())}, nil
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

	s.exposePortMu.Lock()
	defer s.exposePortMu.Unlock()

	getAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.GetContainerAddressMap(context.Background(), &pb.GetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
	}))
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	addressMap := writableContainerAddressMap(getAddressMapResponse.AddressMap)
	port := int32(in.Port)
	if existingTarget, exists := addressMap[port]; exists {
		if route := s.backendRouteForContainerPort(instance, port, existingTarget); route != nil {
			setAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
				ContainerId: in.ContainerId,
				AddressMap:  addressMap,
				Routes:      []*pb.BackendRoute{route},
			}))
			if err != nil {
				return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
			}
			if !setAddressMapResponse.Ok {
				return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: setAddressMapResponse.ErrorMsg}, nil
			}
		}
		recordSandboxExposedPort(s.containerInstances, in.ContainerId, instance, uint32(in.Port))
		return &pb.ContainerSandboxExposePortResponse{Ok: true}, nil
	}

	bindPort, err := getRandomFreePort()
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	if s.containerNetworkManager == nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: "container network manager unavailable"}, nil
	}

	binding := PortBinding{HostPort: bindPort, ContainerPort: int(in.Port)}
	err = s.containerNetworkManager.ExposePort(in.ContainerId, binding.HostPort, binding.ContainerPort)
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to expose container bind port: %v", err)
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	localTarget, err := s.containerPortAddress(in.ContainerId, binding)
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}
	addressMap[port] = localTarget
	routes := make([]*pb.BackendRoute, 0, 1)
	if route := s.backendRouteForContainerPort(instance, port, localTarget); route != nil {
		routes = append(routes, route)
	}
	setAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
		AddressMap:  addressMap,
		Routes:      routes,
	}))
	if err != nil {
		return &pb.ContainerSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	recordSandboxExposedPort(s.containerInstances, in.ContainerId, instance, uint32(in.Port))

	log.Info().Str("container_id", in.ContainerId).Msgf("exposed sandbox port %d to %s", in.Port, addressMap[port])

	return &pb.ContainerSandboxExposePortResponse{Ok: setAddressMapResponse.Ok}, err
}

func (s *ContainerRuntimeServer) containerPortAddress(containerId string, binding PortBinding) (string, error) {
	if s.containerNetworkManager == nil {
		return "", fmt.Errorf("container network manager unavailable")
	}
	return s.containerNetworkManager.ContainerPortAddress(containerId, binding)
}

func (s *ContainerRuntimeServer) backendRouteForContainerPort(instance *ContainerInstance, port int32, localTarget string) *pb.BackendRoute {
	if s.backendRoute == nil || instance == nil || instance.Request == nil || localTarget == "" {
		return nil
	}
	if _, isRoute := types.ParseBackendRouteAddress(localTarget); isRoute {
		return nil
	}
	return s.backendRoute(instance.Request, types.BackendRouteKindContainer, port, localTarget)
}

func writableContainerAddressMap(addressMap map[int32]string) map[int32]string {
	cloned := cloneContainerAddressMap(addressMap)
	if cloned == nil {
		return map[int32]string{}
	}

	return cloned
}

func recordSandboxExposedPort(containerInstances *common.SafeMap[*ContainerInstance], containerId string, instance *ContainerInstance, port uint32) {
	if instance == nil || instance.Request == nil || slices.Contains(instance.Request.Ports, port) {
		return
	}

	instance.Request.Ports = append(instance.Request.Ports, port)
	containerInstances.Set(containerId, instance)
}

func (s *ContainerRuntimeServer) ContainerSandboxUpdateNetworkPermissions(ctx context.Context, in *pb.ContainerSandboxUpdateNetworkPermissionsRequest) (*pb.ContainerSandboxUpdateNetworkPermissionsResponse, error) {
	_, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.ContainerSandboxUpdateNetworkPermissionsResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.ContainerSandboxUpdateNetworkPermissionsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	// Create a container request with the new network permissions
	request := &types.ContainerRequest{
		BlockNetwork: in.BlockNetwork,
		AllowList:    in.AllowList,
	}

	if s.containerNetworkManager == nil {
		return &pb.ContainerSandboxUpdateNetworkPermissionsResponse{Ok: false, ErrorMsg: "container network manager unavailable"}, nil
	}

	// Update network permissions via the network manager
	err = s.containerNetworkManager.UpdateNetworkPermissions(in.ContainerId, request)
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to update network permissions: %v", err)
		return &pb.ContainerSandboxUpdateNetworkPermissionsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	log.Info().Str("container_id", in.ContainerId).Msgf("updated network permissions: block_network=%v, allow_list=%v", in.BlockNetwork, in.AllowList)

	return &pb.ContainerSandboxUpdateNetworkPermissionsResponse{Ok: true}, nil
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
		return &pb.ContainerSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to replace in files at %s: %s", containerPath, err.Error())}, nil
	}

	for _, stagedFile := range stagedFiles {
		err = os.WriteFile(stagedFile.Path, []byte(stagedFile.Content), 0644)
		if err != nil {
			return &pb.ContainerSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: fmt.Sprintf("failed to write replaced file %s: %s", stagedFile.Path, err.Error())}, nil
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
			ErrorMsg: fmt.Sprintf("failed to search for '%s' in %s: ripgrep failed: %v, stderr: %s", in.Pattern, containerPath, err, stderr.String()),
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
