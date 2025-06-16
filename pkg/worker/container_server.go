package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/go-runc"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/shlex"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	containerEventsInterval time.Duration = 5 * time.Second
)

type ContainerServer interface {
	Start() error
	Stop() error
	GetPort() int
	Kill(ctx context.Context, containerId string, signal int) error
	Exec(ctx context.Context, containerId string, cmd string, env []string, cwd string, interactive bool) error
	Status(ctx context.Context, containerId string) (bool, error)
	StreamLogs(containerId string, stream pb.RunCService_RunCStreamLogsServer) error
	Archive(ctx context.Context, containerId string, imageId string, stream pb.RunCService_RunCArchiveServer) error
	SyncWorkspace(ctx context.Context, containerId string, op pb.SyncContainerWorkspaceOperation, path string, newPath string, data []byte, isDir bool) error
	SandboxExec(ctx context.Context, containerId string, cmd string, env map[string]string, cwd string, interactive bool) (int32, error)
	SandboxStatus(ctx context.Context, containerId string, pid int32) (string, int32, error)
	SandboxStdout(ctx context.Context, containerId string, pid int32) (string, error)
	SandboxStderr(ctx context.Context, containerId string, pid int32) (string, error)
	SandboxKill(ctx context.Context, containerId string, pid int32) error
	SandboxUploadFile(ctx context.Context, containerId string, containerPath string, data []byte, mode uint32) error
	SandboxDownloadFile(ctx context.Context, containerId string, containerPath string) ([]byte, error)
	SandboxDeleteFile(ctx context.Context, containerId string, containerPath string) error
	SandboxStatFile(ctx context.Context, containerId string, containerPath string) (*pb.FileInfo, error)
	SandboxListFiles(ctx context.Context, containerId string, containerPath string) ([]*pb.FileInfo, error)
	SandboxExposePort(ctx context.Context, containerId string, port int32) (string, error)
	StartSandbox(ctx context.Context, containerId string, cmd string, env []string, cwd string, interactive bool) (int, error)
	StopSandbox(ctx context.Context, containerId string, pid int) error
	ListSandboxProcesses(ctx context.Context, containerId string) ([]int, error)
}

// BaseContainerServer provides common functionality for container servers
type BaseContainerServer struct {
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerRepoClient     pb.ContainerRepositoryServiceClient
	containerNetworkManager *ContainerNetworkManager
	imageClient             *ImageClient
	port                    int
	podAddr                 string
	grpcServer              *grpc.Server
	mu                      sync.Mutex
}

// NewBaseContainerServer creates a new base container server
func NewBaseContainerServer(podAddr string, containerInstances *common.SafeMap[*ContainerInstance], imageClient *ImageClient, containerRepoClient pb.ContainerRepositoryServiceClient, containerNetworkManager *ContainerNetworkManager) *BaseContainerServer {
	return &BaseContainerServer{
		podAddr:                 podAddr,
		containerInstances:      containerInstances,
		imageClient:             imageClient,
		containerRepoClient:     containerRepoClient,
		containerNetworkManager: containerNetworkManager,
	}
}

// Start starts the gRPC server
func (s *BaseContainerServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.grpcServer != nil {
		return errors.New("server already started")
	}

	listener, err := net.Listen("tcp", ":0") // Random free port
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s.port = listener.Addr().(*net.TCPAddr).Port
	log.Info().Int("port", s.port).Msg("container server started")

	s.grpcServer = grpc.NewServer()

	go func() {
		err := s.grpcServer.Serve(listener)
		if err != nil {
			log.Error().Err(err).Msg("failed to start grpc server")
			os.Exit(1)
		}
	}()

	return nil
}

// Stop stops the gRPC server
func (s *BaseContainerServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.grpcServer == nil {
		return nil
	}

	s.grpcServer.GracefulStop()
	s.grpcServer = nil
	return nil
}

// GetPort returns the port the server is listening on
func (s *BaseContainerServer) GetPort() int {
	return s.port
}

// waitForContainer waits for a container to be ready
func (s *BaseContainerServer) waitForContainer(ctx context.Context, containerId string) error {
	for {
		instance, exists := s.containerInstances.Get(containerId)
		if !exists {
			return errors.New("container not found")
		}

		if instance.Spec == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// This will be implemented by the specific runtime
		state, err := s.getContainerState(ctx, containerId)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if state.Pid != 0 && state.Status == string(types.ContainerStatusRunning) {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// getContainerState is implemented by specific runtimes
func (s *BaseContainerServer) getContainerState(ctx context.Context, containerId string) (*ContainerState, error) {
	return nil, errors.New("not implemented")
}

// RuncServer implements ContainerServer for runc
type RuncServer struct {
	*BaseContainerServer
	runcHandle     runc.Runc
	baseConfigSpec specs.Spec
}

// NewRuncServer creates a new runc server
func NewRuncServer(podAddr string, containerInstances *common.SafeMap[*ContainerInstance], imageClient *ImageClient, containerRepoClient pb.ContainerRepositoryServiceClient, containerNetworkManager *ContainerNetworkManager) (*RuncServer, error) {
	var baseConfigSpec specs.Spec
	specTemplate := strings.TrimSpace(string(baseRuncConfigRaw))
	err := json.Unmarshal([]byte(specTemplate), &baseConfigSpec)
	if err != nil {
		return nil, err
	}

	baseServer := NewBaseContainerServer(podAddr, containerInstances, imageClient, containerRepoClient, containerNetworkManager)
	return &RuncServer{
		BaseContainerServer: baseServer,
		runcHandle:          runc.Runc{},
		baseConfigSpec:      baseConfigSpec,
	}, nil
}

// Start starts the gRPC server
func (s *RuncServer) Start() error {
	if err := s.BaseContainerServer.Start(); err != nil {
		return err
	}

	// Register the gRPC server
	pb.RegisterRunCServiceServer(s.grpcServer, s)
	return nil
}

// Kill kills a container
func (s *RuncServer) Kill(ctx context.Context, containerId string, signal int) error {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return fmt.Errorf("container not found")
	}

	if instance.Process == nil {
		return fmt.Errorf("container process not found")
	}

	return instance.Process.Signal(syscall.Signal(signal))
}

// Exec executes a command in a container
func (s *RuncServer) Exec(ctx context.Context, containerId string, cmd string, env []string, cwd string, interactive bool) error {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return errors.New("container not found")
	}

	parsedCmd, err := shlex.Split(cmd)
	if err != nil {
		return err
	}

	process := s.baseConfigSpec.Process
	process.Args = parsedCmd
	process.Cwd = cwd
	if cwd == "" {
		process.Cwd = instance.Spec.Process.Cwd
	}

	process.Env = append(instance.Spec.Process.Env, env...)

	if instance.Request.IsBuildRequest() {
		// For build containers, we need to use a background context to avoid issues with container shutdown
		ctx = context.Background()
		process.Env = append(process.Env, instance.Request.BuildOptions.BuildSecrets...)
	}

	return s.runcHandle.Exec(ctx, containerId, *process, &runc.ExecOpts{
		OutputWriter: instance.OutputWriter,
	})
}

// Status gets the status of a container
func (s *RuncServer) Status(ctx context.Context, containerId string) (bool, error) {
	state, err := s.runcHandle.State(ctx, containerId)
	if err != nil {
		return false, nil
	}

	return state.Status == types.RuncContainerStatusRunning, nil
}

// getContainerState implements the base server's getContainerState method
func (s *RuncServer) getContainerState(ctx context.Context, containerId string) (*ContainerState, error) {
	state, err := s.runcHandle.State(ctx, containerId)
	if err != nil {
		return nil, err
	}
	return &ContainerState{
		Status: state.Status,
		Pid:    state.Pid,
	}, nil
}

// StreamLogs streams logs from a container
func (s *RuncServer) StreamLogs(containerId string, stream pb.RunCService_RunCStreamLogsServer) error {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return errors.New("container not found")
	}

	buffer := make([]byte, 4096)
	logEntry := &pb.RunCLogEntry{}

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

// Archive archives a container's filesystem
func (s *RuncServer) Archive(ctx context.Context, containerId string, imageId string, stream pb.RunCService_RunCArchiveServer) error {
	state, err := s.runcHandle.State(ctx, containerId)
	if err != nil {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	if state.Status != types.RuncContainerStatusRunning {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not running"})
	}

	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	// Copy initial config file from the base image bundle
	err = copyFile(filepath.Join(instance.BundlePath, specBaseName), filepath.Join(instance.Overlay.TopLayerPath(), initialSpecBaseName))
	if err != nil {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
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
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	configPath := filepath.Join(instance.Overlay.TopLayerPath(), specBaseName)
	err = os.WriteFile(configPath, file, 0644)
	if err != nil {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	progressChan := make(chan int)
	doneChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		lastProgress := -1

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
					err := stream.Send(&pb.RunCArchiveResponse{Done: false, Success: false, Progress: int32(progress), ErrorMsg: ""})
					if err != nil {
						return
					}
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
	err = stream.Send(&pb.RunCArchiveResponse{
		Done: true, Success: s.imageClient.Archive(ctx, topLayerPath, imageId, progressChan) == nil,
	})

	close(doneChan)
	return err
}

// addRequestEnvToInitialSpec adds environment variables from the request to the initial spec
func (s *RuncServer) addRequestEnvToInitialSpec(instance *ContainerInstance) error {
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

// SyncWorkspace syncs files with the container's workspace
func (s *RuncServer) SyncWorkspace(ctx context.Context, containerId string, op pb.SyncContainerWorkspaceOperation, path string, newPath string, data []byte, isDir bool) error {
	_, exists := s.containerInstances.Get(containerId)
	if !exists {
		return errors.New("container not found")
	}

	workspacePath := types.TempContainerWorkspace(containerId)
	destPath := filepath.Join(workspacePath, path)
	destNewPath := filepath.Join(workspacePath, newPath)

	switch op {
	case pb.SyncContainerWorkspaceOperation_DELETE:
		if err := os.RemoveAll(destPath); err != nil {
			return err
		}
	case pb.SyncContainerWorkspaceOperation_WRITE:
		if isDir {
			if err := os.MkdirAll(destPath, 0755); err != nil {
				return err
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return err
			}
			if err := os.WriteFile(destPath, data, 0644); err != nil {
				return err
			}
		}
	case pb.SyncContainerWorkspaceOperation_MOVED:
		if err := os.MkdirAll(filepath.Dir(destNewPath), 0755); err != nil {
			return err
		}
		if err := os.Rename(destPath, destNewPath); err != nil {
			return err
		}
	}

	return nil
}

// GvisorServer implements ContainerServer for gvisor
type GvisorServer struct {
	*BaseContainerServer
	config types.AppConfig
}

// NewGvisorServer creates a new gvisor server
func NewGvisorServer(podAddr string, containerInstances *common.SafeMap[*ContainerInstance], imageClient *ImageClient, containerRepoClient pb.ContainerRepositoryServiceClient, containerNetworkManager *ContainerNetworkManager, config types.AppConfig) (*GvisorServer, error) {
	baseServer := NewBaseContainerServer(podAddr, containerInstances, imageClient, containerRepoClient, containerNetworkManager)
	return &GvisorServer{
		BaseContainerServer: baseServer,
		config:              config,
	}, nil
}

// Start starts the gRPC server
func (s *GvisorServer) Start() error {
	if err := s.BaseContainerServer.Start(); err != nil {
		return err
	}

	// Register the gRPC server
	pb.RegisterRunCServiceServer(s.grpcServer, s)
	return nil
}

// Kill kills a container
func (s *GvisorServer) Kill(ctx context.Context, containerId string, signal int) error {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return fmt.Errorf("container not found")
	}

	if instance.Process == nil {
		return fmt.Errorf("container process not found")
	}

	return instance.Process.Signal(syscall.Signal(signal))
}

// Exec executes a command in a container
func (s *GvisorServer) Exec(ctx context.Context, containerId string, cmd string, env []string, cwd string, interactive bool) error {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return errors.New("container not found")
	}

	parsedCmd, err := shlex.Split(cmd)
	if err != nil {
		return err
	}

	args := []string{
		"--root", "/run/gvisor",
		"exec",
		"--pid-file", filepath.Join("/run/gvisor", containerId, "exec.pid"),
	}

	// Add process args
	args = append(args, containerId)
	args = append(args, parsedCmd...)

	execCmd := exec.CommandContext(ctx, "runsc", args...)

	// Set up environment
	if len(env) > 0 {
		execCmd.Env = append(os.Environ(), env...)
	}

	// Set up working directory
	if cwd != "" {
		execCmd.Dir = cwd
	} else {
		execCmd.Dir = instance.Spec.Process.Cwd
	}

	// Set up I/O
	if instance.OutputWriter != nil {
		execCmd.Stdout = instance.OutputWriter
		execCmd.Stderr = instance.OutputWriter
	}

	// Start the process
	if err := execCmd.Start(); err != nil {
		return fmt.Errorf("failed to start exec process: %v", err)
	}

	// Wait for process to exit
	return execCmd.Wait()
}

// Status gets the status of a container
func (s *GvisorServer) Status(ctx context.Context, containerId string) (bool, error) {
	cmd := exec.CommandContext(ctx, "runsc",
		"--root", "/run/gvisor",
		"state",
		containerId,
	)

	output, err := cmd.Output()
	if err != nil {
		return false, nil
	}

	// Parse state output to get status
	// This is a simplified version - you'll need to properly parse the JSON output
	return strings.Contains(string(output), "running"), nil
}

// getContainerState implements the base server's getContainerState method
func (s *GvisorServer) getContainerState(ctx context.Context, containerId string) (*ContainerState, error) {
	cmd := exec.CommandContext(ctx, "runsc",
		"--root", "/run/gvisor",
		"state",
		containerId,
	)

	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	// Parse state output to get status and PID
	// This is a simplified version - you'll need to properly parse the JSON output
	state := &ContainerState{
		Status: "running", // You'll need to parse this from the actual output
		Pid:    0,         // You'll need to parse this from the actual output
	}

	return state, nil
}

// StreamLogs streams logs from a container
func (s *GvisorServer) StreamLogs(containerId string, stream pb.RunCService_RunCStreamLogsServer) error {
	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return errors.New("container not found")
	}

	buffer := make([]byte, 4096)
	logEntry := &pb.RunCLogEntry{}

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

// Archive archives a container's filesystem
func (s *GvisorServer) Archive(ctx context.Context, containerId string, imageId string, stream pb.RunCService_RunCArchiveServer) error {
	state, err := s.getContainerState(ctx, containerId)
	if err != nil {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	if state.Status != "running" {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not running"})
	}

	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	// Copy initial config file from the base image bundle
	err = copyFile(filepath.Join(instance.BundlePath, specBaseName), filepath.Join(instance.Overlay.TopLayerPath(), initialSpecBaseName))
	if err != nil {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
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
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	configPath := filepath.Join(instance.Overlay.TopLayerPath(), specBaseName)
	err = os.WriteFile(configPath, file, 0644)
	if err != nil {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: err.Error()})
	}

	progressChan := make(chan int)
	doneChan := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		lastProgress := -1

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
					err := stream.Send(&pb.RunCArchiveResponse{Done: false, Success: false, Progress: int32(progress), ErrorMsg: ""})
					if err != nil {
						return
					}
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
	err = stream.Send(&pb.RunCArchiveResponse{
		Done: true, Success: s.imageClient.Archive(ctx, topLayerPath, imageId, progressChan) == nil,
	})

	close(doneChan)
	return err
}

// addRequestEnvToInitialSpec adds environment variables from the request to the initial spec
func (s *GvisorServer) addRequestEnvToInitialSpec(instance *ContainerInstance) error {
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

// SyncWorkspace syncs files with the container's workspace
func (s *GvisorServer) SyncWorkspace(ctx context.Context, containerId string, op pb.SyncContainerWorkspaceOperation, path string, newPath string, data []byte, isDir bool) error {
	_, exists := s.containerInstances.Get(containerId)
	if !exists {
		return errors.New("container not found")
	}

	workspacePath := types.TempContainerWorkspace(containerId)
	destPath := filepath.Join(workspacePath, path)
	destNewPath := filepath.Join(workspacePath, newPath)

	switch op {
	case pb.SyncContainerWorkspaceOperation_DELETE:
		if err := os.RemoveAll(destPath); err != nil {
			return err
		}
	case pb.SyncContainerWorkspaceOperation_WRITE:
		if isDir {
			if err := os.MkdirAll(destPath, 0755); err != nil {
				return err
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return err
			}
			if err := os.WriteFile(destPath, data, 0644); err != nil {
				return err
			}
		}
	case pb.SyncContainerWorkspaceOperation_MOVED:
		if err := os.MkdirAll(filepath.Dir(destNewPath), 0755); err != nil {
			return err
		}
		if err := os.Rename(destPath, destNewPath); err != nil {
			return err
		}
	}

	return nil
}

// mustEmbedUnimplementedRunCServiceServer implements the RunCServiceServer interface
func (s *RuncServer) mustEmbedUnimplementedRunCServiceServer() {}

// mustEmbedUnimplementedRunCServiceServer implements the RunCServiceServer interface
func (s *GvisorServer) mustEmbedUnimplementedRunCServiceServer() {}

// RunCArchive implements the RunCServiceServer interface
func (s *RuncServer) RunCArchive(req *pb.RunCArchiveRequest, stream pb.RunCService_RunCArchiveServer) error {
	return s.Archive(stream.Context(), req.ContainerId, req.ImageId, stream)
}

// RunCArchive implements the RunCServiceServer interface
func (s *GvisorServer) RunCArchive(req *pb.RunCArchiveRequest, stream pb.RunCService_RunCArchiveServer) error {
	return s.Archive(stream.Context(), req.ContainerId, req.ImageId, stream)
}

// RunCKill implements the RunCServiceServer interface
func (s *RuncServer) RunCKill(ctx context.Context, req *pb.RunCKillRequest) (*pb.RunCKillResponse, error) {
	err := s.Kill(ctx, req.ContainerId, syscall.SIGKILL)
	if err != nil {
		return nil, err
	}
	return &pb.RunCKillResponse{Ok: true}, nil
}

// RunCKill implements the RunCServiceServer interface
func (s *GvisorServer) RunCKill(ctx context.Context, req *pb.RunCKillRequest) (*pb.RunCKillResponse, error) {
	err := s.Kill(ctx, req.ContainerId, syscall.SIGKILL)
	if err != nil {
		return nil, err
	}
	return &pb.RunCKillResponse{Ok: true}, nil
}

// RunCStreamLogs implements the RunCServiceServer interface
func (s *GvisorServer) RunCStreamLogs(req *pb.RunCStreamLogsRequest, stream pb.RunCService_RunCStreamLogsServer) error {
	logChan := make(chan string)
	go func() {
		for log := range logChan {
			err := stream.Send(&pb.RunCLogEntry{
				Msg: log,
			})
			if err != nil {
				return
			}
		}
	}()

	err := s.StreamLogs(stream.Context(), req.ContainerId, logChan)
	close(logChan)
	if err != nil {
		return err
	}
	return nil
}

// RunCSyncWorkspace implements the RunCServiceServer interface
func (s *GvisorServer) RunCSyncWorkspace(ctx context.Context, req *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
	err := s.SyncWorkspace(ctx, req.ContainerId, req.Op, req.Path, req.NewPath, req.Data, req.IsDir)
	if err != nil {
		return nil, err
	}
	return &pb.SyncContainerWorkspaceResponse{Ok: true}, nil
}

// RunCSandboxExec implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxExec(ctx context.Context, req *pb.RunCSandboxExecRequest) (*pb.RunCSandboxExecResponse, error) {
	// Convert map[string]string to []string
	env := make([]string, 0, len(req.Env))
	for k, v := range req.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	pid, err := s.SandboxExec(ctx, req.ContainerId, req.Cmd, env, req.Cwd, req.Interactive)
	if err != nil {
		return &pb.RunCSandboxExecResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxExecResponse{
		Ok:  true,
		Pid: pid,
	}, nil
}

// RunCSandboxStatus implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxStatus(ctx context.Context, req *pb.RunCSandboxStatusRequest) (*pb.RunCSandboxStatusResponse, error) {
	status, exitCode, err := s.SandboxStatus(ctx, req.ContainerId, req.Pid)
	if err != nil {
		return &pb.RunCSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxStatusResponse{
		Ok:       true,
		Status:   status,
		ExitCode: exitCode,
	}, nil
}

// RunCSandboxStdout implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxStdout(ctx context.Context, req *pb.RunCSandboxStdoutRequest) (*pb.RunCSandboxStdoutResponse, error) {
	data, err := s.SandboxStdout(ctx, req.ContainerId, req.Pid)
	if err != nil {
		return &pb.RunCSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxStdoutResponse{
		Ok:   true,
		Data: []byte(data),
	}, nil
}

// RunCSandboxStderr implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxStderr(ctx context.Context, req *pb.RunCSandboxStderrRequest) (*pb.RunCSandboxStderrResponse, error) {
	data, err := s.SandboxStderr(ctx, req.ContainerId, req.Pid)
	if err != nil {
		return &pb.RunCSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxStderrResponse{
		Ok:   true,
		Data: []byte(data),
	}, nil
}

// RunCSandboxKill implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxKill(ctx context.Context, req *pb.RunCSandboxKillRequest) (*pb.RunCSandboxKillResponse, error) {
	err := s.SandboxKill(ctx, req.ContainerId, req.Pid)
	if err != nil {
		return &pb.RunCSandboxKillResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxKillResponse{Ok: true}, nil
}

// RunCSandboxListFiles implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxListFiles(ctx context.Context, req *pb.RunCSandboxListFilesRequest) (*pb.RunCSandboxListFilesResponse, error) {
	files, err := s.SandboxListFiles(ctx, req.ContainerId, req.Path)
	if err != nil {
		return &pb.RunCSandboxListFilesResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxListFilesResponse{
		Ok:    true,
		Files: files,
	}, nil
}

// RunCSandboxUploadFile implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxUploadFile(ctx context.Context, req *pb.RunCSandboxUploadFileRequest) (*pb.RunCSandboxUploadFileResponse, error) {
	err := s.SandboxUploadFile(ctx, req.ContainerId, req.Path, req.Data, req.Mode)
	if err != nil {
		return &pb.RunCSandboxUploadFileResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxUploadFileResponse{Ok: true}, nil
}

// RunCSandboxDownloadFile implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxDownloadFile(ctx context.Context, req *pb.RunCSandboxDownloadFileRequest) (*pb.RunCSandboxDownloadFileResponse, error) {
	data, err := s.SandboxDownloadFile(ctx, req.ContainerId, req.Path)
	if err != nil {
		return &pb.RunCSandboxDownloadFileResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxDownloadFileResponse{
		Ok:   true,
		Data: data,
	}, nil
}

// RunCSandboxStatFile implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxStatFile(ctx context.Context, req *pb.RunCSandboxStatFileRequest) (*pb.RunCSandboxStatFileResponse, error) {
	info, err := s.SandboxStatFile(ctx, req.ContainerId, req.Path)
	if err != nil {
		return &pb.RunCSandboxStatFileResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxStatFileResponse{
		Ok:   true,
		Info: info,
	}, nil
}

// RunCSandboxDeleteFile implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxDeleteFile(ctx context.Context, req *pb.RunCSandboxDeleteFileRequest) (*pb.RunCSandboxDeleteFileResponse, error) {
	err := s.SandboxDeleteFile(ctx, req.ContainerId, req.Path)
	if err != nil {
		return &pb.RunCSandboxDeleteFileResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxDeleteFileResponse{Ok: true}, nil
}

// RunCSandboxCreateDirectory implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxCreateDirectory(ctx context.Context, req *pb.RunCSandboxCreateDirectoryRequest) (*pb.RunCSandboxCreateDirectoryResponse, error) {
	err := os.MkdirAll(filepath.Join(types.TempContainerWorkspace(req.ContainerId), req.Path), 0755)
	if err != nil {
		return &pb.RunCSandboxCreateDirectoryResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxCreateDirectoryResponse{Ok: true}, nil
}

// RunCSandboxDeleteDirectory implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxDeleteDirectory(ctx context.Context, req *pb.RunCSandboxDeleteDirectoryRequest) (*pb.RunCSandboxDeleteDirectoryResponse, error) {
	err := os.RemoveAll(filepath.Join(types.TempContainerWorkspace(req.ContainerId), req.Path))
	if err != nil {
		return &pb.RunCSandboxDeleteDirectoryResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxDeleteDirectoryResponse{Ok: true}, nil
}

// RunCSandboxExposePort implements the RunCServiceServer interface
func (s *GvisorServer) RunCSandboxExposePort(ctx context.Context, req *pb.RunCSandboxExposePortRequest) (*pb.RunCSandboxExposePortResponse, error) {
	port := int(req.Port)
	err := s.containerNetworkManager.ExposePort(ctx, req.ContainerId, port)
	if err != nil {
		return &pb.RunCSandboxExposePortResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}
	return &pb.RunCSandboxExposePortResponse{Ok: true}, nil
}

// RunCStatus implements the RunCServiceServer interface
func (s *GvisorServer) RunCStatus(ctx context.Context, req *pb.RunCStatusRequest) (*pb.RunCStatusResponse, error) {
	running, err := s.Status(ctx, req.ContainerId)
	if err != nil {
		return nil, err
	}
	return &pb.RunCStatusResponse{Running: running}, nil
}

// RunCExec implements the RunCServiceServer interface
func (s *GvisorServer) RunCExec(ctx context.Context, req *pb.RunCExecRequest) (*pb.RunCExecResponse, error) {
	err := s.Exec(ctx, req.ContainerId, req.Cmd, req.Env, req.Cwd, req.Interactive)
	if err != nil {
		return nil, err
	}
	return &pb.RunCExecResponse{Ok: true}, nil
}
