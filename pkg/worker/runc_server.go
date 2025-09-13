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

	pb "github.com/beam-cloud/beta9/proto"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
)

type RunCServer struct {
	runcHandle     runc.Runc
	baseConfigSpec specs.Spec
	pb.UnimplementedRunCServiceServer
	containerInstances      *common.SafeMap[*ContainerInstance]
	containerRepoClient     pb.ContainerRepositoryServiceClient
	containerNetworkManager *ContainerNetworkManager
	imageClient             *ImageClient
	port                    int
	podAddr                 string
	createCheckpoint        func(ctx context.Context, opts *CreateCheckpointOpts) error
}

type RunCServerOpts struct {
	PodAddr                 string
	ContainerInstances      *common.SafeMap[*ContainerInstance]
	ImageClient             *ImageClient
	ContainerRepoClient     pb.ContainerRepositoryServiceClient
	ContainerNetworkManager *ContainerNetworkManager
	CreateCheckpoint        func(ctx context.Context, opts *CreateCheckpointOpts) error
}

func NewRunCServer(opts *RunCServerOpts) (*RunCServer, error) {
	var baseConfigSpec specs.Spec
	specTemplate := strings.TrimSpace(string(baseRuncConfigRaw))
	err := json.Unmarshal([]byte(specTemplate), &baseConfigSpec)
	if err != nil {
		return nil, err
	}

	return &RunCServer{
		runcHandle:              runc.Runc{},
		podAddr:                 opts.PodAddr,
		baseConfigSpec:          baseConfigSpec,
		containerInstances:      opts.ContainerInstances,
		imageClient:             opts.ImageClient,
		containerRepoClient:     opts.ContainerRepoClient,
		containerNetworkManager: opts.ContainerNetworkManager,
		createCheckpoint:        opts.CreateCheckpoint,
	}, nil
}

func (s *RunCServer) Start() error {
	listener, err := net.Listen("tcp", ":0") // // Random free port
	if err != nil {
		log.Error().Err(err).Msg("failed to listen")
		os.Exit(1)
	}

	s.port = listener.Addr().(*net.TCPAddr).Port
	log.Info().Int("port", s.port).Msg("runc server started")

	grpcServer := grpc.NewServer()
	pb.RegisterRunCServiceServer(grpcServer, s)

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Error().Err(err).Msg("failed to start grpc server")
			os.Exit(1)
		}
	}()

	return nil
}

func (s *RunCServer) RunCKill(ctx context.Context, in *pb.RunCKillRequest) (*pb.RunCKillResponse, error) {
	_ = s.runcHandle.Kill(ctx, in.ContainerId, int(syscall.SIGTERM), &runc.KillOpts{
		All: true,
	})

	err := s.runcHandle.Delete(ctx, in.ContainerId, &runc.DeleteOpts{
		Force: true,
	})

	return &pb.RunCKillResponse{
		Ok: err == nil,
	}, nil
}

// Execute an arbitary command inside a running container
func (s *RunCServer) RunCExec(ctx context.Context, in *pb.RunCExecRequest) (*pb.RunCExecResponse, error) {
	cmd := fmt.Sprintf("sh -c '%s'", in.Cmd)
	parsedCmd, err := shlex.Split(cmd)
	if err != nil {
		return &pb.RunCExecResponse{}, err
	}

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCExecResponse{Ok: false}, nil
	}

	process := s.baseConfigSpec.Process
	process.Args = parsedCmd
	process.Cwd = instance.Spec.Process.Cwd

	instanceSpec := instance.Spec.Process
	process.Env = append(instanceSpec.Env, in.Env...)

	if instance.Request.IsBuildRequest() {
		/*
			For some reason, if the process that is spun up from this (e.g. `runc exec --process /tmp/runc-process839505971 build-128a153e`)
			is canceled by the context (or just exits), the runc container can no longer be shut down by a `runc kill` command or
			a SIGKILL on the first process.

			For example in build containers, we do a `tail -f /dev/null` to keep the container running. But if the scenario above happens,
			the `tail` process does receive and act on a shut down signal but it is then `blocked and waiting` on some unknown channel to resolve
			which prevents the container from being shut down.
		*/
		ctx = context.Background()
		process.Env = append(process.Env, instance.Request.BuildOptions.BuildSecrets...)
	}

	err = s.runcHandle.Exec(ctx, in.ContainerId, *process, &runc.ExecOpts{
		OutputWriter: instance.OutputWriter,
	})

	return &pb.RunCExecResponse{
		Ok: err == nil,
	}, nil
}

func (s *RunCServer) RunCStatus(ctx context.Context, in *pb.RunCStatusRequest) (*pb.RunCStatusResponse, error) {
	state, err := s.runcHandle.State(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCStatusResponse{
			Running: false,
		}, nil
	}

	return &pb.RunCStatusResponse{
		Running: state.Status == types.RuncContainerStatusRunning,
	}, nil
}

func (s *RunCServer) RunCStreamLogs(req *pb.RunCStreamLogsRequest, stream pb.RunCService_RunCStreamLogsServer) error {
	instance, exists := s.containerInstances.Get(req.ContainerId)
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

func (s *RunCServer) RunCCheckpoint(ctx context.Context, in *pb.RunCCheckpointRequest) (*pb.RunCCheckpointResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCCheckpointResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	checkpointId := uuid.New().String()
	err := s.createCheckpoint(ctx, &CreateCheckpointOpts{
		Request:      instance.Request,
		CheckpointId: checkpointId,
		ContainerIp:  instance.ContainerIp,
	})
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to create checkpoint: %v", err)
		return &pb.RunCCheckpointResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCCheckpointResponse{Ok: true, CheckpointId: checkpointId}, nil
}

func (s *RunCServer) RunCArchive(req *pb.RunCArchiveRequest, stream pb.RunCService_RunCArchiveServer) error {
	ctx := stream.Context()
	state, err := s.runcHandle.State(ctx, req.ContainerId)
	if err != nil {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	if state.Status != types.RuncContainerStatusRunning {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not running"})
	}

	instance, exists := s.containerInstances.Get(req.ContainerId)
	if !exists {
		return stream.Send(&pb.RunCArchiveResponse{Done: true, Success: false, ErrorMsg: "Container not found"})
	}

	// If it's not a build request, we need to use the initial_config.json from the base image bundle
	// Otherwise, we pull in the modified config vars from the running container
	initialConfigPath := filepath.Join(instance.BundlePath, specBaseName)
	if !instance.Request.IsBuildRequest() {
		initialConfigPath = filepath.Join(instance.BundlePath, initialSpecBaseName)
	}

	// Copy initial config file from the base image bundle
	err = copyFile(initialConfigPath, filepath.Join(instance.Overlay.TopLayerPath(), initialSpecBaseName))
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
		Done: true, Success: s.imageClient.Archive(ctx, topLayerPath, req.ImageId, progressChan) == nil,
	})

	close(doneChan)
	return err
}

func (s *RunCServer) addRequestEnvToInitialSpec(instance *ContainerInstance) error {
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

func (s *RunCServer) RunCSyncWorkspace(ctx context.Context, in *pb.SyncContainerWorkspaceRequest) (*pb.SyncContainerWorkspaceResponse, error) {
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

func (s *RunCServer) waitForContainer(ctx context.Context, containerId string) error {
	for {
		instance, exists := s.containerInstances.Get(containerId)
		if !exists {
			return errors.New("container not found")
		}

		if instance.Spec == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		state, err := s.runcHandle.State(ctx, containerId)
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

func (s *RunCServer) RunCSandboxExec(ctx context.Context, in *pb.RunCSandboxExecRequest) (*pb.RunCSandboxExecResponse, error) {
	log.Info().Str("container_id", in.ContainerId).Str("cmd", in.Cmd).Msg("running sandbox command")

	parsedCmd, err := shlex.Split(in.Cmd)
	if err != nil {
		return &pb.RunCSandboxExecResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxExecResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err = s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxExecResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	env := instance.Spec.Process.Env
	formattedEnv := []string{}
	for key, value := range in.Env {
		formattedEnv = append(formattedEnv, fmt.Sprintf("%s=%s", key, value))
	}

	env = append(env, formattedEnv...)

	return s.handleSandboxExec(ctx, in, instance, env, parsedCmd, in.Cwd)
}

func (s *RunCServer) handleSandboxExec(ctx context.Context, in *pb.RunCSandboxExecRequest, instance *ContainerInstance, env, cmd []string, cwd string) (*pb.RunCSandboxExecResponse, error) {
	pid, err := instance.SandboxProcessManager.Exec(cmd, cwd, env, false)
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to execute sandbox process: %v", err)

		return &pb.RunCSandboxExecResponse{
			Ok:  false,
			Pid: -1,
		}, nil
	}

	return &pb.RunCSandboxExecResponse{
		Ok:  true,
		Pid: int32(pid),
	}, nil
}

type SandboxProcessStatus string

const (
	SandboxProcessStatusRunning SandboxProcessStatus = "running"
	SandboxProcessStatusExited  SandboxProcessStatus = "exited"
)

func (s *RunCServer) RunCSandboxStatus(ctx context.Context, in *pb.RunCSandboxStatusRequest) (*pb.RunCSandboxStatusResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	exitCode, err := instance.SandboxProcessManager.Status(int(in.Pid))
	if err != nil {
		return &pb.RunCSandboxStatusResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	status := SandboxProcessStatusRunning
	if exitCode >= 0 {
		status = SandboxProcessStatusExited
	}

	return &pb.RunCSandboxStatusResponse{
		Ok:       true,
		Status:   string(status),
		ExitCode: int32(exitCode),
	}, nil
}

func (s *RunCServer) RunCSandboxStdout(ctx context.Context, in *pb.RunCSandboxStdoutRequest) (*pb.RunCSandboxStdoutResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	stdout, err := instance.SandboxProcessManager.Stdout(int(in.Pid))
	if err != nil {
		return &pb.RunCSandboxStdoutResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.RunCSandboxStdoutResponse{
		Ok:     true,
		Stdout: stdout,
	}, nil
}

func (s *RunCServer) RunCSandboxStderr(ctx context.Context, in *pb.RunCSandboxStderrRequest) (*pb.RunCSandboxStderrResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: "Container not found",
		}, nil
	}

	stderr, err := instance.SandboxProcessManager.Stderr(int(in.Pid))
	if err != nil {
		return &pb.RunCSandboxStderrResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.RunCSandboxStderrResponse{
		Ok:     true,
		Stderr: stderr,
	}, nil
}

func (s *RunCServer) RunCSandboxKill(ctx context.Context, in *pb.RunCSandboxKillRequest) (*pb.RunCSandboxKillResponse, error) {
	log.Info().Str("container_id", in.ContainerId).Int32("pid", in.Pid).Msg("killing sandbox process")

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxKillResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := instance.SandboxProcessManager.Kill(int(in.Pid))
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Int32("pid", in.Pid).Msgf("failed to kill sandbox process: %v", err)
		return &pb.RunCSandboxKillResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxKillResponse{Ok: true}, err
}

func (s *RunCServer) RunCSandboxListExposedPorts(ctx context.Context, in *pb.RunCSandboxListExposedPortsRequest) (*pb.RunCSandboxListExposedPortsResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxListExposedPortsResponse{Ok: false, ErrorMsg: "Container not found"}, nil
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
		return &pb.RunCSandboxListExposedPortsResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxListExposedPortsResponse{Ok: true, ExposedPorts: ports}, nil
}

func (s *RunCServer) RunCSandboxListProcesses(ctx context.Context, in *pb.RunCSandboxListProcessesRequest) (*pb.RunCSandboxListProcessesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxListProcessesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
		return &pb.RunCSandboxListProcessesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	processes := make([]*pb.ProcessInfo, 0)
	ps, err := instance.SandboxProcessManager.ListProcesses()
	if err != nil {
		return &pb.RunCSandboxListProcessesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	for _, process := range ps {
		processes = append(processes, &pb.ProcessInfo{Pid: int32(process.Pid), ExitCode: int32(process.ExitCode), Cwd: process.Cwd, Cmd: process.Cmd, Env: process.Env, Running: process.Running})
	}

	return &pb.RunCSandboxListProcessesResponse{Ok: true, Processes: processes}, nil
}

func (s *RunCServer) RunCSandboxUploadFile(ctx context.Context, in *pb.RunCSandboxUploadFileRequest) (*pb.RunCSandboxUploadFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxUploadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxUploadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	err = os.WriteFile(hostPath, in.Data, os.FileMode(in.Mode))
	if err != nil {
		return &pb.RunCSandboxUploadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxUploadFileResponse{Ok: true}, nil
}

func (s *RunCServer) RunCSandboxCreateDirectory(ctx context.Context, in *pb.RunCSandboxCreateDirectoryRequest) (*pb.RunCSandboxCreateDirectoryResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
		return &pb.RunCSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(filepath.Clean(containerPath), instance)
	if err := os.MkdirAll(hostPath, os.FileMode(in.Mode)); err != nil {
		return &pb.RunCSandboxCreateDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxCreateDirectoryResponse{Ok: true}, nil
}

func (s *RunCServer) RunCSandboxDeleteDirectory(ctx context.Context, in *pb.RunCSandboxDeleteDirectoryRequest) (*pb.RunCSandboxDeleteDirectoryResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	if err := s.waitForContainer(ctx, in.ContainerId); err != nil {
		return &pb.RunCSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(filepath.Clean(containerPath), instance)
	if err := os.RemoveAll(hostPath); err != nil {
		return &pb.RunCSandboxDeleteDirectoryResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxDeleteDirectoryResponse{Ok: true}, nil
}

func (s *RunCServer) RunCSandboxDownloadFile(ctx context.Context, in *pb.RunCSandboxDownloadFileRequest) (*pb.RunCSandboxDownloadFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxDownloadFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxDownloadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	data, err := os.ReadFile(hostPath)
	if err != nil {
		return &pb.RunCSandboxDownloadFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxDownloadFileResponse{Ok: true, Data: data}, nil
}

func (s *RunCServer) RunCSandboxDeleteFile(ctx context.Context, in *pb.RunCSandboxDeleteFileRequest) (*pb.RunCSandboxDeleteFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxDeleteFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxDeleteFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	err = os.RemoveAll(hostPath)
	if err != nil {
		return &pb.RunCSandboxDeleteFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxDeleteFileResponse{Ok: true}, nil
}

func (s *RunCServer) RunCSandboxStatFile(ctx context.Context, in *pb.RunCSandboxStatFileRequest) (*pb.RunCSandboxStatFileResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxStatFileResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxStatFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	stat, err := os.Stat(hostPath)
	if err != nil {
		return &pb.RunCSandboxStatFileResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.RunCSandboxStatFileResponse{Ok: true, FileInfo: &pb.FileInfo{
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

func (s *RunCServer) RunCSandboxListFiles(ctx context.Context, in *pb.RunCSandboxListFilesRequest) (*pb.RunCSandboxListFilesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxListFilesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	files, err := os.ReadDir(hostPath)
	if err != nil {
		return &pb.RunCSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	responseFiles := make([]*pb.FileInfo, 0)
	for _, file := range files {
		stat, err := file.Info()
		if err != nil {
			return &pb.RunCSandboxListFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
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

	return &pb.RunCSandboxListFilesResponse{Ok: true, Files: responseFiles}, nil
}

// getHostPathFromContainerPath maps a container path to its corresponding host path
func (s *RunCServer) getHostPathFromContainerPath(containerPath string, instance *ContainerInstance) string {
	// Check if the path matches any of the container's mounts
	for _, mount := range instance.Spec.Mounts {
		if containerPath == mount.Destination || strings.HasPrefix(containerPath, mount.Destination+"/") {
			relativePath := strings.TrimPrefix(containerPath, mount.Destination)
			return filepath.Join(mount.Source, strings.TrimPrefix(relativePath, "/"))
		}
	}

	// If no mount matches, use the overlay root path
	return filepath.Join(instance.Spec.Root.Path, strings.TrimPrefix(filepath.Clean(containerPath), "/"))
}

func (s *RunCServer) RunCSandboxExposePort(ctx context.Context, in *pb.RunCSandboxExposePortRequest) (*pb.RunCSandboxExposePortResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxExposePortResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	getAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.GetContainerAddressMap(context.Background(), &pb.GetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
	}))
	if err != nil {
		return &pb.RunCSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	addressMap := getAddressMapResponse.AddressMap
	if _, exists := addressMap[int32(in.Port)]; exists {
		return &pb.RunCSandboxExposePortResponse{
			Ok:       false,
			ErrorMsg: fmt.Sprintf("Port %d is already exposed", in.Port),
		}, nil
	}

	bindPort, err := getRandomFreePort()
	if err != nil {
		return &pb.RunCSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	err = s.containerNetworkManager.ExposePort(in.ContainerId, bindPort, int(in.Port))
	if err != nil {
		log.Error().Str("container_id", in.ContainerId).Msgf("failed to expose container bind port: %v", err)
		return &pb.RunCSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	addressMap[int32(in.Port)] = fmt.Sprintf("%s:%d", s.podAddr, bindPort)
	setAddressMapResponse, err := handleGRPCResponse(s.containerRepoClient.SetContainerAddressMap(context.Background(), &pb.SetContainerAddressMapRequest{
		ContainerId: in.ContainerId,
		AddressMap:  addressMap,
	}))
	if err != nil {
		return &pb.RunCSandboxExposePortResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	instance.Request.Ports = append(instance.Request.Ports, uint32(in.Port))
	s.containerInstances.Set(in.ContainerId, instance)

	log.Info().Str("container_id", in.ContainerId).Msgf("exposed sandbox port %d to %s", in.Port, addressMap[int32(in.Port)])

	return &pb.RunCSandboxExposePortResponse{Ok: setAddressMapResponse.Ok}, err
}

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

func (s *RunCServer) RunCSandboxReplaceInFiles(ctx context.Context, in *pb.RunCSandboxReplaceInFilesRequest) (*pb.RunCSandboxReplaceInFilesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)
	stagedFiles, err := stageFilesForReplacement(hostPath, in.Pattern, in.NewString)
	if err != nil {
		return &pb.RunCSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	for _, stagedFile := range stagedFiles {
		err = os.WriteFile(stagedFile.Path, []byte(stagedFile.Content), 0644)
		if err != nil {
			return &pb.RunCSandboxReplaceInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
		}
	}

	return &pb.RunCSandboxReplaceInFilesResponse{Ok: true}, nil
}

func (s *RunCServer) RunCSandboxFindInFiles(ctx context.Context, in *pb.RunCSandboxFindInFilesRequest) (*pb.RunCSandboxFindInFilesResponse, error) {
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCSandboxFindInFilesResponse{Ok: false, ErrorMsg: "Container not found"}, nil
	}

	err := s.waitForContainer(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCSandboxFindInFilesResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	containerPath := in.ContainerPath
	if !filepath.IsAbs(containerPath) {
		containerPath = filepath.Join(instance.Spec.Process.Cwd, containerPath)
	}

	hostPath := s.getHostPathFromContainerPath(containerPath, instance)

	// Build ripgrep command with JSON output format
	args := []string{
		"--json",               // Output in JSON format
		"--line-number",        // Include line numbers
		"--column",             // Include column numbers
		"--with-filename",      // Include filename in output
		"--no-heading",         // Don't include file headers
		"--no-messages",        // Suppress error messages
		"--no-ignore",          // Don't respect .gitignore, etc.
		"--hidden",             // Search hidden files
		"--binary",             // Search binary files
		"--regexp", in.Pattern, // The search pattern
		hostPath, // The directory to search
	}

	cmd := exec.CommandContext(ctx, "rg", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		// ripgrep returns exit code 1 when no matches are found, which is not an error
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			// No matches found, return empty results
			return &pb.RunCSandboxFindInFilesResponse{Ok: true, Results: []*pb.FileSearchResult{}}, nil
		}
		return &pb.RunCSandboxFindInFilesResponse{
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

		// Parse JSON line from ripgrep
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

		// Only process match results
		if rgResult.Type != "match" {
			continue
		}

		filePath := rgResult.Data.Path.Text
		lineNum := rgResult.Data.LineNumber

		// Calculate column positions for each submatch
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

	// Convert map to slice format expected by the response
	for filePath, matches := range fileMatches {
		results = append(results, &pb.FileSearchResult{
			Path:    filePath,
			Matches: matches,
		})
	}

	return &pb.RunCSandboxFindInFilesResponse{Ok: true, Results: results}, nil
}
