package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/beam-cloud/beta9/proto"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/google/shlex"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
)

const (
	defaultWorkingDirectory string = "/mnt/code"
)

type RunCServer struct {
	runcHandle     runc.Runc
	baseConfigSpec specs.Spec
	pb.UnimplementedRunCServiceServer
	containerInstances *common.SafeMap[*ContainerInstance]
	imageClient        *ImageClient
	port               int
}

func NewRunCServer(containerInstances *common.SafeMap[*ContainerInstance], imageClient *ImageClient) (*RunCServer, error) {
	var baseConfigSpec specs.Spec
	specTemplate := strings.TrimSpace(string(baseRuncConfigRaw))
	err := json.Unmarshal([]byte(specTemplate), &baseConfigSpec)
	if err != nil {
		return nil, err
	}

	return &RunCServer{
		runcHandle:         runc.Runc{},
		baseConfigSpec:     baseConfigSpec,
		containerInstances: containerInstances,
		imageClient:        imageClient,
	}, nil
}

func (s *RunCServer) Start() error {
	listener, err := net.Listen("tcp", ":0") // // Random free port
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}

	s.port = listener.Addr().(*net.TCPAddr).Port
	log.Printf("RunCServer started on port %d\n", s.port)

	grpcServer := grpc.NewServer()
	pb.RegisterRunCServiceServer(grpcServer, s)

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Printf("Failed to start grpc server: %v\n", err)
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
	cmd := fmt.Sprintf("bash -c '%s'", in.Cmd)
	parsedCmd, err := shlex.Split(cmd)
	if err != nil {
		return &pb.RunCExecResponse{}, err
	}

	process := s.baseConfigSpec.Process
	process.Env = append(process.Env, "DEBIAN_FRONTEND=noninteractive")
	process.Args = parsedCmd
	process.Cwd = defaultWorkingDirectory

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCExecResponse{Ok: false}, nil
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

					log.Printf("Image upload progress: %d/100\n", progress)
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

	err = stream.Send(&pb.RunCArchiveResponse{
		Done: true, Success: s.imageClient.Archive(ctx, instance.Overlay.TopLayerPath(), req.ImageId, progressChan) == nil,
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
