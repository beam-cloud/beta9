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
	"syscall"

	pb "github.com/beam-cloud/beta9/proto"

	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/go-runc"
	"github.com/google/shlex"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
)

const (
	defaultWorkingDirectory string = "/mnt/code"
	defaultWorkerServerPort int    = 1000
)

type RunCServer struct {
	runcHandle     runc.Runc
	baseConfigSpec specs.Spec
	pb.UnimplementedRunCServiceServer
	containerInstances *common.SafeMap[*ContainerInstance]
	imageClient        *ImageClient
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
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", defaultWorkerServerPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

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
	err := s.runcHandle.Kill(ctx, in.ContainerId, int(syscall.SIGTERM), &runc.KillOpts{
		All: true,
	})

	err = s.runcHandle.Delete(ctx, in.ContainerId, &runc.DeleteOpts{
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
		Running: state.Status == "running",
	}, nil
}

func (s *RunCServer) RunCStreamLogs(req *pb.RunCStreamLogsRequest, stream pb.RunCService_RunCStreamLogsServer) error {
	instance, exists := s.containerInstances.Get(req.ContainerId)
	if !exists {
		log.Println("container not found")
		return errors.New("container not found")
	}

	log.Println("looking for logs")

	buffer := make([]byte, 4096)
	for {
		n, err := instance.LogBuffer.Read(buffer)
		log.Println(err)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		logEntry := &pb.RunCLogEntry{
			Msg: string(buffer[:n]),
		}

		if err := stream.Send(logEntry); err != nil {
			return err
		}
	}

	return nil
}

func (s *RunCServer) RunCArchive(ctx context.Context, in *pb.RunCArchiveRequest) (*pb.RunCArchiveResponse, error) {
	state, err := s.runcHandle.State(ctx, in.ContainerId)
	if err != nil {
		return &pb.RunCArchiveResponse{
			Ok: false,
		}, nil
	}

	if state.Status != "running" {
		return &pb.RunCArchiveResponse{
			Ok: false,
		}, nil
	}

	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return &pb.RunCArchiveResponse{
			Ok: false,
		}, nil
	}

	// Copy initial config file from the base image bundle
	err = copyFile(filepath.Join(instance.BundlePath, "config.json"), filepath.Join(instance.Overlay.TopLayerPath(), "initial_config.json"))
	if err != nil {
		return &pb.RunCArchiveResponse{
			Ok: false,
		}, nil
	}

	tempConfig := s.baseConfigSpec
	tempConfig.Hooks.Prestart = nil
	tempConfig.Process.Terminal = false
	tempConfig.Process.Args = []string{"tail", "-f", "/dev/null"}
	tempConfig.Root.Readonly = false

	file, err := json.MarshalIndent(tempConfig, "", " ")
	if err != nil {
		return nil, err
	}

	configPath := filepath.Join(instance.Overlay.TopLayerPath(), "config.json")
	err = os.WriteFile(configPath, file, 0644)
	if err != nil {
		return &pb.RunCArchiveResponse{
			Ok: false,
		}, nil
	}

	return &pb.RunCArchiveResponse{
		Ok: s.imageClient.Archive(ctx, instance.Overlay.TopLayerPath(), in.ImageId) == nil,
	}, nil
}
