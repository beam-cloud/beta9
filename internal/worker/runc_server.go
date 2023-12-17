package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"syscall"

	pb "github.com/beam-cloud/beam/proto"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/google/shlex"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/slai-labs/go-runc"
	"google.golang.org/grpc"
)

const (
	defaultWorkingDirectory    string = "/workspace"
	defaultContainerServerPort int    = 1000
)

type RunCServer struct {
	runcHandle     runc.Runc
	baseConfigSpec specs.Spec
	pb.UnimplementedRunCServiceServer
	containerInstances *common.SafeMap[*ContainerInstance]
}

func NewRunCServer(containerInstances *common.SafeMap[*ContainerInstance]) (*RunCServer, error) {
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
	}, nil
}

func (s *RunCServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", defaultContainerServerPort))
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
		return errors.New("container not found")
	}

	buffer := make([]byte, 4096)
	for {
		n, err := instance.LogBuffer.Read(buffer)
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

	return &pb.RunCArchiveResponse{
		Ok: true,
	}, nil
}
