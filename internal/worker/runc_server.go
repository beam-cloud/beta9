package worker

import (
	"context"
	"encoding/json"
	"fmt"
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
}

func NewRunCServer() (*RunCServer, error) {
	var baseConfigSpec specs.Spec
	specTemplate := strings.TrimSpace(string(baseRuncConfigRaw))
	err := json.Unmarshal([]byte(specTemplate), &baseConfigSpec)
	if err != nil {
		return nil, err
	}

	return &RunCServer{
		runcHandle:     runc.Runc{},
		baseConfigSpec: baseConfigSpec,
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

	return &pb.RunCKillResponse{
		Ok: true,
	}, err
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

	outputWriter := common.NewOutputWriter(func(s string) {
		log.Println("Output from exec: ", s)
	})

	err = s.runcHandle.Exec(ctx, in.ContainerId, *process, &runc.ExecOpts{
		OutputWriter: outputWriter,
	})

	return &pb.RunCExecResponse{}, err
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
