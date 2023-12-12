package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"syscall"

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

// Worker entry point
func (s *RunCServer) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", defaultContainerServerPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Register scheduler
	// s, err := scheduler.NewSchedulerService()
	// if err != nil {
	// 	return err
	// }

	// pb.RegisterSchedulerServer(grpcServer, s)

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Printf("Failed to start grpc server: %v\n", err)
		}
	}()

	return nil
}

func (s *RunCServer) Kill(ctx context.Context, containerId string) error {
	err := s.runcHandle.Kill(ctx, containerId, int(syscall.SIGTERM), &runc.KillOpts{
		All: true,
	})

	return err
}

// Execute an arbitary command inside a running container
func (s *RunCServer) Exec(ctx context.Context, containerId string, cmd string, outputChan chan common.OutputMsg) error {
	cmd = fmt.Sprintf("bash -c '%s'", cmd)
	parsedCmd, err := shlex.Split(cmd)
	if err != nil {
		return err
	}

	process := s.baseConfigSpec.Process
	process.Env = append(process.Env, "DEBIAN_FRONTEND=noninteractive")
	process.Args = parsedCmd
	process.Cwd = defaultWorkingDirectory

	outputWriter := common.NewOutputWriter(func(s string) {
		if outputChan != nil {
			outputChan <- common.OutputMsg{
				Msg:     strings.TrimSuffix(string(s), "\n"),
				Done:    false,
				Success: false,
			}
		} else {
			log.Print(s)
		}
	})

	return s.runcHandle.Exec(ctx, containerId, *process, &runc.ExecOpts{
		OutputWriter: outputWriter,
	})
}
