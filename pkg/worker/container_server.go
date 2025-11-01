package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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
