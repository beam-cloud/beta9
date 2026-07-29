package thunder

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type ThunderClient interface {
	CreateZone(ctx context.Context, displayName string) (*Zone, error)
	CreateClientEnrollmentToken(ctx context.Context, zoneID, gpuType string, gpuCount int) (*EnrollmentToken, error)
	CreateServerEnrollmentToken(ctx context.Context, zoneID string) (*EnrollmentToken, error)
	DeleteEnrollmentTokenNode(ctx context.Context, enrollmentTokenID string) (*DeleteEnrollmentTokenNodeResponse, error)
	ClientInstallCommand(enrollmentToken string) (string, error)
}

type AgentStateValidator interface {
	ResolveAgentState(ctx context.Context, agentToken string) (*model.AgentTokenState, error)
}

type ServiceOpts struct {
	Repository          Repository
	RedisClient         *common.RedisClient
	Client              ThunderClient
	ContainerRepo       repository.ContainerRepository
	WorkerRepo          repository.WorkerRepository
	AgentStateValidator AgentStateValidator
}

type Service struct {
	pb.UnimplementedThunderServiceServer

	repo           Repository
	client         ThunderClient
	containerRepo  repository.ContainerRepository
	workerRepo     repository.WorkerRepository
	agentValidator AgentStateValidator
}

func NewService(opts ServiceOpts) (*Service, error) {
	repo := opts.Repository
	if repo == nil && opts.RedisClient != nil {
		repo = NewRedisRepository(opts.RedisClient)
	}
	if repo == nil {
		return nil, fmt.Errorf("Thunder repository is required")
	}

	client := opts.Client
	if client == nil {
		client = NewClientFromEnv(nil)
	}
	if client == nil {
		return nil, fmt.Errorf("Thunder client is required")
	}
	if opts.ContainerRepo == nil {
		return nil, fmt.Errorf("container repository is required")
	}
	if opts.WorkerRepo == nil {
		return nil, fmt.Errorf("worker repository is required")
	}

	return &Service{
		repo:           repo,
		client:         client,
		containerRepo:  opts.ContainerRepo,
		workerRepo:     opts.WorkerRepo,
		agentValidator: opts.AgentStateValidator,
	}, nil
}

func (s *Service) CreateClientEnrollment(ctx context.Context, req *pb.CreateClientEnrollmentRequest) (*pb.CreateClientEnrollmentResponse, error) {
	containerID := strings.TrimSpace(req.GetContainerId())
	if containerID == "" {
		return &pb.CreateClientEnrollmentResponse{ErrorMsg: "container id is required"}, nil
	}
	if err := requireWorkerToken(ctx, ""); err != nil {
		return &pb.CreateClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	attrs, err := s.clientEnrollmentAttrs(ctx, containerID)
	if err != nil {
		return &pb.CreateClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}
	if err := requireWorkerToken(ctx, attrs.workspaceID); err != nil {
		return &pb.CreateClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	var installCommand string
	err = s.repo.WithPoolLock(ctx, attrs.workspaceID, attrs.poolName, func(ctx context.Context) error {
		var previousEnrollmentTokenID string
		if existing, found, err := s.repo.GetClientEnrollment(ctx, containerID); err != nil {
			return err
		} else if found {
			previousEnrollmentTokenID = strings.TrimSpace(existing.EnrollmentTokenID)
		}

		zoneID, err := s.ensureZoneLocked(ctx, attrs.workspaceID, attrs.poolName)
		if err != nil {
			return err
		}

		enrollment, err := s.client.CreateClientEnrollmentToken(ctx, zoneID, attrs.gpuType, attrs.gpuCount)
		if err != nil {
			return err
		}
		if enrollment == nil || strings.TrimSpace(enrollment.EnrollmentTokenID) == "" || strings.TrimSpace(enrollment.EnrollmentToken) == "" {
			return fmt.Errorf("Thunder client enrollment response was incomplete")
		}
		installCommand, err = s.client.ClientInstallCommand(enrollment.EnrollmentToken)
		if err != nil {
			return err
		}
		if err := s.repo.SaveClientEnrollment(ctx, &ClientEnrollmentState{
			ContainerID:       containerID,
			WorkspaceID:       attrs.workspaceID,
			WorkerID:          attrs.workerID,
			MachineID:         attrs.machineID,
			PoolName:          attrs.poolName,
			EnrollmentTokenID: enrollment.EnrollmentTokenID,
		}, 0); err != nil {
			if _, deleteErr := s.client.DeleteEnrollmentTokenNode(ctx, enrollment.EnrollmentTokenID); deleteErr != nil && !isThunderNotFound(deleteErr) {
				return fmt.Errorf("failed to save Thunder client enrollment: %w; additionally failed to revoke Thunder enrollment token %q: %v", err, enrollment.EnrollmentTokenID, deleteErr)
			}
			return err
		}
		if previousEnrollmentTokenID != "" && previousEnrollmentTokenID != enrollment.EnrollmentTokenID {
			if _, err := s.client.DeleteEnrollmentTokenNode(ctx, previousEnrollmentTokenID); err != nil && !isThunderNotFound(err) {
				return fmt.Errorf("failed to revoke previous Thunder client enrollment token %q: %w", previousEnrollmentTokenID, err)
			}
		}
		return nil
	})
	if err != nil {
		return &pb.CreateClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	return &pb.CreateClientEnrollmentResponse{Ok: true, InstallCommand: installCommand}, nil
}

func (s *Service) DeleteClientEnrollment(ctx context.Context, req *pb.DeleteClientEnrollmentRequest) (*pb.DeleteClientEnrollmentResponse, error) {
	containerID := strings.TrimSpace(req.GetContainerId())
	if containerID == "" {
		return &pb.DeleteClientEnrollmentResponse{ErrorMsg: "container id is required"}, nil
	}
	if err := requireWorkerToken(ctx, ""); err != nil {
		return &pb.DeleteClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	state, found, err := s.repo.GetClientEnrollment(ctx, containerID)
	if err != nil {
		return &pb.DeleteClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}
	if !found {
		return &pb.DeleteClientEnrollmentResponse{Ok: true}, nil
	}
	if err := requireWorkerToken(ctx, state.WorkspaceID); err != nil {
		return &pb.DeleteClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	if strings.TrimSpace(state.EnrollmentTokenID) != "" {
		_, err = s.client.DeleteEnrollmentTokenNode(ctx, state.EnrollmentTokenID)
		if err != nil && !isThunderNotFound(err) {
			return &pb.DeleteClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
		}
	}
	if err := s.repo.DeleteClientEnrollment(ctx, containerID); err != nil {
		return &pb.DeleteClientEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.DeleteClientEnrollmentResponse{Ok: true}, nil
}

func (s *Service) CreateNodeEnrollment(ctx context.Context, req *pb.CreateNodeEnrollmentRequest) (*pb.CreateNodeEnrollmentResponse, error) {
	agentState, err := s.requireAgentState(ctx, req.GetAgentToken())
	if err != nil {
		return &pb.CreateNodeEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	var enrollmentToken string
	err = s.repo.WithPoolLock(ctx, agentState.WorkspaceID, agentState.PoolName, func(ctx context.Context) error {
		var previousEnrollmentTokenID string
		if existing, found, err := s.repo.GetNodeEnrollment(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID); err != nil {
			return err
		} else if found {
			previousEnrollmentTokenID = strings.TrimSpace(existing.EnrollmentTokenID)
		}

		zoneID, err := s.ensureZoneLocked(ctx, agentState.WorkspaceID, agentState.PoolName)
		if err != nil {
			return err
		}

		enrollment, err := s.client.CreateServerEnrollmentToken(ctx, zoneID)
		if err != nil {
			return err
		}
		if enrollment == nil || strings.TrimSpace(enrollment.EnrollmentTokenID) == "" || strings.TrimSpace(enrollment.EnrollmentToken) == "" {
			return fmt.Errorf("Thunder node enrollment response was incomplete")
		}
		if err := s.repo.SaveNodeEnrollment(ctx, &NodeEnrollmentState{
			WorkspaceID:       agentState.WorkspaceID,
			PoolName:          agentState.PoolName,
			MachineID:         agentState.MachineID,
			EnrollmentTokenID: enrollment.EnrollmentTokenID,
		}, 0); err != nil {
			if _, deleteErr := s.client.DeleteEnrollmentTokenNode(ctx, enrollment.EnrollmentTokenID); deleteErr != nil && !isThunderNotFound(deleteErr) {
				return fmt.Errorf("failed to save Thunder node enrollment: %w; additionally failed to revoke Thunder enrollment token %q: %v", err, enrollment.EnrollmentTokenID, deleteErr)
			}
			return err
		}
		if previousEnrollmentTokenID != "" && previousEnrollmentTokenID != enrollment.EnrollmentTokenID {
			if _, err := s.client.DeleteEnrollmentTokenNode(ctx, previousEnrollmentTokenID); err != nil && !isThunderNotFound(err) {
				return fmt.Errorf("failed to revoke previous Thunder node enrollment token %q: %w", previousEnrollmentTokenID, err)
			}
		}
		enrollmentToken = enrollment.EnrollmentToken
		return nil
	})
	if err != nil {
		return &pb.CreateNodeEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	return &pb.CreateNodeEnrollmentResponse{Ok: true, EnrollmentToken: enrollmentToken}, nil
}

func (s *Service) DeleteNodeEnrollment(ctx context.Context, req *pb.DeleteNodeEnrollmentRequest) (*pb.DeleteNodeEnrollmentResponse, error) {
	agentState, err := s.requireAgentState(ctx, req.GetAgentToken())
	if err != nil {
		return &pb.DeleteNodeEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}

	state, found, err := s.repo.GetNodeEnrollment(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
	if err != nil {
		return &pb.DeleteNodeEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}
	if !found {
		return &pb.DeleteNodeEnrollmentResponse{Ok: true}, nil
	}

	if strings.TrimSpace(state.EnrollmentTokenID) != "" {
		_, err = s.client.DeleteEnrollmentTokenNode(ctx, state.EnrollmentTokenID)
		if err != nil && !isThunderNotFound(err) {
			return &pb.DeleteNodeEnrollmentResponse{ErrorMsg: err.Error()}, nil
		}
	}
	if err := s.repo.DeleteNodeEnrollment(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID); err != nil {
		return &pb.DeleteNodeEnrollmentResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.DeleteNodeEnrollmentResponse{Ok: true}, nil
}

type clientEnrollmentAttrs struct {
	workspaceID string
	workerID    string
	machineID   string
	poolName    string
	gpuType     string
	gpuCount    int
}

func (s *Service) clientEnrollmentAttrs(ctx context.Context, containerID string) (*clientEnrollmentAttrs, error) {
	state, err := s.containerRepo.GetContainerState(containerID)
	if err != nil {
		return nil, err
	}
	if state == nil || state.ContainerId != containerID {
		return nil, fmt.Errorf("container state not found")
	}
	if state.WorkspaceId == "" {
		return nil, fmt.Errorf("container workspace is required")
	}
	if state.WorkerId == "" {
		return nil, fmt.Errorf("container worker is required")
	}

	worker, err := s.workerRepo.GetWorkerById(state.WorkerId)
	if err != nil {
		return nil, err
	}
	if worker == nil || worker.Id != state.WorkerId {
		return nil, fmt.Errorf("worker state not found")
	}
	if worker.PoolName == "" {
		return nil, fmt.Errorf("worker pool is required")
	}

	machineID := strings.TrimSpace(state.MachineId)
	if machineID == "" {
		machineID = strings.TrimSpace(worker.MachineId)
	}
	gpuType := strings.ToLower(strings.TrimSpace(state.Gpu))
	if gpuType == "" || strings.EqualFold(gpuType, string(types.NO_GPU)) {
		return nil, fmt.Errorf("container GPU type is required for Thunder client enrollment")
	}
	gpuCount := int(state.GpuCount)
	if gpuCount <= 0 {
		gpuCount = 1
	}

	return &clientEnrollmentAttrs{
		workspaceID: state.WorkspaceId,
		workerID:    state.WorkerId,
		machineID:   machineID,
		poolName:    worker.PoolName,
		gpuType:     gpuType,
		gpuCount:    gpuCount,
	}, nil
}

func (s *Service) ensureZone(ctx context.Context, workspaceID, poolName string) (string, error) {
	var zoneID string
	err := s.repo.WithPoolLock(ctx, workspaceID, poolName, func(ctx context.Context) error {
		var err error
		zoneID, err = s.ensureZoneLocked(ctx, workspaceID, poolName)
		return err
	})
	if err != nil {
		return "", err
	}
	return zoneID, nil
}

func (s *Service) ensureZoneLocked(ctx context.Context, workspaceID, poolName string) (string, error) {
	state, found, err := s.repo.GetZone(ctx, workspaceID, poolName)
	if err != nil {
		return "", err
	}
	if found && state.ThunderZoneID != "" {
		return state.ThunderZoneID, nil
	}

	zone, err := s.client.CreateZone(ctx, thunderZoneDisplayName(workspaceID, poolName))
	if err != nil {
		return "", err
	}
	if zone == nil || strings.TrimSpace(zone.ZoneID) == "" {
		return "", fmt.Errorf("Thunder zone response did not include a zone id")
	}
	return zone.ZoneID, s.repo.SaveZone(ctx, &ZoneState{
		WorkspaceID:   workspaceID,
		PoolName:      poolName,
		ThunderZoneID: zone.ZoneID,
	})
}

func thunderZoneDisplayName(workspaceID, poolName string) string {
	workspaceID = strings.TrimSpace(workspaceID)
	poolName = strings.TrimSpace(poolName)
	if workspaceID == "" {
		return poolName
	}
	if poolName == "" {
		return workspaceID
	}
	return workspaceID + "-" + poolName
}

func (s *Service) requireAgentState(ctx context.Context, agentToken string) (*model.AgentTokenState, error) {
	agentToken = strings.TrimSpace(agentToken)
	if agentToken == "" {
		return nil, fmt.Errorf("agent token is required")
	}
	if s == nil || s.agentValidator == nil {
		return nil, fmt.Errorf("agent state validator is unavailable")
	}
	return s.agentValidator.ResolveAgentState(ctx, agentToken)
}

func requireWorkerToken(ctx context.Context, workspaceID string) error {
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo == nil || authInfo.Token == nil || !types.IsWorkerTokenType(authInfo.Token.TokenType) {
		return fmt.Errorf("worker token is required")
	}
	if authInfo.Token.TokenType == types.TokenTypeWorkerPrivate {
		if authInfo.Workspace == nil || authInfo.Workspace.ExternalId == "" {
			return fmt.Errorf("worker token is not scoped to a workspace")
		}
		if workspaceID != "" && workspaceID != authInfo.Workspace.ExternalId {
			return fmt.Errorf("worker token cannot access workspace %q", workspaceID)
		}
	}
	return nil
}

func isThunderNotFound(err error) bool {
	var thunderErr *ThunderError
	return errors.As(err, &thunderErr) && thunderErr.StatusCode == http.StatusNotFound
}
