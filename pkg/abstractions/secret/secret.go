package secret

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SecretService interface {
	pb.SecretServiceServer
	CreateSecret(ctx context.Context, req *pb.CreateSecretRequest) (*pb.CreateSecretResponse, error)
	GetSecret(ctx context.Context, req *pb.GetSecretRequest) (*pb.GetSecretResponse, error)
	ListSecrets(ctx context.Context, req *pb.ListSecretsRequest) (*pb.ListSecretsResponse, error)
	UpdateSecret(ctx context.Context, req *pb.UpdateSecretRequest) (*pb.UpdateSecretResponse, error)
	DeleteSecret(ctx context.Context, req *pb.DeleteSecretRequest) (*pb.DeleteSecretResponse, error)
}

type WorkspaceSecretService struct {
	pb.UnimplementedSecretServiceServer
	backendRepo repository.BackendRepository
}

var secretRoutePrefix = "/secret"

func NewWorkspaceSecretService(backendRepo repository.BackendRepository, routeGroup *echo.Group) *WorkspaceSecretService {
	ss := &WorkspaceSecretService{
		backendRepo: backendRepo,
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerSecretRoutes(routeGroup.Group(secretRoutePrefix, authMiddleware), ss)

	return ss
}

func (s *WorkspaceSecretService) CreateSecret(ctx context.Context, req *pb.CreateSecretRequest) (*pb.CreateSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	// Save the secret
	_, err := s.backendRepo.CreateSecret(ctx, authInfo.Workspace, authInfo.Token.Id, req.Name, req.Value)
	if err != nil {
		return &pb.CreateSecretResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, err
	}

	return &pb.CreateSecretResponse{
		Ok:     true,
		ErrMsg: "",
	}, nil
}

func (s *WorkspaceSecretService) GetSecret(ctx context.Context, req *pb.GetSecretRequest) (*pb.GetSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	secret, err := s.backendRepo.GetSecretByName(ctx, authInfo.Workspace, req.Name)
	if err != nil {
		return &pb.GetSecretResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, err
	}

	return &pb.GetSecretResponse{
		Ok:     true,
		ErrMsg: "",
		Secret: &pb.WorkspaceSecret{
			Name:      secret.Name,
			Value:     secret.Value,
			UpdatedAt: timestamppb.New(secret.UpdatedAt),
			CreatedAt: timestamppb.New(secret.CreatedAt),
		},
	}, nil
}

func (s *WorkspaceSecretService) ListSecrets(ctx context.Context, req *pb.ListSecretsRequest) (*pb.ListSecretsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	secrets, err := s.backendRepo.ListSecrets(ctx, authInfo.Workspace)
	if err != nil {
		return &pb.ListSecretsResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, err
	}

	secretList := make([]*pb.WorkspaceSecret, 0, len(secrets))
	for _, secret := range secrets {
		secretList = append(secretList, &pb.WorkspaceSecret{
			Name:      secret.Name,
			Value:     secret.Value,
			UpdatedAt: timestamppb.New(secret.UpdatedAt),
			CreatedAt: timestamppb.New(secret.CreatedAt),
		})
	}

	return &pb.ListSecretsResponse{
		Ok:      true,
		ErrMsg:  "",
		Secrets: secretList,
	}, nil
}

func (s *WorkspaceSecretService) UpdateSecret(ctx context.Context, req *pb.UpdateSecretRequest) (*pb.UpdateSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	_, err := s.backendRepo.UpdateSecret(ctx, authInfo.Workspace, authInfo.Token.Id, req.Name, req.Value)
	if err != nil {
		return &pb.UpdateSecretResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, err
	}

	return &pb.UpdateSecretResponse{
		Ok:     true,
		ErrMsg: "",
	}, nil
}

func (s *WorkspaceSecretService) DeleteSecret(ctx context.Context, req *pb.DeleteSecretRequest) (*pb.DeleteSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := s.backendRepo.DeleteSecret(ctx, authInfo.Workspace, req.Id)
	if err != nil {
		return &pb.DeleteSecretResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, err
	}

	return &pb.DeleteSecretResponse{
		Ok:     true,
		ErrMsg: "",
	}, nil
}
