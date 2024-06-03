package secret

import (
	"context"
	"errors"

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

func NewSecretService(backendRepo repository.BackendRepository, routeGroup *echo.Group) SecretService {
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
	secret, err := s.backendRepo.CreateSecret(ctx, authInfo.Workspace, authInfo.Token.Id, req.Name, req.Value)
	if err != nil {
		return &pb.CreateSecretResponse{
			Ok:     false,
			ErrMsg: "Failed to create secret",
		}, errors.New("failed to create secret")
	}

	return &pb.CreateSecretResponse{
		Ok:     true,
		ErrMsg: "",
		Name:   secret.Name,
	}, nil
}

func (s *WorkspaceSecretService) GetSecret(ctx context.Context, req *pb.GetSecretRequest) (*pb.GetSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	secret, err := s.backendRepo.GetSecretByNameDecrypted(ctx, authInfo.Workspace, req.Name)
	if err != nil {
		return &pb.GetSecretResponse{
			Ok:     false,
			ErrMsg: "Failed to get secret",
		}, errors.New("failed to get secret")
	}

	return &pb.GetSecretResponse{
		Ok:     true,
		ErrMsg: "",
		Secret: &pb.Secret{
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
			ErrMsg: "Failed to list secrets",
		}, errors.New("failed to list secrets")
	}

	secretList := make([]*pb.Secret, 0, len(secrets))
	for _, secret := range secrets {
		secretList = append(secretList, &pb.Secret{
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
			ErrMsg: "Failed to update secret",
		}, errors.New("failed to update secret")
	}

	return &pb.UpdateSecretResponse{
		Ok:     true,
		ErrMsg: "",
	}, nil
}

func (s *WorkspaceSecretService) DeleteSecret(ctx context.Context, req *pb.DeleteSecretRequest) (*pb.DeleteSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := s.backendRepo.DeleteSecret(ctx, authInfo.Workspace, req.Name)
	if err != nil {
		return &pb.DeleteSecretResponse{
			Ok:     false,
			ErrMsg: "Failed to delete secret",
		}, errors.New("failed to delete secret")
	}

	return &pb.DeleteSecretResponse{
		Ok:     true,
		ErrMsg: "",
	}, nil
}
