package secret

import (
	"context"
	"database/sql"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
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

func NewSecretService(backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, routeGroup *echo.Group) SecretService {
	ss := &WorkspaceSecretService{
		backendRepo: backendRepo,
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo, workspaceRepo)
	registerSecretRoutes(routeGroup.Group(secretRoutePrefix, authMiddleware), ss)

	return ss
}

func handleErrMsg(err error) string {
	msg := err.Error()

	if err, ok := err.(*pq.Error); ok {
		if err.Code.Name() == "unique_violation" { // Unique violation
			msg = "Secret already exists"
		} else {
			// Don't expose pg error messages
			msg = "Failed to create secret"
		}
	}

	if err == sql.ErrNoRows {
		msg = "Secret not found"
	}

	return msg
}

func (s *WorkspaceSecretService) CreateSecret(ctx context.Context, req *pb.CreateSecretRequest) (*pb.CreateSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.CreateSecretResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	// Save the secret
	secret, err := s.backendRepo.CreateSecret(ctx, authInfo.Workspace, authInfo.Token.Id, req.Name, req.Value)
	if err != nil {
		return &pb.CreateSecretResponse{
			Ok:     false,
			ErrMsg: handleErrMsg(err),
		}, nil
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
			ErrMsg: handleErrMsg(err),
		}, nil
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

	if !auth.HasPermission(authInfo) {
		return &pb.ListSecretsResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	secrets, err := s.backendRepo.ListSecrets(ctx, authInfo.Workspace)
	if err != nil {
		return &pb.ListSecretsResponse{
			Ok:     false,
			ErrMsg: handleErrMsg(err),
		}, nil
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

	if !auth.HasPermission(authInfo) {
		return &pb.UpdateSecretResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	_, err := s.backendRepo.UpdateSecret(ctx, authInfo.Workspace, authInfo.Token.Id, req.Name, req.Value)
	if err != nil {
		return &pb.UpdateSecretResponse{
			Ok:     false,
			ErrMsg: handleErrMsg(err),
		}, nil
	}

	return &pb.UpdateSecretResponse{
		Ok:     true,
		ErrMsg: "",
	}, nil
}

func (s *WorkspaceSecretService) DeleteSecret(ctx context.Context, req *pb.DeleteSecretRequest) (*pb.DeleteSecretResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.DeleteSecretResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	err := s.backendRepo.DeleteSecret(ctx, authInfo.Workspace, req.Name)
	if err != nil {
		return &pb.DeleteSecretResponse{
			Ok:     false,
			ErrMsg: handleErrMsg(err),
		}, nil
	}

	return &pb.DeleteSecretResponse{
		Ok:     true,
		ErrMsg: "",
	}, nil
}
