package volume

import (
	"context"
	"log"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/repository"
	pb "github.com/beam-cloud/beam/proto"
)

type VolumeService interface {
	GetORCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error)
}

type StructWorkspaceVolumeService struct {
	pb.UnimplementedVolumeServiceServer
	backendRepo repository.BackendRepository
}

func NewStructWorkspaceVolumeService() (*StructWorkspaceVolumeService, error) {
	backendRepo, err := repository.NewBackendPostgresRepository()
	if err != nil {
		return nil, err
	}

	return &StructWorkspaceVolumeService{
		backendRepo: backendRepo,
	}, nil
}

func (vs *StructWorkspaceVolumeService) GetOrCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceId := authInfo.Workspace.Id

	volume, err := vs.backendRepo.GetOrCreateVolume(ctx, workspaceId, in.Name)
	if err != nil {
		return &pb.GetOrCreateVolumeResponse{
			Ok: false,
		}, err
	}
	log.Println(volume)

	return &pb.GetOrCreateVolumeResponse{
		VolumeId: volume.ExternalId,
		Ok:       true,
	}, nil
}
