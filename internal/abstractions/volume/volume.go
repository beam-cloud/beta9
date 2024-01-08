package volume

import (
	"context"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/repository"
	pb "github.com/beam-cloud/beam/proto"
)

type VolumeService interface {
	GetOrCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error)
}

type GlobalVolumeService struct {
	pb.UnimplementedVolumeServiceServer
	backendRepo repository.BackendRepository
}

func NewGlobalVolumeService(backendRepo repository.BackendRepository) (*GlobalVolumeService, error) {
	return &GlobalVolumeService{
		backendRepo: backendRepo,
	}, nil
}

func (vs *GlobalVolumeService) GetOrCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceId := authInfo.Workspace.Id

	volume, err := vs.backendRepo.GetOrCreateVolume(ctx, workspaceId, in.Name)
	if err != nil {
		return &pb.GetOrCreateVolumeResponse{
			Ok: false,
		}, nil
	}

	return &pb.GetOrCreateVolumeResponse{
		VolumeId: volume.ExternalId,
		Ok:       true,
	}, nil
}
