package volume

import (
	"context"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	pb "github.com/beam-cloud/beta9/proto"
)

type VolumeService interface {
	pb.VolumeServiceServer
	GetOrCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error)
}

type GlobalVolumeService struct {
	pb.UnimplementedVolumeServiceServer
	backendRepo repository.BackendRepository
}

func NewGlobalVolumeService(backendRepo repository.BackendRepository) (VolumeService, error) {
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
