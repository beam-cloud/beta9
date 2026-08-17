package disk

import (
	"context"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DiskService interface {
	pb.DiskServiceServer
	GetOrCreateDisk(ctx context.Context, in *pb.GetOrCreateDiskRequest) (*pb.GetOrCreateDiskResponse, error)
	ListDisks(ctx context.Context, in *pb.ListDisksRequest) (*pb.ListDisksResponse, error)
	DeleteDisk(ctx context.Context, in *pb.DeleteDiskRequest) (*pb.DeleteDiskResponse, error)
}

type GlobalDiskService struct {
	pb.UnimplementedDiskServiceServer
	backendRepo repository.BackendRepository
}

const diskRoutePrefix = "/disk"

func NewGlobalDiskService(backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, routeGroup *echo.Group) (DiskService, error) {
	gds := &GlobalDiskService{
		backendRepo: backendRepo,
	}

	authMiddleware := auth.AuthMiddleware(backendRepo, workspaceRepo)
	registerDiskRoutes(routeGroup.Group(diskRoutePrefix, authMiddleware), gds)

	return gds, nil
}

func diskToProto(disk *types.Disk, workspaceExternalId, workspaceName string) *pb.DiskInstance {
	return &pb.DiskInstance{
		Id:            disk.ExternalId,
		Name:          disk.Name,
		Size:          disk.Size,
		MountPath:     disk.MountPath,
		CreatedAt:     timestamppb.New(disk.CreatedAt.Time),
		UpdatedAt:     timestamppb.New(disk.UpdatedAt.Time),
		WorkspaceId:   workspaceExternalId,
		WorkspaceName: workspaceName,
	}
}

func (ds *GlobalDiskService) GetOrCreateDisk(ctx context.Context, in *pb.GetOrCreateDiskRequest) (*pb.GetOrCreateDiskResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if strings.TrimSpace(in.Name) == "" {
		return &pb.GetOrCreateDiskResponse{Ok: false, ErrMsg: "Disk name is required"}, nil
	}

	disk, err := ds.backendRepo.GetOrCreateDisk(ctx, authInfo.Workspace.Id, &types.Disk{
		Name:      types.SafeDurableDiskName(in.Name),
		Size:      in.Size,
		MountPath: in.MountPath,
	})
	if err != nil {
		return &pb.GetOrCreateDiskResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	return &pb.GetOrCreateDiskResponse{
		Ok:   true,
		Disk: diskToProto(disk, authInfo.Workspace.ExternalId, authInfo.Workspace.Name),
	}, nil
}

func (ds *GlobalDiskService) ListDisks(ctx context.Context, in *pb.ListDisksRequest) (*pb.ListDisksResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	disks, err := ds.backendRepo.ListDisksWithRelated(ctx, authInfo.Workspace.Id)
	if err != nil {
		return &pb.ListDisksResponse{Ok: false, ErrMsg: "Unable to list disks"}, nil
	}

	instances := make([]*pb.DiskInstance, 0, len(disks))
	for _, disk := range disks {
		workspaceExternalId := disk.Workspace.ExternalId
		if workspaceExternalId == "" {
			workspaceExternalId = authInfo.Workspace.ExternalId
		}
		instances = append(instances, diskToProto(&disk.Disk, workspaceExternalId, disk.Workspace.Name))
	}

	return &pb.ListDisksResponse{Ok: true, Disks: instances}, nil
}

func (ds *GlobalDiskService) DeleteDisk(ctx context.Context, in *pb.DeleteDiskRequest) (*pb.DeleteDiskResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.DeleteDiskResponse{Ok: false, ErrMsg: "Unauthorized Access"}, nil
	}

	if strings.TrimSpace(in.Name) == "" {
		return &pb.DeleteDiskResponse{Ok: false, ErrMsg: "Disk name is required"}, nil
	}

	if err := ds.backendRepo.DeleteDisk(ctx, authInfo.Workspace.Id, in.Name); err != nil {
		return &pb.DeleteDiskResponse{Ok: false, ErrMsg: "Unable to delete disk"}, nil
	}

	return &pb.DeleteDiskResponse{Ok: true}, nil
}
