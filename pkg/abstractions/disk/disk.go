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
	ListDiskSnapshots(ctx context.Context, in *pb.ListDiskSnapshotsRequest) (*pb.ListDiskSnapshotsResponse, error)
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
		Filesystem:    disk.Filesystem,
		Driver:        disk.Driver,
		MountPath:     disk.MountPath,
		CreatedAt:     timestamppb.New(disk.CreatedAt.Time),
		UpdatedAt:     timestamppb.New(disk.UpdatedAt.Time),
		WorkspaceId:   workspaceExternalId,
		WorkspaceName: workspaceName,
	}
}

func snapshotToProto(snapshot types.DiskSnapshot) *pb.DiskSnapshotInstance {
	instance := &pb.DiskSnapshotInstance{
		Id:               snapshot.ExternalId,
		DiskName:         snapshot.DiskName,
		Format:           snapshot.Format,
		Status:           string(snapshot.Status),
		Generation:       snapshot.Generation,
		SizeBytes:        snapshot.SizeBytes,
		LogicalSizeBytes: snapshot.LogicalSizeBytes,
		StoredSizeBytes:  snapshot.StoredSizeBytes,
		CreatedAt:        timestamppb.New(snapshot.CreatedAt.Time),
	}
	if snapshot.CompletedAt.Valid {
		instance.CompletedAt = timestamppb.New(snapshot.CompletedAt.Time)
	}
	return instance
}

func (ds *GlobalDiskService) GetOrCreateDisk(ctx context.Context, in *pb.GetOrCreateDiskRequest) (*pb.GetOrCreateDiskResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if strings.TrimSpace(in.Name) == "" {
		return &pb.GetOrCreateDiskResponse{Ok: false, ErrMsg: "Disk name is required"}, nil
	}

	disk, err := ds.backendRepo.GetOrCreateDisk(ctx, authInfo.Workspace.Id, &types.Disk{
		Name:       types.SafeDurableDiskName(in.Name),
		Size:       in.Size,
		Filesystem: in.Filesystem,
		Driver:     in.Driver,
		MountPath:  in.MountPath,
	})
	if err != nil {
		return &pb.GetOrCreateDiskResponse{Ok: false, ErrMsg: "Unable to get or create disk"}, nil
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

func (ds *GlobalDiskService) ListDiskSnapshots(ctx context.Context, in *pb.ListDiskSnapshotsRequest) (*pb.ListDiskSnapshotsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	snapshots, err := ds.backendRepo.ListDiskSnapshots(ctx, types.DiskSnapshotFilter{
		WorkspaceId: authInfo.Workspace.Id,
		DiskName:    in.DiskName,
	})
	if err != nil {
		return &pb.ListDiskSnapshotsResponse{Ok: false, ErrMsg: "Unable to list disk snapshots"}, nil
	}

	instances := make([]*pb.DiskSnapshotInstance, 0, len(snapshots))
	for _, snapshot := range snapshots {
		instances = append(instances, snapshotToProto(snapshot))
	}

	return &pb.ListDiskSnapshotsResponse{Ok: true, Snapshots: instances}, nil
}
