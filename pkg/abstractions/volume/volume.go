package volume

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type VolumeService interface {
	pb.VolumeServiceServer
	GetOrCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error)
	DeleteVolume(ctx context.Context, in *pb.DeleteVolumeRequest) (*pb.DeleteVolumeResponse, error)
	ListPath(ctx context.Context, in *pb.ListPathRequest) (*pb.ListPathResponse, error)
	DeletePath(ctx context.Context, in *pb.DeletePathRequest) (*pb.DeletePathResponse, error)
	MovePath(ctx context.Context, in *pb.MovePathRequest) (*pb.MovePathResponse, error)
	CopyPathStream(stream pb.VolumeService_CopyPathStreamServer) error
}

type GlobalVolumeService struct {
	pb.UnimplementedVolumeServiceServer
	config                 types.FileServiceConfig
	workspaceStorageConfig types.WorkspaceStorageConfig
	backendRepo            repository.BackendRepository
}

type FileInfo struct {
	Path    string `json:"path"`
	Size    uint64 `json:"size"`
	ModTime int64  `json:"mod_time"`
	IsDir   bool   `json:"is_dir"`
}

var volumeRoutePrefix string = "/volume"

func NewGlobalVolumeService(config types.FileServiceConfig, workspaceStorageConfig types.WorkspaceStorageConfig, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, routeGroup *echo.Group) (VolumeService, error) {
	gvs := &GlobalVolumeService{
		config:                 config,
		workspaceStorageConfig: workspaceStorageConfig,
		backendRepo:            backendRepo,
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo, workspaceRepo)
	registerVolumeRoutes(routeGroup.Group(volumeRoutePrefix, authMiddleware), gvs)

	return gvs, nil
}

func (vs *GlobalVolumeService) newWorkspaceStorageClientForPresign(ctx context.Context, workspace *types.Workspace) (*clients.WorkspaceStorageClient, error) {
	if !workspace.StorageAvailable() {
		return nil, errors.New("workspace storage is unavailable")
	}
	return clients.NewWorkspaceStorageClientWithDefaultPresignEndpoint(
		ctx,
		workspace.Name,
		workspace.Storage,
		vs.workspaceStorageConfig,
	)
}

func (vs *GlobalVolumeService) GetOrCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	volume, err := vs.getOrCreateVolume(ctx, authInfo.Workspace, in.Name)
	if err != nil {
		return &pb.GetOrCreateVolumeResponse{
			Ok:     false,
			ErrMsg: "Unable to get or create volume",
		}, nil
	}

	return &pb.GetOrCreateVolumeResponse{
		Ok: true,
		Volume: &pb.VolumeInstance{
			Id:            volume.ExternalId,
			Name:          volume.Name,
			CreatedAt:     timestamppb.New(volume.CreatedAt.Time),
			UpdatedAt:     timestamppb.New(volume.UpdatedAt.Time),
			WorkspaceId:   authInfo.Workspace.ExternalId,
			WorkspaceName: authInfo.Workspace.Name,
		},
	}, nil
}

func (vs *GlobalVolumeService) DeleteVolume(ctx context.Context, in *pb.DeleteVolumeRequest) (*pb.DeleteVolumeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.DeleteVolumeResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	if err := vs.deleteVolume(ctx, authInfo.Workspace, in.Name); err != nil {
		return &pb.DeleteVolumeResponse{
			Ok:     false,
			ErrMsg: "Unable to delete volume",
		}, nil
	}

	return &pb.DeleteVolumeResponse{
		Ok: true,
	}, nil
}

func (vs *GlobalVolumeService) ListVolumes(ctx context.Context, in *pb.ListVolumesRequest) (*pb.ListVolumesResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	volumes, err := vs.listVolumes(ctx, authInfo.Workspace)
	if err != nil {
		return &pb.ListVolumesResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	vols := make([]*pb.VolumeInstance, len(volumes))
	for i, v := range volumes {
		vols[i] = &pb.VolumeInstance{
			Id:            v.ExternalId,
			Name:          v.Name,
			Size:          v.Size,
			CreatedAt:     timestamppb.New(v.CreatedAt.Time),
			UpdatedAt:     timestamppb.New(v.UpdatedAt.Time),
			WorkspaceId:   v.Workspace.ExternalId,
			WorkspaceName: v.Workspace.Name,
		}
	}

	return &pb.ListVolumesResponse{
		Ok:      true,
		Volumes: vols,
	}, nil
}

func (vs *GlobalVolumeService) ListPath(ctx context.Context, in *pb.ListPathRequest) (*pb.ListPathResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	paths, err := vs.listPath(ctx, in.Path, authInfo.Workspace)
	if err != nil {
		return &pb.ListPathResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	infos := make([]*pb.PathInfo, len(paths))
	for i, info := range paths {
		infos[i] = &pb.PathInfo{
			Path:    info.Path,
			Size:    info.Size,
			ModTime: timestamppb.New(time.Unix(info.ModTime, 0)),
			IsDir:   info.IsDir,
		}
	}

	return &pb.ListPathResponse{
		Ok:        true,
		PathInfos: infos,
	}, nil
}

func (vs *GlobalVolumeService) CopyPathStream(stream pb.VolumeService_CopyPathStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())
	reader := common.NewChunkReader(first.Content, func() ([]byte, error) {
		chunk, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if chunk.Path != first.Path {
			return nil, errors.New("volume path changed during upload")
		}
		return chunk.Content, nil
	})
	if err := vs.copyPath(stream.Context(), first.Path, reader, authInfo.Workspace); err != nil {
		return stream.SendAndClose(&pb.CopyPathResponse{ErrMsg: err.Error()})
	}
	return stream.SendAndClose(&pb.CopyPathResponse{Ok: true})
}

func (vs *GlobalVolumeService) objectKey(ctx context.Context, input string, workspace *types.Workspace) (string, error) {
	name, relative := parseVolumeInput(input)
	if name == "" {
		return "", errors.New("must provide volume name")
	}
	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, name)
	if err != nil {
		return "", err
	}
	return volumeObjectKey(volume.ExternalId, relative)
}

func (vs *GlobalVolumeService) copyPath(ctx context.Context, input string, reader io.Reader, workspace *types.Workspace) error {
	key, err := vs.objectKey(ctx, input, workspace)
	if err != nil {
		return err
	}
	client, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return err
	}
	return client.UploadWithReader(ctx, key, reader)
}

func (vs *GlobalVolumeService) DeletePath(ctx context.Context, in *pb.DeletePathRequest) (*pb.DeletePathResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	paths, err := vs.deletePath(ctx, in.Path, authInfo.Workspace)
	if err != nil {
		return &pb.DeletePathResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.DeletePathResponse{
		Ok:      true,
		Deleted: paths,
	}, nil
}

func (vs *GlobalVolumeService) MovePath(ctx context.Context, in *pb.MovePathRequest) (*pb.MovePathResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	newPath, err := vs.movePath(ctx, in.GetOriginalPath(), in.GetNewPath(), authInfo.Workspace)
	if err != nil {
		return &pb.MovePathResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.MovePathResponse{
		Ok:      true,
		NewPath: newPath,
	}, nil
}

func (vs *GlobalVolumeService) StatPath(ctx context.Context, in *pb.StatPathRequest) (*pb.StatPathResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	key, err := vs.objectKey(ctx, in.Path, authInfo.Workspace)
	if err != nil {
		return &pb.StatPathResponse{ErrMsg: err.Error()}, nil
	}
	client, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
	if err != nil {
		return &pb.StatPathResponse{ErrMsg: err.Error()}, nil
	}
	exists, object, err := client.Head(ctx, key)
	if err != nil {
		return &pb.StatPathResponse{ErrMsg: err.Error()}, nil
	}
	if exists {
		return &pb.StatPathResponse{Ok: true, PathInfo: &pb.PathInfo{Path: in.Path, Size: uint64(aws.ToInt64(object.ContentLength)), ModTime: timestamppb.New(aws.ToTime(object.LastModified))}}, nil
	}
	children, err := client.ListDirectory(ctx, key)
	if err != nil {
		return &pb.StatPathResponse{ErrMsg: err.Error()}, nil
	}
	if len(children) > 0 {
		return &pb.StatPathResponse{Ok: true, PathInfo: &pb.PathInfo{Path: in.Path, IsDir: true}}, nil
	}
	return &pb.StatPathResponse{Ok: true, ErrMsg: "Path does not exist"}, nil
}

// Volume business logic
func (vs *GlobalVolumeService) getOrCreateVolume(ctx context.Context, workspace *types.Workspace, volumeName string) (*types.Volume, error) {
	return vs.backendRepo.GetOrCreateVolume(ctx, workspace.Id, volumeName)
}

func (vs *GlobalVolumeService) deleteVolume(ctx context.Context, workspace *types.Workspace, volumeName string) error {
	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return err
	}

	storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return err
	}

	volumePath := path.Join(types.DefaultVolumesPrefix, volume.ExternalId)
	_, err = storageClient.DeleteWithPrefix(ctx, volumePath)
	if err != nil {
		return err
	}

	return vs.backendRepo.DeleteVolume(ctx, volume.WorkspaceId, volume.Name)

}

func (vs *GlobalVolumeService) listVolumes(ctx context.Context, workspace *types.Workspace) ([]types.VolumeWithRelated, error) {
	volumes, err := vs.backendRepo.ListVolumesWithRelated(ctx, workspace.Id)
	if err != nil {
		return nil, err
	}

	return volumes, nil
}

func (vs *GlobalVolumeService) deletePath(ctx context.Context, inputPath string, workspace *types.Workspace) ([]string, error) {
	// Parse the volume and path/pattern
	volumeName, volumePath := parseVolumeInput(inputPath)
	if volumeName == "" {
		return nil, errors.New("must provide volume name")
	}
	if volumePath == "" {
		return nil, errors.New("must provide path or pattern")
	}

	// Check if the volume exists
	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return nil, errors.New("unable to find volume")
	}

	volumePath, err = volumeObjectKey(volume.ExternalId, volumePath)
	if err != nil {
		return nil, err
	}

	storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return nil, err
	}

	deleted, err := storageClient.DeleteWithPrefix(ctx, volumePath)
	if err != nil {
		return nil, err
	}

	return deleted, nil

}

func (vs *GlobalVolumeService) listPath(ctx context.Context, inputPath string, workspace *types.Workspace) ([]FileInfo, error) {
	// Parse the volume and path/pattern
	volumeName, volumePath := parseVolumeInput(inputPath)
	if volumeName == "" {
		return nil, errors.New("must provide volume name")
	}

	// Check if the volume exists
	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return nil, errors.New("unable to find volume")
	}

	fullVolumePath, err := volumeObjectKey(volume.ExternalId, volumePath)
	if err != nil {
		return nil, err
	}

	storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return nil, err
	}

	rootVolumePath := path.Join(types.DefaultVolumesPrefix, volume.ExternalId)
	objects, err := storageClient.ListDirectory(ctx, fullVolumePath)
	if err != nil {
		return nil, errors.New("unable to list files")
	}

	files := make([]FileInfo, 0, len(objects))

	for _, obj := range objects {
		isDir := strings.HasSuffix(*obj.Key, "/")

		files = append(files, FileInfo{
			Path:    strings.TrimSuffix(strings.TrimPrefix(*obj.Key, rootVolumePath+"/"), "/"),
			Size:    uint64(*obj.Size),
			ModTime: obj.LastModified.Unix(),
			IsDir:   isDir,
		})
	}

	return files, nil
}

func parseVolumeInput(input string) (string, string) {
	volumeName, volumePath := "", ""
	if parts := strings.Split(input, "/"); len(parts) > 0 {
		volumeName = parts[0]
		volumePath = strings.Join(parts[1:], "/")
	}

	return volumeName, path.Clean(volumePath)
}

func (vs *GlobalVolumeService) movePath(ctx context.Context, originalPath string, newPath string, workspace *types.Workspace) (string, error) {
	originalVolumeName, originalRelativePath := parseVolumeInput(originalPath)
	newVolumeName, newRelativePath := parseVolumeInput(newPath)

	if originalVolumeName != newVolumeName {
		return "", errors.New("moving across different volumes is not supported")
	}

	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, originalVolumeName)
	if err != nil {
		return "", errors.New("unable to find volume")
	}

	originalVolumePath, err := volumeObjectKey(volume.ExternalId, originalRelativePath)
	if err != nil {
		return "", err
	}
	newVolumePath, err := volumeObjectKey(volume.ExternalId, newRelativePath)
	if err != nil {
		return "", err
	}

	storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return "", err
	}

	err = storageClient.MoveObject(ctx, originalVolumePath, newVolumePath)
	if err != nil {
		return "", err
	}

	return newPath, nil

}
