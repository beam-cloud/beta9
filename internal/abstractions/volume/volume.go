package volume

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type VolumeService interface {
	pb.VolumeServiceServer
	GetOrCreateVolume(ctx context.Context, in *pb.GetOrCreateVolumeRequest) (*pb.GetOrCreateVolumeResponse, error)
	ListPath(ctx context.Context, in *pb.ListPathRequest) (*pb.ListPathResponse, error)
	DeletePath(ctx context.Context, in *pb.DeletePathRequest) (*pb.DeletePathResponse, error)
	CopyPathStream(stream pb.VolumeService_CopyPathStreamServer) error
}

type GlobalVolumeService struct {
	pb.UnimplementedVolumeServiceServer
	backendRepo repository.BackendRepository
}

type FileInfo struct {
	Path    string `json:"path"`
	Size    uint64 `json:"size"`
	ModTime int64  `json:"mod_time"`
	IsDir   bool   `json:"is_dir"`
}

var volumeRoutePrefix string = "/volume"

func NewGlobalVolumeService(backendRepo repository.BackendRepository, routeGroup *echo.Group) (VolumeService, error) {
	gvs := &GlobalVolumeService{
		backendRepo: backendRepo,
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerVolumeRoutes(routeGroup.Group(volumeRoutePrefix, authMiddleware), gvs)

	return gvs, nil
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
			CreatedAt:     timestamppb.New(volume.CreatedAt),
			UpdatedAt:     timestamppb.New(volume.UpdatedAt),
			WorkspaceId:   authInfo.Workspace.ExternalId,
			WorkspaceName: authInfo.Workspace.Name,
		},
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
			CreatedAt:     timestamppb.New(v.CreatedAt),
			UpdatedAt:     timestamppb.New(v.UpdatedAt),
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
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	ch := make(chan CopyPathContent)

	go func() {
		defer close(ch)

		for {
			request, err := stream.Recv()
			if err != nil {
				return
			}

			ch <- CopyPathContent{
				Path:    request.Path,
				Content: request.Content,
			}
		}
	}()

	if err := vs.copyPathStream(ctx, ch, authInfo.Workspace); err != nil {
		return stream.SendAndClose(&pb.CopyPathResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		})
	}

	return stream.SendAndClose(&pb.CopyPathResponse{Ok: true})
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

// Volume business logic
func (vs *GlobalVolumeService) getOrCreateVolume(ctx context.Context, workspace *types.Workspace, volumeName string) (*types.Volume, error) {
	volume, err := vs.backendRepo.GetOrCreateVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return nil, err
	}

	volumePath := JoinVolumePath(workspace.Name, volume.ExternalId)
	if _, err := os.Stat(volumePath); os.IsNotExist(err) {
		os.MkdirAll(volumePath, os.FileMode(0755))
	}

	return volume, nil
}

func (vs *GlobalVolumeService) listVolumes(ctx context.Context, workspace *types.Workspace) ([]types.VolumeWithRelated, error) {
	volumes, err := vs.backendRepo.ListVolumesWithRelated(ctx, workspace.Id)
	if err != nil {
		return nil, err
	}

	for i, v := range volumes {
		path := JoinVolumePath(workspace.Name, v.ExternalId)

		size, err := CalculateDirSize(path)
		if err != nil {
			size = 0
		}

		volumes[i].Size = size
	}

	return volumes, nil
}

type CopyPathContent struct {
	Path    string
	Content []byte
}

func (vs *GlobalVolumeService) copyPathStream(ctx context.Context, stream <-chan CopyPathContent, workspace *types.Workspace) error {
	var file *os.File
	var fullVolumePath string

	for chunk := range stream {
		if file == nil {
			volumeName, volumePath := parseVolumeInput(chunk.Path)
			if volumeName == "" {
				return errors.New("must provide volume name")
			}
			if volumePath == "" {
				return errors.New("must provide path")
			}

			// Check if the volume exists
			volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, volumeName)
			if err != nil {
				return errors.New("unable to find volume")
			}

			// Get paths and prevent access above parent directory
			_, fullVolumePath, err := GetVolumePaths(workspace.Name, volume.ExternalId, volumePath)
			if err != nil {
				return err
			}

			os.MkdirAll(path.Dir(fullVolumePath), os.FileMode(0755))

			file, err = os.Create(fullVolumePath)
			if err != nil {
				return errors.New("unable to create file on volume")
			}
			defer file.Close()
		}

		if _, err := file.Write(chunk.Content); err != nil {
			os.RemoveAll(fullVolumePath)
			return errors.New("unable to write file content to volume")
		}
	}

	return nil
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

	// Get paths and prevent access above parent directory
	rootVolumePath, fullVolumePath, err := GetVolumePaths(workspace.Name, volume.ExternalId, volumePath)
	if err != nil {
		return nil, err
	}

	// Prevent deleting root volume path
	if fullVolumePath == rootVolumePath {
		return nil, errors.New("parent directory cannot be deleted")
	}

	// Find path matches
	matches, err := filepath.Glob(fullVolumePath)
	if err != nil {
		return nil, errors.New("unable to find files on volume")
	}

	// Delete paths; both files and directories
	deleted := make([]string, len(matches))
	for i, fpath := range matches {
		if err := os.RemoveAll(fpath); err == nil {
			// Modify path to be relative
			deleted[i] = strings.TrimPrefix(fpath, rootVolumePath+"/")
		}
	}

	return deleted, nil
}

func (vs *GlobalVolumeService) getFileFd(
	ctx context.Context,
	path string,
	workspace *types.Workspace,
) (*os.File, error) {
	volumeName, volumePath := parseVolumeInput(path)
	if volumeName == "" {
		return nil, errors.New("must provide volume name")
	}

	// Check if the volume exists
	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return nil, errors.New("unable to find volume")
	}

	// Get paths and prevent access above parent directory
	_, fullVolumePath, err := GetVolumePaths(workspace.Name, volume.ExternalId, volumePath)
	if err != nil {
		return nil, err
	}

	return os.Open(fullVolumePath)
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

	// Get paths and prevent access above parent directory
	rootVolumePath, fullVolumePath, err := GetVolumePaths(workspace.Name, volume.ExternalId, volumePath)
	if err != nil {
		return nil, err
	}

	// List all contents if path is a directory
	if info, err := os.Stat(fullVolumePath); err == nil && info.IsDir() {
		fullVolumePath += "/*"
	}

	// Find path matches
	matches, err := filepath.Glob(fullVolumePath)
	if err != nil {
		return nil, errors.New("unable to find files on volume")
	}

	// Modify paths to be relative
	files := make([]FileInfo, len(matches))
	for i, p := range matches {
		info, _ := os.Stat(p)
		size := info.Size()
		if info.IsDir() {
			s, _ := CalculateDirSize(p)
			size = int64(s)
		}
		files[i] = FileInfo{
			Path:    strings.TrimPrefix(p, rootVolumePath+"/"),
			Size:    uint64(size),
			ModTime: info.ModTime().Unix(),
			IsDir:   info.IsDir(),
		}
	}

	return files, nil
}

func parseVolumeInput(input string) (string, string) {
	volumeName, volumePath := "", ""
	if parts := strings.Split(input, string(os.PathSeparator)); len(parts) > 0 {
		volumeName = parts[0]
		volumePath = strings.Join(parts[1:], string(os.PathSeparator))
	}
	return volumeName, filepath.Clean(volumePath)
}

// GetVolumePaths returns the absolute parent directory and absolute volumePath.
// Because volumePath can contain 1 or more of "..", we will return an error if volumePath
// tries to go above the rootVolumePath. The error should be used to indicate someone accessing
// a directory above a Volume's directory, which may be a security issue depending on the context.
func GetVolumePaths(workspaceName string, volumeExternalId string, volumePath string) (string, string, error) {
	rootVolumePath := JoinVolumePath(workspaceName, volumeExternalId)
	fullVolumePath := JoinVolumePath(workspaceName, volumeExternalId, volumePath)

	if !strings.HasPrefix(fullVolumePath, rootVolumePath) {
		return "", "", errors.New("parent directory does not exist")
	}

	return rootVolumePath, fullVolumePath, nil
}

func JoinVolumePath(workspaceName, volumeExternalId string, subPaths ...string) string {
	return path.Join(append([]string{types.DefaultVolumesPath, workspaceName, volumeExternalId}, subPaths...)...)
}

func CalculateDirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	})
	return size, err
}
