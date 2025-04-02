package volume

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

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
	config      types.FileServiceConfig
	backendRepo repository.BackendRepository
	rdb         *common.RedisClient
}

type FileInfo struct {
	Path    string `json:"path"`
	Size    uint64 `json:"size"`
	ModTime int64  `json:"mod_time"`
	IsDir   bool   `json:"is_dir"`
}

type VolumePathTokenData struct {
	WorkspaceId string `redis:"workspace_id"`
	VolumePath  string `redis:"volume_path"`
}

var volumeRoutePrefix string = "/volume"

func NewGlobalVolumeService(config types.FileServiceConfig, backendRepo repository.BackendRepository, workspaceRepo repository.WorkspaceRepository, rdb *common.RedisClient, routeGroup *echo.Group) (VolumeService, error) {
	gvs := &GlobalVolumeService{
		config:      config,
		backendRepo: backendRepo,
		rdb:         rdb,
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo, workspaceRepo)
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

func (vs *GlobalVolumeService) DeleteVolume(ctx context.Context, in *pb.DeleteVolumeRequest) (*pb.DeleteVolumeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

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

	log.Printf("CopyPathStream: %v", authInfo.Workspace.Name)

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

	path, err := vs.getFilePath(ctx, in.Path, authInfo.Workspace)
	if err != nil {
		return &pb.StatPathResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return &pb.StatPathResponse{
			Ok:     true,
			ErrMsg: "Path does not exist",
		}, nil
	}

	return &pb.StatPathResponse{
		Ok: true,
		PathInfo: &pb.PathInfo{
			Path:    in.Path,
			Size:    uint64(info.Size()),
			ModTime: timestamppb.New(info.ModTime()),
			IsDir:   info.IsDir(),
		},
	}, nil
}

// Volume business logic
func (vs *GlobalVolumeService) getOrCreateVolume(ctx context.Context, workspace *types.Workspace, volumeName string) (*types.Volume, error) {
	volume, err := vs.backendRepo.GetOrCreateVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return nil, err
	}

	if workspace.StorageAvailable() {
		return volume, nil
	}

	volumePath := JoinVolumePath(workspace.Name, volume.ExternalId)
	if _, err := os.Stat(volumePath); os.IsNotExist(err) {
		os.MkdirAll(volumePath, os.FileMode(0755))
	}

	return volume, nil
}

func (vs *GlobalVolumeService) deleteVolume(ctx context.Context, workspace *types.Workspace, volumeName string) error {
	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return err
	}

	if workspace.StorageAvailable() {
		storageClient, err := clients.NewStorageClient(ctx, workspace.Name, workspace.Storage)
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

	volumeDir, _, err := GetVolumePaths(workspace.Name, volume.ExternalId, "")
	if err != nil {
		return err
	}

	if err := os.RemoveAll(volumeDir); err != nil {
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

type CopyPathContent struct {
	Path    string
	Content []byte
}

func (vs *GlobalVolumeService) copyPathStream(ctx context.Context, stream <-chan CopyPathContent, workspace *types.Workspace) error {
	var file *os.File
	var fullVolumePath string
	var tmpFileSuffix string = ".tmp"

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
			_, fullVolumePath, err = GetVolumePaths(workspace.Name, volume.ExternalId, volumePath)
			if err != nil {
				return err
			}

			os.MkdirAll(path.Dir(fullVolumePath), os.FileMode(0755))
			file, err = os.Create(fullVolumePath + tmpFileSuffix)
			if err != nil {
				return errors.New("unable to create file on volume")
			}
			defer file.Close()
		}

		if _, err := file.Write(chunk.Content); err != nil {
			os.RemoveAll(fullVolumePath + tmpFileSuffix)
			return errors.New("unable to write file content to volume")
		}
	}

	err := file.Sync()
	if err != nil {
		return err
	}

	return os.Rename(fullVolumePath+tmpFileSuffix, fullVolumePath)
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

	if workspace.StorageAvailable() {
		storageClient, err := clients.NewStorageClient(ctx, workspace.Name, workspace.Storage)
		if err != nil {
			return nil, err
		}

		volumePath = path.Join(types.DefaultVolumesPrefix, volume.ExternalId, volumePath)
		deleted, err := storageClient.DeleteWithPrefix(ctx, volumePath)
		if err != nil {
			return nil, err
		}

		return deleted, nil
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

func (vs *GlobalVolumeService) getFilePath(
	ctx context.Context,
	path string,
	workspace *types.Workspace,
) (string, error) {
	volumeName, volumePath := parseVolumeInput(path)
	if volumeName == "" {
		return "", errors.New("must provide volume name")
	}

	// Check if the volume exists
	volume, err := vs.backendRepo.GetVolume(ctx, workspace.Id, volumeName)
	if err != nil {
		return "", errors.New("unable to find volume")
	}

	// Get paths and prevent access above parent directory
	_, fullVolumePath, err := GetVolumePaths(workspace.Name, volume.ExternalId, volumePath)
	if err != nil {
		return "", err
	}

	return fullVolumePath, err
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

	var files []FileInfo = []FileInfo{}

	if workspace.StorageAvailable() {
		storageClient, err := clients.NewStorageClient(ctx, workspace.Name, workspace.Storage)
		if err != nil {
			return nil, err
		}

		volumePath = path.Join(types.DefaultVolumesPrefix, volume.ExternalId, volumePath)
		objects, err := storageClient.ListDirectory(ctx, volumePath)
		if err != nil {
			return nil, errors.New("unable to list files")
		}

		files = make([]FileInfo, 0, len(objects))

		for _, obj := range objects {
			isDir := strings.HasSuffix(*obj.Key, "/")

			path := strings.TrimPrefix(*obj.Key, volumePath+"/")
			if isDir {
				path = strings.TrimSuffix(path, "/")
			}

			files = append(files, FileInfo{
				Path:    path,
				Size:    uint64(*obj.Size),
				ModTime: obj.LastModified.Unix(),
				IsDir:   isDir,
			})
		}
	} else {
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

		files = make([]FileInfo, len(matches))

		// Modify paths to be relative
		for i, p := range matches {
			info, _ := os.Stat(p)
			size := info.Size()
			files[i] = FileInfo{
				Path:    strings.TrimPrefix(p, rootVolumePath+"/"),
				Size:    uint64(size),
				ModTime: info.ModTime().Unix(),
				IsDir:   info.IsDir(),
			}
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

	if workspace.StorageAvailable() {
		storageClient, err := clients.NewStorageClient(ctx, workspace.Name, workspace.Storage)
		if err != nil {
			return "", err
		}

		originalVolumePath := path.Join(types.DefaultVolumesPrefix, volume.ExternalId, originalRelativePath)
		newVolumePath := path.Join(types.DefaultVolumesPrefix, volume.ExternalId, newRelativePath)

		err = storageClient.MoveObject(ctx, originalVolumePath, newVolumePath)
		if err != nil {
			return "", err
		}

		return newPath, nil
	}

	_, originalFullPath, _ := GetVolumePaths(workspace.Name, volume.ExternalId, originalRelativePath)

	if _, err := os.Stat(originalFullPath); os.IsNotExist(err) {
		return "", fmt.Errorf("error finding original path %s", originalPath)
	}

	_, newFullPath, _ := GetVolumePaths(workspace.Name, volume.ExternalId, newRelativePath)
	os.MkdirAll(path.Dir(newFullPath), os.FileMode(0755))

	if err := os.Rename(originalFullPath, newFullPath); err != nil {
		return "", fmt.Errorf("failed to move from %s to %s: no such file or directory", originalPath, newPath)
	}

	originalDir := path.Dir(originalFullPath)
	if _, err := os.Stat(originalDir); !os.IsNotExist(err) {
		if err := os.Remove(originalDir); err != nil {
			fmt.Printf("Non-critical: failed to remove original directory %s, might not be empty: %v\n", originalDir, err)
		}
	}

	return newPath, nil
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

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return 0, nil
	}

	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return err
	})
	return size, err
}

func (gvs *GlobalVolumeService) ValidateWorkspaceVolumePathDownloadToken(workspaceId string, volumePath string, token string) error {
	key := common.RedisKeys.WorkspaceVolumePathDownloadToken(token)

	var data VolumePathTokenData
	res, err := gvs.rdb.HGetAll(context.TODO(), key).Result()
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return errors.New("invalid token")
	}

	err = common.ToStruct(res, &data)
	if err != nil {
		return err
	}

	if data.WorkspaceId != workspaceId || data.VolumePath != volumePath {
		return errors.New("invalid token")
	}

	return nil
}

func generateToken(length int) (string, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

const temporaryDownloadTokenLength = 32
const temporaryDownloadTokenExpiry = 60 // 1 minute

func (gvs *GlobalVolumeService) GenerateWorkspaceVolumePathDownloadToken(workspaceId string, volumePath string) (string, error) {
	token, err := generateToken(temporaryDownloadTokenLength)
	if err != nil {
		return "", err
	}

	key := common.RedisKeys.WorkspaceVolumePathDownloadToken(token)

	data := VolumePathTokenData{
		WorkspaceId: workspaceId,
		VolumePath:  volumePath,
	}

	err = gvs.rdb.HMSet(context.TODO(), key, common.ToSlice(data)...).Err()
	if err != nil {
		return "", err
	}

	err = gvs.rdb.Expire(context.TODO(), key, temporaryDownloadTokenExpiry*time.Second).Err()
	if err != nil {
		return "", err
	}

	return token, nil
}
