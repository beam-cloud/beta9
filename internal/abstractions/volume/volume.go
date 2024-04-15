package volume

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
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

func (vs *GlobalVolumeService) ListPath(ctx context.Context, in *pb.ListPathRequest) (*pb.ListPathResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	paths, err := vs.listPath(ctx, in.Path, authInfo.Workspace)
	if err != nil {
		return &pb.ListPathResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.ListPathResponse{
		Ok:    true,
		Paths: paths,
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
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			ch <- CopyPathContent{
				Path:    request.Path,
				Content: request.Content,
			}
		}
	}()

	if err := vs.copyPathStream(ctx, ch, authInfo.Workspace); err != nil {
		return stream.SendAndClose(&pb.CopyPathResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
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

			rootVolumePath := GetVolumePath(workspace.Name, volume.ExternalId)
			fullVolumePath = GetVolumePath(workspace.Name, volume.ExternalId, volumePath)

			// Prevent access above parent directory
			if !strings.HasPrefix(fullVolumePath, rootVolumePath) {
				return errors.New("parent directory does not exist")
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

	// Set up paths
	rootVolumePath := GetVolumePath(workspace.Name, volume.ExternalId)
	fullVolumePath := GetVolumePath(workspace.Name, volume.ExternalId, volumePath)

	// Prevent access above parent directory
	if !strings.HasPrefix(fullVolumePath, rootVolumePath) {
		return nil, errors.New("parent directory does not exist")
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

func (vs *GlobalVolumeService) listPath(ctx context.Context, inputPath string, workspace *types.Workspace) ([]string, error) {
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

	// Set up paths
	rootVolumePath := GetVolumePath(workspace.Name, volume.ExternalId)
	fullVolumePath := GetVolumePath(workspace.Name, volume.ExternalId, volumePath)

	// Prevent access above parent directory
	if !strings.HasPrefix(fullVolumePath, rootVolumePath) {
		return nil, errors.New("parent directory does not exist")
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
	for i, p := range matches {
		matches[i] = strings.TrimPrefix(p, rootVolumePath+"/")
	}

	return matches, nil
}

func parseVolumeInput(input string) (string, string) {
	volumeName, volumePath := "", ""
	if parts := strings.Split(input, string(os.PathSeparator)); len(parts) > 0 {
		volumeName = parts[0]
		volumePath = strings.Join(parts[1:], string(os.PathSeparator))
	}
	return volumeName, filepath.Clean(volumePath)
}

func GetVolumePath(workspaceName, volumeExternalId string, subPaths ...string) string {
	return path.Join(append([]string{types.DefaultVolumesPath, workspaceName, volumeExternalId}, subPaths...)...)
}
