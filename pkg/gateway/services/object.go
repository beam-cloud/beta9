package gatewayservices

import (
	"context"
	"io"
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	useWorkspaceStorage := authInfo.Workspace.StorageAvailable()
	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil {
		exists := true

		objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
		if _, err := os.Stat(objectPath); os.IsNotExist(err) {
			exists = false
		}

		if useWorkspaceStorage {
			storageClient, err := clients.NewStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
			if err != nil {
				return &pb.HeadObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create storage client",
				}, nil
			}

			exists, _, _ = storageClient.Head(ctx, path.Join(types.DefaultObjectPrefix, existingObject.ExternalId))
		}

		if exists {
			return &pb.HeadObjectResponse{
				Ok:     true,
				Exists: true,
				ObjectMetadata: &pb.ObjectMetadata{
					Name: existingObject.Hash,
					Size: existingObject.Size,
				},
				ObjectId:            existingObject.ExternalId,
				UseWorkspaceStorage: useWorkspaceStorage,
			}, nil
		} else {
			return &pb.HeadObjectResponse{
				Ok:                  false,
				Exists:              false,
				UseWorkspaceStorage: useWorkspaceStorage,
			}, nil
		}
	}

	return &pb.HeadObjectResponse{
		Ok:                  true,
		Exists:              false,
		UseWorkspaceStorage: useWorkspaceStorage,
	}, nil
}

func (gws *GatewayService) CreateObject(ctx context.Context, in *pb.CreateObjectRequest) (*pb.CreateObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
	os.MkdirAll(objectPath, 0644)

	storageClient, err := clients.NewStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to create storage client",
		}, nil
	}

	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil && !in.Overwrite {
		return &pb.CreateObjectResponse{
			Ok:       true,
			ObjectId: existingObject.ExternalId,
		}, nil
	}

	newObject, err := gws.backendRepo.CreateObject(ctx, in.Hash, in.Size, authInfo.Workspace.Id)
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to create object",
		}, nil
	}

	presignedURL, err := storageClient.GeneratePresignedURL(ctx, path.Join(types.DefaultObjectPrefix, newObject.ExternalId), 60*60*24)
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to generate presigned URL",
		}, nil
	}

	return &pb.CreateObjectResponse{
		Ok:           true,
		ObjectId:     newObject.ExternalId,
		PresignedUrl: presignedURL,
	}, nil
}

func (gws *GatewayService) PutObjectStream(stream pb.GatewayService_PutObjectStreamServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
	os.MkdirAll(objectPath, 0644)

	var size int
	var file *os.File
	var newObject types.Object

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			return stream.SendAndClose(&pb.PutObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to receive stream of bytes",
			})
		}

		if file == nil {
			newObject, err = gws.backendRepo.CreateObject(ctx, request.Hash, 0, authInfo.Workspace.Id)
			if err != nil {
				return stream.SendAndClose(&pb.PutObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create object",
				})
			}

			file, err = os.Create(path.Join(objectPath, newObject.ExternalId))
			if err != nil {
				gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
				return stream.SendAndClose(&pb.PutObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create file",
				})
			}
			defer file.Close()
		}

		s, err := file.Write(request.ObjectContent)
		if err != nil {
			os.Remove(path.Join(objectPath, newObject.ExternalId))
			gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
			return stream.SendAndClose(&pb.PutObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to write file content",
			})
		}
		size += s
	}

	if err := gws.backendRepo.UpdateObjectSizeByExternalId(ctx, newObject.ExternalId, size); err != nil {
		os.Remove(path.Join(objectPath, newObject.ExternalId))
		gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		return stream.SendAndClose(&pb.PutObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to complete file upload",
		})
	}

	return stream.SendAndClose(&pb.PutObjectResponse{
		Ok:       true,
		ObjectId: newObject.ExternalId,
	})
}
