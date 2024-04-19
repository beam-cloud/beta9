package gatewayservices

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil {
		return &pb.HeadObjectResponse{
			Ok:     true,
			Exists: true,
			ObjectMetadata: &pb.ObjectMetadata{
				Name: existingObject.Hash,
				Size: existingObject.Size,
			},
			ObjectId: existingObject.ExternalId,
		}, nil
	}

	return &pb.HeadObjectResponse{
		Ok:     true,
		Exists: false,
	}, nil
}

func (gws *GatewayService) PutObject(ctx context.Context, in *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
	os.MkdirAll(objectPath, 0644)

	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil && !in.Overwrite {
		return &pb.PutObjectResponse{
			Ok:       true,
			ObjectId: existingObject.ExternalId,
		}, nil
	}

	hash := sha256.Sum256(in.ObjectContent)
	hashStr := hex.EncodeToString(hash[:])

	newObject, err := gws.backendRepo.CreateObject(ctx, hashStr, int64(len(in.ObjectContent)), authInfo.Workspace.Id)
	if err != nil {
		return &pb.PutObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to create object",
		}, nil
	}

	filePath := path.Join(objectPath, newObject.ExternalId)
	err = os.WriteFile(filePath, in.ObjectContent, 0644)
	if err != nil {
		return &pb.PutObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to write file",
		}, nil
	}

	return &pb.PutObjectResponse{
		Ok:       true,
		ObjectId: newObject.ExternalId,
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

// ReplaceObjectContent modifies files in an extracted object directory
func (gws *GatewayService) ReplaceObjectContent(stream pb.GatewayService_ReplaceObjectContentServer) error {
	ctx := stream.Context()

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	extractedObjectPath := path.Join(types.DefaultExtractedObjectPath, authInfo.Workspace.Name)
	os.MkdirAll(extractedObjectPath, 0644)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}

		log.Println("req: ", req)

		if err != nil {
			return status.Errorf(codes.Unknown, "Received an error: %v", err)
		}

		destPath := path.Join(types.DefaultExtractedObjectPath, authInfo.Workspace.Name, req.ObjectId, req.Path)

		switch req.Op {
		case pb.ReplaceObjectContentOperation_DELETE:
			os.RemoveAll(destPath)
		case pb.ReplaceObjectContentOperation_WRITE:
			os.MkdirAll(destPath, 0644)
			os.WriteFile(destPath, req.Data, 0644)
		}
	}

	return stream.SendAndClose(&pb.ReplaceObjectContentResponse{Ok: true})
}
