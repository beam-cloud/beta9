package gatewayservices

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
)

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Context.Id)
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

	objectPath := path.Join(types.DefaultObjectPath, authInfo.Context.Name)
	os.MkdirAll(objectPath, 0644)

	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Context.Id)
	if err == nil && !in.Overwrite {
		return &pb.PutObjectResponse{
			Ok:       true,
			ObjectId: existingObject.ExternalId,
		}, nil
	}

	hash := sha256.Sum256(in.ObjectContent)
	hashStr := hex.EncodeToString(hash[:])

	newObject, err := gws.backendRepo.CreateObject(ctx, hashStr, int64(len(in.ObjectContent)), authInfo.Context.Id)
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
			ErrorMsg: "Unable to write files",
		}, nil
	}

	return &pb.PutObjectResponse{
		Ok:       true,
		ObjectId: newObject.ExternalId,
	}, nil
}
