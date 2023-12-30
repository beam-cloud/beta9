package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"

	"github.com/beam-cloud/beam/internal/auth"
	pb "github.com/beam-cloud/beam/proto"
)

type GatewayService struct {
	gw *Gateway
	pb.UnimplementedGatewayServiceServer
}

func NewGatewayService(gw *Gateway) (*GatewayService, error) {
	return &GatewayService{
		gw: gw,
	}, nil
}

func (gws *GatewayService) Authorize(ctx context.Context, in *pb.AuthorizeRequest) (*pb.AuthorizeResponse, error) {
	authInfo, authFound := auth.AuthInfoFromContext(ctx)
	if authFound {
		return &pb.AuthorizeResponse{
			ContextId: authInfo.Context.ExternalId,
			Ok:        true,
		}, nil
	}

	// See if the this gateway has been configured previously
	existingContexts, err := gws.gw.BackendRepo.ListContexts(ctx)
	if err != nil || len(existingContexts) >= 1 {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Invalid token",
		}, nil
	}

	// If no contexts are found, we can create a new one for the user
	// and generate a new token
	context, err := gws.gw.BackendRepo.CreateContext(ctx)
	if err != nil {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Failed to create new context",
		}, nil
	}

	// Now that we have a context, create a new token
	token, err := gws.gw.BackendRepo.CreateToken(ctx, context.Id)
	if err != nil {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Failed to create new token",
		}, nil
	}

	return &pb.AuthorizeResponse{
		Ok:        true,
		NewToken:  token.Key,
		ContextId: context.ExternalId,
	}, nil
}

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	existingObject, err := gws.gw.BackendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Context.Id)
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

	objectPath := path.Join(GatewayConfig.DefaultObjectPath, authInfo.Context.Name)
	os.MkdirAll(objectPath, 0644)

	existingObject, err := gws.gw.BackendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Context.Id)
	if err == nil && !in.Overwrite {
		return &pb.PutObjectResponse{
			Ok:       true,
			ObjectId: existingObject.ExternalId,
		}, nil
	}

	hash := sha256.Sum256(in.ObjectContent)
	hashStr := hex.EncodeToString(hash[:])

	newObject, err := gws.gw.BackendRepo.CreateObject(ctx, hashStr, int64(len(in.ObjectContent)), authInfo.Context.Id)
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
