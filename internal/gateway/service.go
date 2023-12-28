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
			ContextId: authInfo.Context.ExternalID,
			Ok:        true,
		}, nil
	}

	// See if the this gateway has been configured previously
	existingContexts, err := gws.gw.BackendRepo.ListContexts(ctx)
	if err != nil || len(existingContexts) >= 1 {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Found existing contexts",
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
	token, err := gws.gw.BackendRepo.CreateToken(ctx, context.ID)
	if err != nil {
		return &pb.AuthorizeResponse{
			Ok:       false,
			ErrorMsg: "Failed to create new token",
		}, nil
	}

	return &pb.AuthorizeResponse{
		Ok:        true,
		NewToken:  token.Key,
		ContextId: context.ExternalID,
	}, nil
}

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	filePath := path.Join(GatewayConfig.DefaultObjectPath, in.ObjectId)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &pb.HeadObjectResponse{
				Ok:       true,
				Exists:   false,
				ErrorMsg: "Object not found.",
			}, nil
		}

		return &pb.HeadObjectResponse{
			Ok:       true,
			Exists:   false,
			ErrorMsg: err.Error(),
		}, nil
	}

	metadata := &pb.ObjectMetadata{
		Name: fileInfo.Name(),
		Size: fileInfo.Size(),
	}

	return &pb.HeadObjectResponse{
		Ok:             true,
		Exists:         true,
		ObjectMetadata: metadata,
	}, nil
}

func (gws *GatewayService) PutObject(ctx context.Context, in *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	os.MkdirAll(GatewayConfig.DefaultObjectPath, 0644)

	hash := sha256.Sum256(in.ObjectContent)
	objectId := hex.EncodeToString(hash[:])
	filePath := path.Join(GatewayConfig.DefaultObjectPath, objectId)

	// Check if object already exists
	_, err := os.Stat(filePath)
	if err == nil && !in.Overwrite {
		return &pb.PutObjectResponse{
			Ok:       true,
			ObjectId: objectId,
		}, nil
	}

	err = os.WriteFile(filePath, in.ObjectContent, 0644)
	if err != nil {
		return &pb.PutObjectResponse{
			Ok:       false,
			ErrorMsg: err.Error(),
		}, nil
	}

	return &pb.PutObjectResponse{
		Ok:       true,
		ObjectId: objectId,
	}, nil
}
