package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"

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
