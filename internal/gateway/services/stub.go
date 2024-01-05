package gatewayservices

import (
	"context"
	"encoding/json"
	"log"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
)

func (gws *GatewayService) GetOrCreateStub(ctx context.Context, in *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	volumes := []types.Volume{}
	err := json.Unmarshal(in.Volumes, &volumes)
	if err != nil {
		log.Println(err)
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	stubConfig := types.StubConfigV1{
		Runtime: types.Runtime{
			Cpu:     in.Cpu,
			Gpu:     types.GpuType(in.Gpu),
			Memory:  in.Memory,
			ImageId: in.ImageId,
		},
		Volumes: volumes,
	}

	object, err := gws.backendRepo.GetObjectByExternalId(ctx, in.ObjectId, authInfo.Workspace.Id)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	err = common.ExtractObjectFile(ctx, object.ExternalId, authInfo.Workspace.Name)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	stub, err := gws.backendRepo.GetOrCreateStub(ctx, in.Name, in.StubType, stubConfig, object.Id, authInfo.Workspace.Id)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	return &pb.GetOrCreateStubResponse{
		Ok:     err == nil,
		StubId: stub.ExternalId,
	}, nil
}
