package gatewayservices

import (
	"context"
	"database/sql"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) GetOrCreateStub(ctx context.Context, in *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stubConfig := types.StubConfigV1{
		Runtime: types.Runtime{
			Cpu:     in.Cpu,
			Gpu:     types.GpuType(in.Gpu),
			Memory:  in.Memory,
			ImageId: in.ImageId,
		},
		Handler:       in.Handler,
		OnStart:       in.OnStart,
		CallbackUrl:   in.CallbackUrl,
		PythonVersion: in.PythonVersion,
		TaskPolicy: types.TaskPolicy{
			MaxRetries: uint(in.Retries),
			Timeout:    int(in.Timeout),
		},
		KeepWarmSeconds: uint(in.KeepWarmSeconds),
		Concurrency:     uint(in.Concurrency),
		MaxContainers:   uint(in.MaxContainers),
		MaxPendingTasks: uint(in.MaxPendingTasks),
		Volumes:         in.Volumes,
		Secrets:         []types.Secret{},
		Authorized:      in.Authorized,
	}

	// Get secrets
	for _, secret := range in.Secrets {
		secret, err := gws.backendRepo.GetSecretByName(ctx, authInfo.Workspace, secret.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				continue // Skip secret if not found
			}

			return &pb.GetOrCreateStubResponse{
				Ok: false,
			}, nil
		}

		stubConfig.Secrets = append(stubConfig.Secrets, types.Secret{
			Name:  secret.Name,
			Value: secret.Value,
		})

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

	stub, err := gws.backendRepo.GetOrCreateStub(ctx, in.Name, in.StubType, stubConfig, object.Id, authInfo.Workspace.Id, in.ForceCreate)
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

func (gws *GatewayService) DeployStub(ctx context.Context, in *pb.DeployStubRequest) (*pb.DeployStubResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stub, err := gws.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil || stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	lastestDeployment, err := gws.backendRepo.GetLatestDeploymentByName(ctx, authInfo.Workspace.Id, in.Name, string(stub.Type))
	if err != nil {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	version := uint(1)
	if lastestDeployment != nil {
		version = lastestDeployment.Version + 1
	}

	deployment, err := gws.backendRepo.CreateDeployment(ctx, authInfo.Workspace.Id, in.Name, version, stub.Id, string(stub.Type))
	if err != nil {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	return &pb.DeployStubResponse{
		Ok:           true,
		DeploymentId: deployment.ExternalId,
		Version:      uint32(deployment.Version),
	}, nil
}
