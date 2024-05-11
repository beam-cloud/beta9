package gatewayservices

import (
	"context"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/internal/auth"
	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/ptr"
)

func (gws *GatewayService) ListDeployments(ctx context.Context, in *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	filter := types.DeploymentFilter{
		WorkspaceID: authInfo.Workspace.Id,
	}

	limit := uint32(1000)
	if in.Limit > 0 && in.Limit < limit {
		limit = in.Limit
	}
	filter.Limit = limit

	for field, value := range in.Filters {
		switch field {
		case "name":
			filter.Name = value.Values[0]
		case "active":
			v := strings.ToLower(value.Values[0])

			if v == "yes" || v == "y" {
				filter.Active = ptr.To(true)
			} else if v == "no" || v == "n" {
				filter.Active = ptr.To(false)
			} else if val, err := strconv.ParseBool(v); err == nil {
				filter.Active = ptr.To(val)
			}
		case "version":
			val, err := strconv.ParseUint(value.Values[0], 10, 32)
			if err == nil {
				filter.Version = uint(val)
			}
		}
	}

	deploymentsWithRelated, err := gws.backendRepo.ListDeploymentsWithRelated(ctx, filter)
	if err != nil {
		return &pb.ListDeploymentsResponse{
			Ok:     false,
			ErrMsg: "Unable to list deployments",
		}, nil
	}

	deployments := make([]*pb.Deployment, len(deploymentsWithRelated))
	for i, deployment := range deploymentsWithRelated {
		deployments[i] = &pb.Deployment{
			Id:            deployment.ExternalId,
			Name:          deployment.Name,
			Active:        deployment.Active,
			StubId:        deployment.Stub.ExternalId,
			StubName:      deployment.Stub.Name,
			StubType:      string(deployment.Stub.Type),
			Version:       uint32(deployment.Version),
			WorkspaceId:   deployment.Workspace.ExternalId,
			WorkspaceName: deployment.Workspace.Name,
			CreatedAt:     timestamppb.New(deployment.CreatedAt),
			UpdatedAt:     timestamppb.New(deployment.UpdatedAt),
		}
	}

	return &pb.ListDeploymentsResponse{
		Ok:          true,
		Deployments: deployments,
	}, nil
}

func (gws *GatewayService) StopDeployment(ctx context.Context, in *pb.StopDeploymentRequest) (*pb.StopDeploymentResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	// Get deployment
	deploymentWithRelated, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.Id)
	if err != nil {
		return &pb.StopDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to get deployment",
		}, nil
	}

	// Stop active containers
	containers, err := gws.containerRepo.GetActiveContainersByStubId(deploymentWithRelated.Stub.ExternalId)
	if err == nil {
		for _, container := range containers {
			gws.scheduler.Stop(container.ContainerId)
		}
	}

	// Disable deployment
	deploymentWithRelated.Active = false
	_, err = gws.backendRepo.UpdateDeployment(ctx, deploymentWithRelated.Deployment)
	if err != nil {
		return &pb.StopDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to update deployment",
		}, nil
	}

	// Publish reload instance event
	eventBus := common.NewEventBus(gws.redisClient)
	eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
		"stub_id":   deploymentWithRelated.Stub.ExternalId,
		"stub_type": deploymentWithRelated.StubType,
	}})

	return &pb.StopDeploymentResponse{
		Ok: true,
	}, nil
}
