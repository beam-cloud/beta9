package gatewayservices

import (
	"context"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/internal/auth"
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
			StubType:      deployment.Stub.Type,
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
