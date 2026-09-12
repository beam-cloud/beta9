package gatewayservices

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const thunderServiceUnavailableError = "Thunder service is unavailable"

func (gws *GatewayService) ListPrivatePools(ctx context.Context, in *pb.ListPrivatePoolsRequest) (*pb.ListPrivatePoolsResponse, error) {
	return gws.computeService.ListPrivatePools(ctx, in)
}

func (gws *GatewayService) CreateBYOCPool(ctx context.Context, in *pb.CreateBYOCPoolRequest) (*pb.CreateBYOCPoolResponse, error) {
	return gws.computeService.CreateBYOCPool(ctx, in)
}

func (gws *GatewayService) GetBYOCPool(ctx context.Context, in *pb.GetBYOCPoolRequest) (*pb.GetBYOCPoolResponse, error) {
	return gws.computeService.GetBYOCPool(ctx, in)
}

func (gws *GatewayService) ScaleBYOCPool(ctx context.Context, in *pb.ScaleBYOCPoolRequest) (*pb.ScaleBYOCPoolResponse, error) {
	return gws.computeService.ScaleBYOCPool(ctx, in)
}

func (gws *GatewayService) GetProviderJoinCommand(ctx context.Context, in *pb.GetProviderJoinCommandRequest) (*pb.GetProviderJoinCommandResponse, error) {
	return gws.computeService.GetProviderJoinCommand(ctx, in)
}

func (gws *GatewayService) ListProviderMachines(ctx context.Context, in *pb.ListProviderMachinesRequest) (*pb.ListProviderMachinesResponse, error) {
	return gws.computeService.ListProviderMachines(ctx, in)
}

func (gws *GatewayService) GetEndpointUsage(ctx context.Context, in *pb.GetEndpointUsageRequest) (*pb.GetEndpointUsageResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo == nil || authInfo.Workspace == nil {
		return &pb.GetEndpointUsageResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	if gws.endpointRepo == nil {
		return &pb.GetEndpointUsageResponse{Ok: false, ErrMsg: "managed endpoints are not enabled"}, nil
	}
	kind := types.UsageSpend
	if in.GetKind() == string(types.UsageEarned) {
		kind = types.UsageEarned
	}
	from, to, err := usageWindow(in)
	if err != nil {
		return &pb.GetEndpointUsageResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	report, err := gws.endpointRepo.GetUsage(ctx, kind, authInfo.Workspace.ExternalId, from, to)
	if err != nil {
		return &pb.GetEndpointUsageResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	toProto := func(m map[string]types.Usage) map[string]*pb.EndpointUsage {
		out := make(map[string]*pb.EndpointUsage, len(m))
		for k, v := range m {
			out[k] = usageToProto(v)
		}
		return out
	}
	return &pb.GetEndpointUsageResponse{Ok: true, Total: usageToProto(report.Total), PerModel: toProto(report.PerModel), PerDay: toProto(report.PerDay)}, nil
}

// usageWindow parses the request's inclusive UTC day range; end_date
// defaults to today.
func usageWindow(in *pb.GetEndpointUsageRequest) (from, to time.Time, err error) {
	if from, err = time.Parse(time.DateOnly, in.GetStartDate()); err != nil {
		return from, to, fmt.Errorf("invalid start_date: %w", err)
	}
	to = time.Now().UTC()
	if in.GetEndDate() != "" {
		if to, err = time.Parse(time.DateOnly, in.GetEndDate()); err != nil {
			return from, to, fmt.Errorf("invalid end_date: %w", err)
		}
	}
	return from, to, nil
}

func usageToProto(u types.Usage) *pb.EndpointUsage {
	return &pb.EndpointUsage{
		Requests: u.Requests, PromptTokens: u.PromptTokens, CompletionTokens: u.CompletionTokens,
		CachedTokens: u.CachedTokens, MicroUsd: u.MicroUSD,
		PromptMicroUsd: u.PromptMicroUSD, CompletionMicroUsd: u.CompletionMicroUSD,
		CachedMicroUsd: u.CachedMicroUSD, RequestMicroUsd: u.RequestMicroUSD,
	}
}

func (gws *GatewayService) ListMachineContainers(ctx context.Context, in *pb.ListMachineContainersRequest) (*pb.ListMachineContainersResponse, error) {
	return gws.computeService.ListMachineContainers(ctx, in)
}

func (gws *GatewayService) CreatePool(ctx context.Context, in *pb.CreatePoolRequest) (*pb.CreatePoolResponse, error) {
	return gws.computeService.CreatePool(ctx, in)
}

func (gws *GatewayService) DeletePool(ctx context.Context, in *pb.DeletePoolRequest) (*pb.DeletePoolResponse, error) {
	return gws.computeService.DeletePool(ctx, in)
}

func (gws *GatewayService) ExtendPoolCapacity(ctx context.Context, in *pb.ExtendPoolCapacityRequest) (*pb.ExtendPoolCapacityResponse, error) {
	return gws.computeService.ExtendPoolCapacity(ctx, in)
}

func (gws *GatewayService) ListPoolMachines(ctx context.Context, in *pb.ListPoolMachinesRequest) (*pb.ListPoolMachinesResponse, error) {
	return gws.computeService.ListPoolMachines(ctx, in)
}

func (gws *GatewayService) DownloadMachineSSHKey(ctx context.Context, in *pb.DownloadMachineSSHKeyRequest) (*pb.DownloadMachineSSHKeyResponse, error) {
	return gws.computeService.DownloadMachineSSHKey(ctx, in)
}

func (gws *GatewayService) RotateMachineSSHKey(ctx context.Context, in *pb.RotateMachineSSHKeyRequest) (*pb.RotateMachineSSHKeyResponse, error) {
	return gws.computeService.RotateMachineSSHKey(ctx, in)
}

func (gws *GatewayService) ActivateMachineSSHKey(ctx context.Context, in *pb.ActivateMachineSSHKeyRequest) (*pb.ActivateMachineSSHKeyResponse, error) {
	return gws.computeService.ActivateMachineSSHKey(ctx, in)
}

func (gws *GatewayService) ListPoolOffers(ctx context.Context, in *pb.ListPoolOffersRequest) (*pb.ListPoolOffersResponse, error) {
	return gws.computeService.ListPoolOffers(ctx, in)
}

func (gws *GatewayService) LaunchPoolCapacity(ctx context.Context, in *pb.LaunchPoolCapacityRequest) (*pb.LaunchPoolCapacityResponse, error) {
	return gws.computeService.LaunchPoolCapacity(ctx, in)
}

func (gws *GatewayService) CreatePoolJoinToken(ctx context.Context, in *pb.CreatePoolJoinTokenRequest) (*pb.CreatePoolJoinTokenResponse, error) {
	return gws.computeService.CreatePoolJoinToken(ctx, in)
}

func (gws *GatewayService) RevokePoolJoinToken(ctx context.Context, in *pb.RevokePoolJoinTokenRequest) (*pb.RevokePoolJoinTokenResponse, error) {
	return gws.computeService.RevokePoolJoinToken(ctx, in)
}

func (gws *GatewayService) GetPoolJoinCommand(ctx context.Context, in *pb.GetPoolJoinCommandRequest) (*pb.GetPoolJoinCommandResponse, error) {
	return gws.computeService.GetPoolJoinCommand(ctx, in)
}

func (gws *GatewayService) JoinAgent(ctx context.Context, in *pb.JoinAgentRequest) (*pb.JoinAgentResponse, error) {
	return gws.computeService.JoinAgent(ctx, in)
}

func (gws *GatewayService) RequestAgentTransportCredential(ctx context.Context, in *pb.RequestAgentTransportCredentialRequest) (*pb.RequestAgentTransportCredentialResponse, error) {
	return gws.computeService.RequestAgentTransportCredential(ctx, in)
}

func (gws *GatewayService) GetAgentPoolVirtualization(ctx context.Context, in *pb.GetAgentPoolVirtualizationRequest) (*pb.GetAgentPoolVirtualizationResponse, error) {
	return gws.computeService.GetAgentPoolVirtualization(ctx, in)
}

func (gws *GatewayService) CreateNodeEnrollment(ctx context.Context, in *pb.CreateNodeEnrollmentRequest) (*pb.CreateNodeEnrollmentResponse, error) {
	if gws.thunderService == nil {
		return &pb.CreateNodeEnrollmentResponse{ErrorMsg: thunderServiceUnavailableError}, nil
	}
	return gws.thunderService.CreateNodeEnrollment(ctx, in)
}

func (gws *GatewayService) DeleteNodeEnrollment(ctx context.Context, in *pb.DeleteNodeEnrollmentRequest) (*pb.DeleteNodeEnrollmentResponse, error) {
	if gws.thunderService == nil {
		return &pb.DeleteNodeEnrollmentResponse{ErrorMsg: thunderServiceUnavailableError}, nil
	}
	return gws.thunderService.DeleteNodeEnrollment(ctx, in)
}

func (gws *GatewayService) StreamAgent(in *pb.StreamAgentRequest, stream pb.GatewayService_StreamAgentServer) error {
	return gws.computeService.StreamAgent(in, stream)
}

func (gws *GatewayService) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest) (*pb.UpdateAgentRouteStatusResponse, error) {
	return gws.computeService.UpdateAgentRouteStatus(ctx, in)
}

func (gws *GatewayService) UpdateAgentAvailability(ctx context.Context, in *pb.UpdateAgentAvailabilityRequest) (*pb.UpdateAgentAvailabilityResponse, error) {
	return gws.computeService.UpdateAgentAvailability(ctx, in)
}

func (gws *GatewayService) UpdateAgentSSHStatus(ctx context.Context, in *pb.UpdateAgentSSHStatusRequest) (*pb.UpdateAgentSSHStatusResponse, error) {
	return gws.computeService.UpdateAgentSSHStatus(ctx, in)
}

func (gws *GatewayService) StreamAgentTelemetry(stream pb.GatewayService_StreamAgentTelemetryServer) error {
	return gws.computeService.StreamAgentTelemetry(stream)
}
