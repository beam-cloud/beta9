package gatewayservices

import (
	"context"

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

func (gws *GatewayService) GetProviderEarnings(ctx context.Context, in *pb.GetProviderEarningsRequest) (*pb.GetProviderEarningsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo == nil || authInfo.Workspace == nil {
		return &pb.GetProviderEarningsResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	if gws.endpointRepo == nil {
		return &pb.GetProviderEarningsResponse{Ok: false, ErrMsg: "provider earnings are unavailable"}, nil
	}
	days := int(in.GetDays())
	if days <= 0 {
		days = 30
	}
	report, err := gws.endpointRepo.GetProviderEarnings(ctx, authInfo.Workspace.ExternalId, days)
	if err != nil {
		return &pb.GetProviderEarningsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	toProto := func(m map[string]types.ProviderEarnings) map[string]*pb.ProviderEarnings {
		out := make(map[string]*pb.ProviderEarnings, len(m))
		for k, v := range m {
			out[k] = providerEarningsToProto(v)
		}
		return out
	}
	return &pb.GetProviderEarningsResponse{
		Ok: true, Total: providerEarningsToProto(report.Total),
		PerMachine: toProto(report.PerMachine), PerDay: toProto(report.PerDay),
	}, nil
}

func providerEarningsToProto(e types.ProviderEarnings) *pb.ProviderEarnings {
	return &pb.ProviderEarnings{
		Requests: e.Requests, PromptTokens: e.PromptTokens, CompletionTokens: e.CompletionTokens,
		Images: e.Images, EarningsMicroUsd: e.EarningsMicroUSD,
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
