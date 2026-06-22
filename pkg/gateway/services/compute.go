package gatewayservices

import (
	"context"

	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) ListPrivatePools(ctx context.Context, in *pb.ListPrivatePoolsRequest) (*pb.ListPrivatePoolsResponse, error) {
	return gws.computeService.ListPrivatePools(ctx, in)
}

func (gws *GatewayService) CreateBYOCAWSPoolOnboarding(ctx context.Context, in *pb.CreateBYOCAWSPoolOnboardingRequest) (*pb.CreateBYOCAWSPoolOnboardingResponse, error) {
	return gws.computeService.CreateBYOCAWSPoolOnboarding(ctx, in)
}

func (gws *GatewayService) GetBYOCPoolOnboardingStatus(ctx context.Context, in *pb.GetBYOCPoolOnboardingStatusRequest) (*pb.GetBYOCPoolOnboardingStatusResponse, error) {
	return gws.computeService.GetBYOCPoolOnboardingStatus(ctx, in)
}

func (gws *GatewayService) GetBYOCAWSStack(ctx context.Context, in *pb.GetBYOCAWSStackRequest) (*pb.GetBYOCAWSStackResponse, error) {
	return gws.computeService.GetBYOCAWSStack(ctx, in)
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

func (gws *GatewayService) StreamAgent(in *pb.StreamAgentRequest, stream pb.GatewayService_StreamAgentServer) error {
	return gws.computeService.StreamAgent(in, stream)
}

func (gws *GatewayService) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest) (*pb.UpdateAgentRouteStatusResponse, error) {
	return gws.computeService.UpdateAgentRouteStatus(ctx, in)
}

func (gws *GatewayService) StreamAgentTelemetry(stream pb.GatewayService_StreamAgentTelemetryServer) error {
	return gws.computeService.StreamAgentTelemetry(stream)
}
