package gatewayservices

import (
	"context"

	pb "github.com/beam-cloud/beta9/proto"
)

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

func (gws *GatewayService) CreateMarketplaceListing(ctx context.Context, in *pb.CreateMarketplaceListingRequest) (*pb.CreateMarketplaceListingResponse, error) {
	return gws.computeService.CreateMarketplaceListing(ctx, in)
}

func (gws *GatewayService) UpdateMarketplaceListing(ctx context.Context, in *pb.UpdateMarketplaceListingRequest) (*pb.UpdateMarketplaceListingResponse, error) {
	return gws.computeService.UpdateMarketplaceListing(ctx, in)
}

func (gws *GatewayService) DeleteMarketplaceListing(ctx context.Context, in *pb.DeleteMarketplaceListingRequest) (*pb.DeleteMarketplaceListingResponse, error) {
	return gws.computeService.DeleteMarketplaceListing(ctx, in)
}

func (gws *GatewayService) ListMarketplaceListings(ctx context.Context, in *pb.ListMarketplaceListingsRequest) (*pb.ListMarketplaceListingsResponse, error) {
	return gws.computeService.ListMarketplaceListings(ctx, in)
}

func (gws *GatewayService) GetMarketplaceJoinCommand(ctx context.Context, in *pb.GetMarketplaceJoinCommandRequest) (*pb.GetMarketplaceJoinCommandResponse, error) {
	return gws.computeService.GetMarketplaceJoinCommand(ctx, in)
}

func (gws *GatewayService) ListMarketplaceOffers(ctx context.Context, in *pb.ListMarketplaceOffersRequest) (*pb.ListMarketplaceOffersResponse, error) {
	return gws.computeService.ListMarketplaceOffers(ctx, in)
}

func (gws *GatewayService) GetMarketplaceOffer(ctx context.Context, in *pb.GetMarketplaceOfferRequest) (*pb.GetMarketplaceOfferResponse, error) {
	return gws.computeService.GetMarketplaceOffer(ctx, in)
}

func (gws *GatewayService) ListMarketplaceMachines(ctx context.Context, in *pb.ListMarketplaceMachinesRequest) (*pb.ListMarketplaceMachinesResponse, error) {
	return gws.computeService.ListMarketplaceMachines(ctx, in)
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

func (gws *GatewayService) UpdateAgentAvailability(ctx context.Context, in *pb.UpdateAgentAvailabilityRequest) (*pb.UpdateAgentAvailabilityResponse, error) {
	return gws.computeService.UpdateAgentAvailability(ctx, in)
}

func (gws *GatewayService) StreamAgentTelemetry(stream pb.GatewayService_StreamAgentTelemetryServer) error {
	return gws.computeService.StreamAgentTelemetry(stream)
}
