package gatewayservices

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/auth"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) ExportWorkspaceConfig(ctx context.Context, in *pb.ExportWorkspaceConfigRequest) (*pb.ExportWorkspaceConfigResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	return &pb.ExportWorkspaceConfigResponse{
		GatewayHttpHost: gws.appConfig.GatewayService.HTTP.ExternalHost,
		GatewayHttpPort: int32(gws.appConfig.GatewayService.HTTP.ExternalPort),
		GatewayHttpTls:  gws.appConfig.GatewayService.HTTP.TLS,
		GatewayGrpcHost: gws.appConfig.GatewayService.GRPC.ExternalHost,
		GatewayGrpcPort: int32(gws.appConfig.GatewayService.GRPC.ExternalPort),
		GatewayGrpcTls:  gws.appConfig.GatewayService.GRPC.TLS,
		WorkspaceId:     authInfo.Workspace.ExternalId,
	}, nil
}
