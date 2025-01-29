package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	common "github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// NewWorkerRepositoryClient creates a new worker repository client
func NewWorkerRepositoryClient(ctx context.Context, config types.AppConfig, token string) (pb.WorkerRepositoryServiceClient, error) {
	host := fmt.Sprintf("%s:%d", config.GatewayService.GRPC.ExternalHost, config.GatewayService.GRPC.ExternalPort)
	conn, err := newGRPCConn(host, token)
	if err != nil {
		return nil, err
	}

	return pb.NewWorkerRepositoryServiceClient(conn), nil
}

// NewContainerRepositoryClient creates a new container repository client
func NewContainerRepositoryClient(ctx context.Context, config types.AppConfig, token string) (pb.ContainerRepositoryServiceClient, error) {
	host := fmt.Sprintf("%s:%d", config.GatewayService.GRPC.ExternalHost, config.GatewayService.GRPC.ExternalPort)
	conn, err := newGRPCConn(host, token)
	if err != nil {
		return nil, err
	}

	return pb.NewContainerRepositoryServiceClient(conn), nil
}

// newGRPCConn creates a new gRPC connection (with or without TLS/Auth) to the provided host
func newGRPCConn(host string, token string) (*grpc.ClientConn, error) {
	grpcOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	isTLS := strings.HasSuffix(host, "443")
	if isTLS {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		grpcOption = grpc.WithTransportCredentials(h2creds)
	}

	var dialOpts = []grpc.DialOption{grpcOption}

	if token != "" {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(common.ClientAuthInterceptor(token)))
	}

	return grpc.NewClient(host, dialOpts...)
}
