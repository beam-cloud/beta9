package worker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultGRPCMaxRetries = 3
	defaultGRPCRetryDelay = time.Second
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
	creds := insecure.NewCredentials()
	if strings.HasSuffix(host, "443") {
		creds = credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithUnaryInterceptor(common.GRPCClientRetryInterceptor(defaultGRPCMaxRetries, defaultGRPCRetryDelay)),
	}

	if token != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(common.GRPCClientAuthInterceptor(token)))
		opts = append(opts, grpc.WithStreamInterceptor(common.GRPCClientAuthStreamInterceptor(token)))
	}

	return grpc.Dial(host, opts...)
}

// handleGRPCResponse handles a repository gRPC response and returns the response & an error if the response is not Ok
func handleGRPCResponse[T interface {
	GetOk() bool
	GetErrorMsg() string
}](response T, err error) (T, error) {
	if err != nil {
		return response, err
	}

	if !response.GetOk() {
		return response, errors.New(response.GetErrorMsg())
	}

	return response, nil
}
