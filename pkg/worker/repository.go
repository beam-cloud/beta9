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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
		grpc.WithUnaryInterceptor(retryInterceptor(defaultGRPCMaxRetries, defaultGRPCRetryDelay)),
	}

	if token != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(common.ClientAuthInterceptor(token)))
	}

	return grpc.Dial(host, opts...)
}

// retryInterceptor retries the call on some gRPC errors
func retryInterceptor(maxRetries int, delay time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		for i := 0; i < maxRetries; i++ {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}

			st, ok := status.FromError(err)
			if !ok || (st.Code() != codes.Unavailable && st.Code() != codes.ResourceExhausted) {
				return err
			}

			time.Sleep(delay)
		}

		return errors.New("max retries reached")
	}
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
