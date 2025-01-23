package worker

import (
	"context"
	"crypto/tls"
	"net"
	"strings"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type RepositoryClient struct {
	ServiceUrl   string
	ServiceToken string
	conn         *grpc.ClientConn
	client       pb.RepositoryServiceClient
	existingConn net.Conn
}

func NewRepositoryClient(serviceUrl, serviceToken string, existingConn net.Conn) (pb.RepositoryServiceClient, error) {
	grpcOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	isTLS := strings.HasSuffix(serviceUrl, "443")
	if isTLS {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		grpcOption = grpc.WithTransportCredentials(h2creds)
	}

	var dialOpts = []grpc.DialOption{grpcOption}

	// Use existingConn if provided
	if existingConn != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return existingConn, nil
		}))
	}

	maxMessageSize := 1 << 30 // 1Gi
	if serviceToken != "" {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(AuthInterceptor(serviceToken)),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMessageSize),
				grpc.MaxCallSendMsgSize(maxMessageSize),
			))
	}

	conn, err := grpc.Dial(serviceUrl, dialOpts...)
	if err != nil {
		return nil, err
	}

	return pb.NewRepositoryServiceClient(conn), nil
}

func AuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}
