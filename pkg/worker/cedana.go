package worker

import (
	"context"
	"crypto/tls"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type CedanaClient struct {
	ServiceUrl   string
	ServiceToken string
	conn         *grpc.ClientConn
	// client       pb.CedanaClient
}

func NewCedanaClient(serviceUrl, serviceToken string) (*CedanaClient, error) {
	client := &CedanaClient{
		ServiceUrl:   serviceUrl,
		ServiceToken: serviceToken,
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *CedanaClient) connect() error {
	grpcOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	isTLS := strings.HasSuffix(c.ServiceUrl, "443")
	if isTLS {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		grpcOption = grpc.WithTransportCredentials(h2creds)
	}

	var dialOpts = []grpc.DialOption{grpcOption}

	maxMessageSize := 1024 * 1024 * 1024 // 1Gi
	if c.ServiceToken != "" {
		dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(AuthInterceptor(c.ServiceToken)),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMessageSize),
				grpc.MaxCallSendMsgSize(maxMessageSize),
			))
	}

	conn, err := grpc.Dial(c.ServiceUrl, dialOpts...)
	if err != nil {
		return err
	}

	c.conn = conn
	// c.client = pb.NewCedanaClient(conn)
	return nil
}

func (c *CedanaClient) Close() error {
	return c.conn.Close()
}

func AuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}

func (c *CedanaClient) Checkpoint(containerId string) error {
	return nil
}

// func (c *CedanaClient) Available(containerId string) (*pb.CedanaAvailableResponse, error) {
// 	return resp, nil
// }
