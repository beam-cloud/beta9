package common

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type GatewayClient struct {
	conn *grpc.ClientConn
	pb.GatewayServiceClient
}

type authCredentials struct {
	token string
}

func (a *authCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": "Bearer " + a.token,
	}, nil
}

func (a *authCredentials) RequireTransportSecurity() bool {
	return false
}

func NewGatewayClient(config types.GatewayServiceConfig) (*GatewayClient, error) {
	address := fmt.Sprintf("%s:%d", config.Host, config.GRPC.Port)

	creds := insecure.NewCredentials()
	if strings.HasSuffix(address, ":443") {
		creds = credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
	}

	authCreds := &authCredentials{token: config.Token}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(authCreds),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(config.GRPC.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(config.GRPC.MaxSendMsgSize),
		),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gateway service: %v", err)
	}

	client := pb.NewGatewayServiceClient(conn)

	return &GatewayClient{conn, client}, nil
}

func (gc *GatewayClient) Close() {
	gc.conn.Close()
}
