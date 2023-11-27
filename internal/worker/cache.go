package worker

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	pb "github.com/beam-cloud/beam/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type CacheClient struct {
	ServiceUrl   string
	ServiceToken string
	conn         *grpc.ClientConn
	client       pb.CacheClient
}

const getContentRequestTimeout = 5 * time.Second
const storeContentRequestTimeout = 60 * time.Second

func NewCacheClient(serviceUrl, serviceToken string) (*CacheClient, error) {
	client := &CacheClient{
		ServiceUrl:   serviceUrl,
		ServiceToken: serviceToken,
	}

	err := client.connect()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *CacheClient) connect() error {
	grpcOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	isTLS := strings.HasSuffix(c.ServiceUrl, "443")
	if isTLS {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		grpcOption = grpc.WithTransportCredentials(h2creds)
	}

	var dialOpts = []grpc.DialOption{grpcOption}

	maxMessageSize := 1 << 30 // 1Gi
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
	c.client = pb.NewCacheClient(conn)
	return nil
}

func (c *CacheClient) GetContent(hash string, offset int64, length int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getContentRequestTimeout)
	defer cancel()

	getContentResponse, err := c.client.GetContent(ctx, &pb.GetContentRequest{Hash: hash, Offset: offset, Length: length})
	if err != nil {
		return nil, err
	}
	return getContentResponse.Content, nil
}

func (c *CacheClient) StoreContent(chunks chan []byte) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), storeContentRequestTimeout)
	defer cancel()

	stream, err := c.client.StoreContent(ctx)
	if err != nil {
		return "", err
	}

	for chunk := range chunks {
		req := &pb.StoreContentRequest{Content: chunk}
		if err := stream.Send(req); err != nil {
			return "", err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}

	return resp.Hash, nil
}

func (c *CacheClient) Close() error {
	return c.conn.Close()
}

func AuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}
