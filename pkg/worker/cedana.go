package worker

import (
	"context"
	"time"

	api "github.com/cedana/cedana/api/services/task"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	Address                    = "0.0.0.0:8080"
	defaultStartDeadline       = 10 * time.Second
	defaultCheckpointDeadline  = 2 * time.Minute
	defaultRestoreDeadline     = 2 * time.Minute
	defaultHealthCheckDeadline = 10 * time.Second
	CedanaPath                 = "/usr/bin/cedana"
)

type CedanaClient struct {
	conn    *grpc.ClientConn
	service api.TaskServiceClient
}

func NewCedanaClient(ctx context.Context) (*CedanaClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	taskConn, err := grpc.NewClient(Address, opts...)
	if err != nil {
		return nil, err
	}

	taskClient := api.NewTaskServiceClient(taskConn)

	client := &CedanaClient{
		service: taskClient,
		conn:    taskConn,
	}

	return client, err
}

func (c *CedanaClient) Close() error {
	return c.conn.Close()
}

func (c *CedanaClient) Start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, defaultStartDeadline)
	defer cancel()

	args := &api.StartArgs{}
	_, err := c.service.Start(ctx, args)
	if err != nil {
		return err
	}
	return nil
}

func (c *CedanaClient) Checkpoint(ctx context.Context, containerId string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultCheckpointDeadline)
	defer cancel()

	args := api.DumpArgs{
		Type:           api.CRType_LOCAL,
		JID:            containerId,
		TcpEstablished: true,
		// Dump dir taken from config
	}
	_, err := c.service.Dump(ctx, &args)
	// TODO gather metrics from response
	if err != nil {
		return err
	}
	return nil
}

func (c *CedanaClient) Restore(ctx context.Context, containerId string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultCheckpointDeadline)
	defer cancel()

	args := &api.RestoreArgs{
		Type:           api.CRType_LOCAL,
		JID:            containerId,
		TcpEstablished: true,
	}
	_, err := c.service.Restore(ctx, args)
	// TODO gather metrics from response
	if err != nil {
		return err
	}
	return nil
}

func (c *CedanaClient) DetailedHealthCheck(ctx context.Context) (*api.DetailedHealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultHealthCheckDeadline)
	defer cancel()

	resp, err := c.service.DetailedHealthCheck(ctx, nil)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
