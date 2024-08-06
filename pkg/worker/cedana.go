package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	api "github.com/cedana/cedana/api/services/task"
	cedana "github.com/cedana/cedana/types"
	"github.com/opencontainers/runtime-spec/specs-go"

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

func AddCedanaDaemonHook(request *types.ContainerRequest, hook *[]specs.Hook, config *cedana.Config) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return err
	}

	args := []string{}
	if request.Gpu == "" {
		args = []string{"sh", "-c", fmt.Sprintf("nohup %s %s %s &", CedanaPath, "daemon start --config", string(configJSON))}
	} else {
		args = []string{"sh", "-c", fmt.Sprintf("nohup %s %s %s &", CedanaPath, "daemon start -g --config", string(configJSON))}
	}
	// TODO: Detect CUDA version and pass it to --cuda flag
	// TODO: Redirect logs to track
	*hook = append(*hook, specs.Hook{
		Path: "/bin/sh",
		Args: args,
	})

	return nil
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
		Type: api.CRType_LOCAL,
		JID:  containerId,
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
		Type: api.CRType_LOCAL,
		JID:  containerId,
	}
	_, err := c.service.Restore(ctx, args)
	// TODO gather metrics from response
	if err != nil {
		return err
	}
	return nil
}

func (c *CedanaClient) HealthCheck(ctx context.Context, containerId string) bool {
	ctx, cancel := context.WithTimeout(ctx, defaultHealthCheckDeadline)
	defer cancel()

	_, err := c.service.DetailedHealthCheck(ctx, nil)
	// TODO: print details?
	if err != nil {
		return false
	}

	return true
}
