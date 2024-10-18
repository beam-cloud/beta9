package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	api "github.com/cedana/cedana/pkg/api/services/task"
	types "github.com/cedana/cedana/pkg/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultCedanaPort          = 8080
	host                       = "0.0.0.0"
	binPath                    = "/usr/bin/cedana"
	runcRoot                   = "/run/runc"
	logLevel                   = "debug"
	defaultStartDeadline       = 10 * time.Second
	defaultCheckpointDeadline  = 2 * time.Minute
	defaultRestoreDeadline     = 2 * time.Minute
	defaultHealthCheckDeadline = 10 * time.Second
)

type CedanaClient struct {
	conn    *grpc.ClientConn
	service api.TaskServiceClient
	daemon  *exec.Cmd
}

func NewCedanaClient(ctx context.Context, config types.Config, port int, gpuEnabled bool) (*CedanaClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	addr := fmt.Sprintf("%s:%d", host, port)
	taskConn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	taskClient := api.NewTaskServiceClient(taskConn)

	// Launch the daemon
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cedana config: %v", err)
	}
	log.Printf("launching cedana daemon with config JSON: %s", configJSON)
	daemon := exec.CommandContext(ctx, binPath, "daemon", "start",
		// FIXDME: configjson invalid parsing error
		fmt.Sprintf("--config='%s'", configJSON),
		fmt.Sprintf("--port=%d", port),
		fmt.Sprintf("--gpu-enabled=%t", gpuEnabled))
	daemon.Stdout = os.Stdout
	daemon.Stderr = os.Stderr
	daemon.Env = append(daemon.Env, fmt.Sprintf("CEDANA_LOG_LEVEL=%s", logLevel))
	err = daemon.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start cedana daemon: %v", err)
	}

	// Cleanup the daemon on exit
	go func() {
		daemon.Wait()
		taskConn.Close()
	}()

	client := &CedanaClient{
		service: taskClient,
		conn:    taskConn,
		daemon:  daemon,
	}

	// Wait for the daemon to be ready, and do health check
	details, err := client.DetailedHealthCheckWait(context.Background())
	if err != nil || (details != nil && len(details.UnhealthyReasons) > 0) {
		defer daemon.Process.Kill()
		defer taskConn.Close()
		if err == nil && len(details.UnhealthyReasons) > 0 {
			return nil, fmt.Errorf("cedana health check failed: %+v", details.UnhealthyReasons)
		} else {
			return nil, fmt.Errorf("cedana health check failed: %v", err)
		}
	}

	return client, nil
}

// Start managing a runc container
func (c *CedanaClient) Manage(ctx context.Context, containerId string, gpuEnabled bool) error {
	ctx, cancel := context.WithTimeout(ctx, defaultStartDeadline)
	defer cancel()

	args := &api.RuncManageArgs{
		ContainerID: containerId,
		GPU:         gpuEnabled,
		Root:        runcRoot,
	}
	_, err := c.service.RuncManage(ctx, args)
	if err != nil {
		return err
	}
	return nil
}

// Checkpoint a runc container
func (c *CedanaClient) Checkpoint(ctx context.Context, containerId string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultCheckpointDeadline)
	defer cancel()

	args := api.JobDumpArgs{
		Type:     api.CRType_LOCAL,
		JID:      containerId,
		CriuOpts: &api.CriuOpts{TcpEstablished: true, LeaveRunning: true},
		// Dump dir taken from config
	}
	res, err := c.service.JobDump(ctx, &args)
	_ = res.DumpStats
	if err != nil {
		return err
	}
	return nil
}

// Restore a runc container
func (c *CedanaClient) Restore(ctx context.Context, containerId string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultCheckpointDeadline)
	defer cancel()

	args := &api.JobRestoreArgs{
		Type:     api.CRType_LOCAL,
		JID:      containerId,
		CriuOpts: &api.CriuOpts{TcpEstablished: true},
	}
	res, err := c.service.JobRestore(ctx, args)
	_ = res.RestoreStats
	if err != nil {
		return err
	}
	return nil
}

// Perform a detailed health check of cedana C/R capabilities
func (c *CedanaClient) DetailedHealthCheckWait(ctx context.Context) (*api.DetailedHealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultHealthCheckDeadline)
	defer cancel()

	opts := []grpc.CallOption{}
	opts = append(opts, grpc.WaitForReady(true))

	resp, err := c.service.DetailedHealthCheck(ctx, &api.DetailedHealthCheckRequest{}, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
