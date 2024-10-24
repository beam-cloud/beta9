package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	api "github.com/cedana/cedana/pkg/api/services/task"
	types "github.com/cedana/cedana/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultCedanaPort          = 8080
	host                       = "0.0.0.0"
	binPath                    = "/usr/bin/cedana"
	sharedLibPath              = "/usr/local/lib/libcedana-gpu.so"
	runcRoot                   = "/run/runc"
	logLevel                   = "debug"
	checkpointPathBase         = "/data"
	defaultManageDeadline      = 10 * time.Second
	defaultCheckpointDeadline  = 2 * time.Minute
	defaultRestoreDeadline     = 2 * time.Minute
	defaultHealthCheckDeadline = 30 * time.Second
)

type CedanaClient struct {
	conn    *grpc.ClientConn
	service api.TaskServiceClient
	daemon  *exec.Cmd
}

func NewCedanaClient(ctx context.Context, config types.Config, gpuEnabled bool) (*CedanaClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	port, err := getRandomFreePort()
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	taskConn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	taskClient := api.NewTaskServiceClient(taskConn)

	// Launch the daemon
	daemon := exec.CommandContext(ctx, binPath, "daemon", "start",
		fmt.Sprintf("--port=%d", port),
		fmt.Sprintf("--gpu-enabled=%t", gpuEnabled))

	daemon.Stdout = os.Stdout
	daemon.Stderr = os.Stderr

	// XXX: Set config using env until config JSON parsing is fixed
	daemon.Env = append(os.Environ(), fmt.Sprintf("CEDANA_LOG_LEVEL=%s", logLevel))
	daemon.Env = append(daemon.Env, fmt.Sprintf("CEDANA_CLIENT_LEAVE_RUNNING=%t", config.Client.LeaveRunning))
	daemon.Env = append(daemon.Env, fmt.Sprintf("CEDANA_DUMP_STORAGE_DIR=%s", config.SharedStorage.DumpStorageDir))
	daemon.Env = append(daemon.Env, fmt.Sprintf("CEDANA_URL=%s", config.Connection.CedanaUrl))
	daemon.Env = append(daemon.Env, fmt.Sprintf("CEDANA_AUTH_TOKEN=%s", config.Connection.CedanaAuthToken))
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
	_, err = client.DetailedHealthCheckWait(ctx)
	// if err != nil || len(details.UnhealthyReasons) > 0 {
	// 	defer daemon.Process.Kill()
	// 	defer taskConn.Close()
	// 	if err != nil {
	// 		return nil, fmt.Errorf("cedana health check failed: %v", err)
	// 	}
	// 	if len(details.UnhealthyReasons) > 0 {
	// 		return nil, fmt.Errorf("cedana health failed with reasons: %v", details.UnhealthyReasons)
	// 	}
	// }

	return client, nil
}

func (c *CedanaClient) Close() {
	c.conn.Close()
	c.daemon.Process.Kill()
}

// Updates the runc container spec to make the shared library available
// as well as the shared memory that is used for communication
func (c *CedanaClient) prepareContainerSpec(spec *specs.Spec, gpuEnabled bool) error {
	if !gpuEnabled {
		return nil // no need to do anything
	}

	// First check if shared library is on worker
	if _, err := os.Stat(sharedLibPath); os.IsNotExist(err) {
		return fmt.Errorf("%s not found on worker. Was the daemon started with GPU enabled?", sharedLibPath)
	}

	// Remove nvidia prestart hook as we don't need actual device mounts
	spec.Hooks.Prestart = nil

	// Add shared memory mount from worker instead, remove existing /dev/shm mount
	for i, m := range spec.Mounts {
		if m.Destination == "/dev/shm" {
			spec.Mounts = append(spec.Mounts[:i], spec.Mounts[i+1:]...)
			break
		}
	}
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: "/dev/shm",
		Source:      "/dev/shm",
		Type:        "bind",
		Options: []string{
			"rbind",
			"rprivate",
			"nosuid",
			"nodev",
			"rw",
		},
	})

	// Add the shared library to the container
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: sharedLibPath,
		Source:      sharedLibPath,
		Type:        "bind",
		Options: []string{
			"rbind",
			"rprivate",
			"nosuid",
			"nodev",
			"rw",
		},
	})

	// XXX: Remove /usr/lib/worker/x86_64-linux-gnu from mounts
	for i, m := range spec.Mounts {
		if m.Destination == "/usr/lib/worker/x86_64-linux-gnu" {
			spec.Mounts = append(spec.Mounts[:i], spec.Mounts[i+1:]...)
			break
		}
	}

	spec.Process.Env = append(spec.Process.Env, "LD_PRELOAD="+sharedLibPath)

	return nil
}

// Start managing a runc container
func (c *CedanaClient) Manage(ctx context.Context, containerId string, gpuEnabled bool) error {
	ctx, cancel := context.WithTimeout(ctx, defaultManageDeadline)
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

	external := []string{} // Add any external mounts that cause CRIU failures here

	args := api.JobDumpArgs{
		Type: api.CRType_LOCAL,
		JID:  containerId,
		CriuOpts: &api.CriuOpts{
			TcpEstablished: true,
			LeaveRunning:   true,
			External:       external,
		},
		Dir: fmt.Sprintf("%s/%s", checkpointPathBase, containerId),
	}
	res, err := c.service.JobDump(ctx, &args)
	if err != nil {
		return err
	}
	_ = res.DumpStats
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
	if err != nil {
		return err
	}
	_ = res.RestoreStats
	return nil
}

// Perform a detailed health check of cedana C/R capabilities
func (c *CedanaClient) DetailedHealthCheckWait(ctx context.Context) (*api.DetailedHealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultHealthCheckDeadline)
	defer cancel()

	opts := []grpc.CallOption{}
	opts = append(opts, grpc.WaitForReady(true))

	res, err := c.service.DetailedHealthCheck(ctx, &api.DetailedHealthCheckRequest{}, opts...)
	if err != nil {
		return nil, err
	}

	return res, nil
}
