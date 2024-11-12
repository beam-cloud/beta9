package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/go-runc"
	api "github.com/cedana/cedana/pkg/api/services/task"
	types "github.com/cedana/cedana/pkg/types"

	"github.com/opencontainers/runtime-spec/specs-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	cedanaHost                 = "0.0.0.0"
	cedanaBinPath              = "/usr/bin/cedana"
	cedanaSharedLibPath        = "/usr/local/lib/libcedana-gpu.so"
	runcRoot                   = "/run/runc"
	cedanaLogLevel             = "debug"
	checkpointPathBase         = "/tmp/checkpoints"
	defaultManageDeadline      = 10 * time.Second
	defaultCheckpointDeadline  = 10 * time.Minute
	defaultRestoreDeadline     = 5 * time.Minute
	defaultHealthCheckDeadline = 5 * time.Second
	cedanaUseRemoteDB          = true // Do not change, or migrations across workers will fail
)

type CedanaClient struct {
	conn    *grpc.ClientConn
	service api.TaskServiceClient
	daemon  *exec.Cmd
	config  types.Config
}

func NewCedanaClient(
	ctx context.Context,
	config types.Config,
	gpuEnabled bool,
) (*CedanaClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	port, err := getRandomFreePort()
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", cedanaHost, port)
	taskConn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	taskClient := api.NewTaskServiceClient(taskConn)

	// Launch the daemon
	daemon := exec.CommandContext(ctx, cedanaBinPath, "daemon", "start",
		fmt.Sprintf("--port=%d", port),
		fmt.Sprintf("--gpu-enabled=%t", gpuEnabled))

	daemon.Stdout = os.Stdout
	daemon.Stderr = os.Stderr

	// XXX: Set config using env until config JSON parsing is fixed
	daemon.Env = append(os.Environ(),
		fmt.Sprintf("CEDANA_LOG_LEVEL=%s", cedanaLogLevel),
		fmt.Sprintf("CEDANA_CLIENT_LEAVE_RUNNING=%t", config.Client.LeaveRunning),
		fmt.Sprintf("CEDANA_DUMP_STORAGE_DIR=%s", config.SharedStorage.DumpStorageDir),
		fmt.Sprintf("CEDANA_URL=%s", config.Connection.CedanaUrl),
		fmt.Sprintf("CEDANA_AUTH_TOKEN=%s", config.Connection.CedanaAuthToken),
		fmt.Sprintf("CEDANA_REMOTE=%t", cedanaUseRemoteDB),
	)

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
		config:  config,
	}

	// Wait for the daemon to be ready, and do health check
	details, err := client.DetailedHealthCheckWait(ctx)
	if err != nil || len(details.UnhealthyReasons) > 0 {
		defer daemon.Process.Kill()
		defer taskConn.Close()

		if err != nil {
			return nil, fmt.Errorf("cedana health check failed: %v", err)
		}

		if len(details.UnhealthyReasons) > 0 {
			return nil, fmt.Errorf(
				"cedana health failed with reasons: %v",
				details.UnhealthyReasons,
			)
		}
	}

	return client, nil
}

func (c *CedanaClient) Close() {
	c.conn.Close()
	c.daemon.Process.Kill()
}

// Updates the runc container spec to make the shared library available
// as well as the shared memory that is used for communication
func (c *CedanaClient) PrepareContainerSpec(spec *specs.Spec, gpuEnabled bool) error {
	if !gpuEnabled {
		return nil // no need to do anything
	}

	// First check if shared library is on worker
	if _, err := os.Stat(cedanaSharedLibPath); os.IsNotExist(err) {
		return fmt.Errorf(
			"%s not found on worker. Was the daemon started with GPU enabled?",
			cedanaSharedLibPath,
		)
	}

	// Remove nvidia prestart hook as we don't need actual device mounts
	spec.Hooks.Prestart = nil

	// TODO: will this causes issues on multi-gpu nodes...?

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
		Destination: cedanaSharedLibPath,
		Source:      cedanaSharedLibPath,
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

	spec.Process.Env = append(spec.Process.Env, "LD_PRELOAD="+cedanaSharedLibPath)
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

// Checkpoint a runc container, returns the path to the checkpoint
func (c *CedanaClient) Checkpoint(ctx context.Context, containerId string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCheckpointDeadline)
	defer cancel()

	external := []string{} // Add any external mounts that cause CRIU failures here

	args := api.JobDumpArgs{
		JID: containerId,
		CriuOpts: &api.CriuOpts{
			TcpClose:     true,
			LeaveRunning: true,
			External:     external,
			// TODO: add skip in flight connections option
		},
		Dir: fmt.Sprintf("%s/%s", checkpointPathBase, containerId),
	}
	res, err := c.service.JobDump(ctx, &args)
	if err != nil {
		return "", err
	}
	return res.GetState().GetCheckpointPath(), nil
}

// Restore a runc container. If a checkpoint path is provided, it will be used as the checkpoint.
// If empty path is provided, the latest checkpoint path from DB will be used.
func (c *CedanaClient) Restore(
	ctx context.Context,
	containerId string,
	checkpointPath string,
	opts *runc.CreateOpts,
) (*api.ProcessState, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCheckpointDeadline)
	defer cancel()

	// NOTE: Cedana uses bundle path to find the config.json
	bundle := strings.TrimRight(opts.ConfigPath, filepath.Base(opts.ConfigPath))

	ttyPath := filepath.Join(os.TempDir(), fmt.Sprintf("cedana-tty-%s.sock", containerId))
  tty := exec.CommandContext(ctx, cedanaBinPath, "debug", "recvtty", ttyPath)
  tty.Stdout = opts.OutputWriter
  tty.Stderr = opts.OutputWriter
  err := tty.Start()
  if err != nil {
    return nil, err
  }

	args := &api.JobRestoreArgs{
		JID: containerId,
		RuncOpts: &api.RuncOpts{
			Root:          runcRoot,
			Bundle:        bundle,
			Detach:        true,
			ConsoleSocket: ttyPath,
		},
		CriuOpts:       &api.CriuOpts{TcpClose: true},
		CheckpointPath: checkpointPath,
	}
	res, err := c.service.JobRestore(ctx, args)
	if err != nil {
		return nil, err
	}
	if opts.Started != nil {
		opts.Started <- int(res.GetState().GetPID())
	}

  tty.Wait()

	return res.State, nil
}

// Perform a detailed health check of cedana C/R capabilities
func (c *CedanaClient) DetailedHealthCheckWait(
	ctx context.Context,
) (*api.DetailedHealthCheckResponse, error) {
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
