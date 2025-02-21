package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	cedanagrpc "buf.build/gen/go/cedana/task/grpc/go/_gogrpc"
	cedanaproto "buf.build/gen/go/cedana/task/protocolbuffers/go"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/go-runc"
	types "github.com/cedana/cedana/pkg/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/opencontainers/runtime-spec/specs-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	runcRoot                   = "/run/runc"
	cedanaHost                 = "0.0.0.0"
	cedanaBinPath              = "/usr/bin/cedana"
	cedanaSharedLibPath        = "/usr/local/lib/libcedana-gpu.so"
	cedanaLogLevel             = "info"
	checkpointPathBase         = "/tmp/checkpoints"
	defaultManageDeadline      = 10 * time.Second
	defaultCheckpointDeadline  = 10 * time.Minute
	defaultRestoreDeadline     = 5 * time.Minute
	defaultHealthCheckDeadline = 30 * time.Second
	cedanaUseRemoteDB          = true // Do not change, or migrations across workers will fail
)

type CedanaClient struct {
	conn    *grpc.ClientConn
	service cedanagrpc.TaskServiceClient
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

	taskClient := cedanagrpc.NewTaskServiceClient(taskConn)

	// Launch the daemon
	daemon := exec.CommandContext(ctx, cedanaBinPath, "daemon", "start",
		fmt.Sprintf("--port=%d", port),
		fmt.Sprintf("--gpu-enabled=%t", gpuEnabled))

	daemon.Stdout = &common.ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Info().Str("operation", "cedana daemon start") }}
	daemon.Stderr = &common.ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Error().Str("operation", "cedana daemon start") }}

	// XXX: Set config using env until config JSON parsing is fixed
	daemon.Env = append(os.Environ(),
		fmt.Sprintf("CEDANA_LOG_LEVEL=%s", cedanaLogLevel),
		fmt.Sprintf("CEDANA_URL=%s", config.Connection.URL),
		fmt.Sprintf("CEDANA_AUTH_TOKEN=%s", config.Connection.AuthToken),
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
func (c *CedanaClient) PrepareContainerSpec(spec *specs.Spec, containerId string, containerHostname string, gpuEnabled bool) error {
	os.MkdirAll(checkpointSignalDir(containerId), os.ModePerm) // Add a mount point for the checkpoint signal file

	spec.Mounts = append(spec.Mounts, specs.Mount{
		Type:        "bind",
		Source:      checkpointSignalDir(containerId),
		Destination: "/cedana",
		Options: []string{
			"rbind",
			"rprivate",
			"nosuid",
			"nodev",
		},
	})

	containerIdPath := filepath.Join(checkpointSignalDir(containerId), checkpointContainerIdFileName)
	err := os.WriteFile(containerIdPath, []byte(containerId), 0644)
	if err != nil {
		return err
	}

	containerHostnamePath := filepath.Join(checkpointSignalDir(containerId), checkpointContainerHostnameFileName)
	err = os.WriteFile(containerHostnamePath, []byte(containerHostname), 0644)
	if err != nil {
		return err
	}

	if !gpuEnabled {
		return nil // No need to do anything else if GPU is not enabled
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

	// Add shared memory mount from worker instead, remove existing /dev/shm mount
	for i, m := range spec.Mounts {
		if m.Destination == "/dev/shm" {
			spec.Mounts = append(spec.Mounts[:i], spec.Mounts[i+1:]...)
			break
		}
	}

	// Add shared memory mount from worker
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

	spec.Process.Env = append(spec.Process.Env, "CEDANA_JID="+containerId, "LD_PRELOAD="+cedanaSharedLibPath)
	return nil
}

// Start managing a runc container
func (c *CedanaClient) Manage(ctx context.Context, containerId string, gpuEnabled bool) error {
	ctx, cancel := context.WithTimeout(ctx, defaultManageDeadline)
	defer cancel()

	args := &cedanaproto.RuncManageArgs{
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

	args := cedanaproto.JobDumpArgs{
		JID: containerId,
		CriuOpts: &cedanaproto.CriuOpts{
			TcpClose:        true,
			TcpEstablished:  true,
			LeaveRunning:    true,
			TcpSkipInFlight: true,
		},
		Dir: fmt.Sprintf("%s/%s", checkpointPathBase, containerId),
	}
	res, err := c.service.JobDump(ctx, &args)
	if err != nil {
		return "", err
	}

	log.Info().Str("container_id", containerId).Interface("dump_stats", res.GetDumpStats()).Msg("dump stats")
	return res.GetState().GetCheckpointPath(), nil
}

type cedanaRestoreOpts struct {
	jobId          string
	containerId    string
	checkpointPath string
	cacheFunc      func(string, string) (string, error)
}

// Restore a runc container. If a checkpoint path is provided, it will be used as the checkpoint.
// If empty path is provided, the latest checkpoint path from DB will be used.
func (c *CedanaClient) Restore(
	ctx context.Context,
	restoreOpts cedanaRestoreOpts,
	runcOpts *runc.CreateOpts,
) (*cedanaproto.ProcessState, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultCheckpointDeadline)
	defer cancel()

	// NOTE: Cedana uses bundle path to find the config.json
	bundle := strings.TrimRight(runcOpts.ConfigPath, filepath.Base(runcOpts.ConfigPath))

	// If a cache function is provided, attempt to cache the checkpoint nearby
	if restoreOpts.cacheFunc != nil {
		checkpointPath, err := restoreOpts.cacheFunc(restoreOpts.containerId, restoreOpts.checkpointPath)
		if err == nil {
			log.Info().Str("container_id", restoreOpts.containerId).Msgf("using cached checkpoint located at: %s", checkpointPath)
			restoreOpts.checkpointPath = checkpointPath
		} else {
			log.Error().Str("container_id", restoreOpts.containerId).Msgf("failed to cache checkpoint nearby: %v", err)
		}
	}

	args := &cedanaproto.JobRestoreArgs{
		JID: restoreOpts.jobId,
		RuncOpts: &cedanaproto.RuncOpts{
			Root:          runcRoot,
			Bundle:        bundle,
			Detach:        true,
			ConsoleSocket: runcOpts.ConsoleSocket.Path(),
			ContainerID:   restoreOpts.containerId,
		},
		CriuOpts:       &cedanaproto.CriuOpts{TcpClose: true, TcpEstablished: true},
		CheckpointPath: restoreOpts.checkpointPath,
	}
	res, err := c.service.JobRestore(ctx, args)
	if err != nil {
		return nil, err
	}

	log.Info().Str("container_id", restoreOpts.containerId).Interface("restore_stats", res.GetRestoreStats()).Msg("restore stats")

	if runcOpts.Started != nil {
		runcOpts.Started <- int(res.GetState().GetPID())
	}

	return res.State, nil
}

// Perform a detailed health check of cedana C/R capabilities
func (c *CedanaClient) DetailedHealthCheckWait(
	ctx context.Context,
) (*cedanaproto.DetailedHealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultHealthCheckDeadline)
	defer cancel()

	opts := []grpc.CallOption{}
	opts = append(opts, grpc.WaitForReady(true))

	res, err := c.service.DetailedHealthCheck(ctx, &cedanaproto.DetailedHealthCheckRequest{}, opts...)
	if err != nil {
		return nil, err
	}

	return res, nil
}
