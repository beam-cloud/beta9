package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"

	cedanadaemon "buf.build/gen/go/cedana/cedana/protocolbuffers/go/daemon"
	cedanarunc "buf.build/gen/go/cedana/cedana/protocolbuffers/go/plugins/runc"
	"buf.build/gen/go/cedana/criu/protocolbuffers/go/criu"
	common "github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	cedana "github.com/cedana/cedana/pkg/client"
	"github.com/cedana/cedana/pkg/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const runcRoot = "/run/runc"

type CedanaCRIUManager struct {
	client *cedana.Client
}

func InitializeCedanaCRIU(
	ctx context.Context,
	c config.Config,
) (*CedanaCRIUManager, error) {
	path, err := exec.LookPath("cedana")
	if err != nil {
		return nil, fmt.Errorf("cedana binary not found: %w", err)
	}

	// Apply the config globally
	config.Global = c

	// Parse the config for the daemon
	configJson, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("incompatible config type: %w", err)
	}

	cmd := exec.CommandContext(ctx, path, "daemon", "start", fmt.Sprintf("--config=%s", configJson))

	cmd.Stdout = &common.ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Info().Str("operation", "cedana daemon start") }}
	cmd.Stderr = &common.ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Error().Str("operation", "cedana daemon start") }}

	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start cedana daemon: %v", err)
	}

	client, err := cedana.New(c.Address, c.Protocol)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	// Cleanup the daemon on exit
	go func() {
		cmd.Wait()
		client.Close()
	}()

	// Wait for the daemon to be ready, and do health check
	resp, err := client.HealthCheck(ctx, &cedanadaemon.HealthCheckReq{Full: false}, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("cedana health check failed: %w", err)
	}

	errorsFound := false
	for _, result := range resp.Results {
		for _, component := range result.Components {
			for _, errs := range component.Errors {
				log.Error().Str("name", component.Name).Str("data", component.Data).Msgf("cedana health check error: %v", errs)
				errorsFound = true
			}
			for _, warning := range component.Warnings {
				log.Warn().Str("name", component.Name).Str("data", component.Data).Msgf("cedana health check warning: %v", warning)
			}
		}
	}
	if errorsFound {
		return nil, fmt.Errorf("cedana health check failed")
	}

	log.Info().Msg("cedana client initialized")

	return &CedanaCRIUManager{client: client}, nil
}

func (c *CedanaCRIUManager) Available() bool {
	return c.client != nil
}

// Spawn a runc container using cedana, creating a 'job' in cedana
func (c *CedanaCRIUManager) Run(ctx context.Context, request *types.ContainerRequest, bundlePath string, runcOpts *runc.CreateOpts) (int, error) {
	// If config path provided directly, derive bundle from it
	args := &cedanadaemon.RunReq{
		Action:     cedanadaemon.RunAction_START_NEW,
		JID:        request.ContainerId,
		GPUEnabled: request.RequiresGPU(),
		Attachable: true,
		Type:       "runc",
		Details: &cedanadaemon.Details{
			Runc: &cedanarunc.Runc{
				ID:     request.ContainerId,
				Bundle: bundlePath,
				Root:   runcRoot,
			},
		},
	}

	resp, profilingData, err := c.client.Run(ctx, args)
	if err != nil {
		return -1, fmt.Errorf("failed to run runc container: %w", err)
	}

	if runcOpts.Started != nil {
		runcOpts.Started <- int(resp.PID)
	}

	_ = profilingData

	_, stdout, stderr, exitCodeChan, _, err := c.client.AttachIO(ctx, &cedanadaemon.AttachReq{PID: resp.PID})
	if err != nil {
		return -1, fmt.Errorf("failed to attach to runc container: %w", err)
	}

	go io.Copy(runcOpts.OutputWriter, stdout)
	go io.Copy(runcOpts.OutputWriter, stderr)

	exitCode := <-exitCodeChan
	return exitCode, nil
}

func (c *CedanaCRIUManager) CreateCheckpoint(ctx context.Context, request *types.ContainerRequest, onComplete func(request *types.ContainerRequest, err error)) (string, error) {
	args := &cedanadaemon.DumpReq{
		Name: request.ContainerId,
		Type: "job",
		Criu: &criu.CriuOpts{
			TcpSkipInFlight: proto.Bool(true),
			TcpEstablished:  proto.Bool(true),
			LeaveRunning:    proto.Bool(true),
			LinkRemap:       proto.Bool(true),
		},
		Details: &cedanadaemon.Details{JID: &request.ContainerId},
	}

	resp, profilingData, err := c.client.Dump(ctx, args)
	if err != nil {
		return "", fmt.Errorf("failed to dump runc container: %w", err)
	}
	_ = profilingData

	onComplete(request, nil)

	return resp.Path, nil
}

type cedanaRestoreOpts struct {
	jobId          string
	containerId    string
	checkpointPath string
	cacheFunc      func(string, string) (string, error)
}

func (c *CedanaCRIUManager) RestoreCheckpoint(ctx context.Context, opts *RestoreOpts) (int, error) {
	restoreOpts := cedanaRestoreOpts{
		checkpointPath: opts.state.RemoteKey,
		jobId:          opts.state.ContainerId,
		containerId:    opts.request.ContainerId,
	}

	bundle := strings.TrimRight(opts.configPath, filepath.Base(opts.configPath))

	args := &cedanadaemon.RestoreReq{
		Path:       restoreOpts.checkpointPath,
		Type:       "job",
		Attachable: true,
		Criu: &criu.CriuOpts{
			TcpClose:       proto.Bool(true),
			TcpEstablished: proto.Bool(true),
		},
		Details: &cedanadaemon.Details{
			JID: &restoreOpts.jobId,
			Runc: &cedanarunc.Runc{
				ID:     restoreOpts.containerId,
				Bundle: bundle,
				Root:   runcRoot,
			},
		},
	}

	resp, profilingData, err := c.client.Restore(ctx, args)
	if err != nil {
		return -1, fmt.Errorf("failed to restore runc container: %w", err)
	}

	if opts.runcOpts.Started != nil {
		opts.runcOpts.Started <- int(resp.PID)
	}

	_ = profilingData

	_, stdout, stderr, exitCodeChan, _, err := c.client.AttachIO(ctx, &cedanadaemon.AttachReq{PID: resp.PID})
	if err != nil {
		return -1, fmt.Errorf("failed to attach to runc container: %w", err)
	}

	go io.Copy(opts.runcOpts.OutputWriter, stdout)
	go io.Copy(opts.runcOpts.OutputWriter, stderr)

	exitCode := <-exitCodeChan
	return exitCode, nil
}
