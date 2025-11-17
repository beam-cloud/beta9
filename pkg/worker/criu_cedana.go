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
	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
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

func (c *CedanaCRIUManager) CreateCheckpoint(ctx context.Context, rt runtime.Runtime, checkpointId string, request *types.ContainerRequest) (string, error) {
	// Cedana currently only supports runc, not gVisor
	if rt.Name() != "runc" {
		return "", fmt.Errorf("cedana checkpoint only supports runc runtime, got: %s", rt.Name())
	}
	args := &cedanadaemon.DumpReq{
		Name: checkpointId,
		Type: "job",
		Criu: &criu.CriuOpts{
			TcpSkipInFlight: proto.Bool(true),
			TcpEstablished:  proto.Bool(true),
			LeaveRunning:    proto.Bool(true),
			LinkRemap:       proto.Bool(true),
		},
		Details: &cedanadaemon.Details{JID: &checkpointId},
	}

	resp, profilingData, err := c.client.Dump(ctx, args)
	if err != nil {
		return "", fmt.Errorf("failed to dump runc container: %w", err)
	}
	_ = profilingData

	return resp.Path, nil
}

type cedanaRestoreOpts struct {
	jobId          string
	containerId    string
	checkpointPath string
	cacheFunc      func(string, string) (string, error)
}

func (c *CedanaCRIUManager) RestoreCheckpoint(ctx context.Context, rt runtime.Runtime, opts *RestoreOpts) (int, error) {
	// Cedana currently only supports runc, not gVisor
	if rt.Name() != "runc" {
		return -1, fmt.Errorf("cedana restore only supports runc runtime, got: %s", rt.Name())
	}
	restoreOpts := cedanaRestoreOpts{
		checkpointPath: opts.checkpoint.RemoteKey,
		jobId:          opts.checkpoint.SourceContainerId,
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

	if opts.started != nil {
		opts.started <- int(resp.PID)
	}

	_ = profilingData

	_, stdout, stderr, exitCodeChan, _, err := c.client.AttachIO(ctx, &cedanadaemon.AttachReq{PID: resp.PID})
	if err != nil {
		return -1, fmt.Errorf("failed to attach to runc container: %w", err)
	}

	if opts.outputWriter != nil {
		go io.Copy(opts.outputWriter, stdout)
		go io.Copy(opts.outputWriter, stderr)
	}

	exitCode := <-exitCodeChan
	return exitCode, nil
}
