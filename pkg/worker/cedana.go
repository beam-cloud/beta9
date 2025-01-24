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
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/go-runc"
	cedana "github.com/cedana/cedana/pkg/client"
	"github.com/cedana/cedana/pkg/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

const (
	fullHealthCheck = true
	runcRoot        = "/run/runc"
)

type CedanaClient struct {
	*cedana.Client
}

func InitializeCedana(
	ctx context.Context,
	c config.Config,
) (*cedana.Client, error) {
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
	resp, err := client.HealthCheck(ctx, &cedanadaemon.HealthCheckReq{Full: fullHealthCheck}, grpc.WaitForReady(true))
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

	return client, nil
}

// Spawn a runc container using cedana, creating a 'job' in cedana
func (c *CedanaClient) Run(ctx context.Context, containerId string, bundle string, gpuEnabled bool, runcOpts *runc.CreateOpts) (int, error) {
	// If config path provided directly, derive bundle from it
	if runcOpts.ConfigPath != "" {
		bundle = strings.TrimRight(runcOpts.ConfigPath, filepath.Base(runcOpts.ConfigPath))
	}

	args := &cedanadaemon.RunReq{
		Action:     cedanadaemon.RunAction_START_NEW,
		JID:        containerId, // just use containerId for convenience
		GPUEnabled: gpuEnabled,
		Attachable: true,
		Type:       "runc",
		Details: &cedanadaemon.Details{
			Runc: &cedanarunc.Runc{
				ID:     containerId,
				Bundle: bundle,
				Root:   runcRoot,
			},
		},
	}

	resp, profilingData, err := c.Client.Run(ctx, args)
	if err != nil {
		return -1, fmt.Errorf("failed to run runc container: %w", err)
	}

	_ = profilingData

	_, stdout, stderr, exitCode, errors, err := c.AttachIO(ctx, &cedanadaemon.AttachReq{PID: resp.PID})
	if err != nil {
		return -1, fmt.Errorf("failed to attach to runc container: %w", err)
	}

	go io.Copy(runcOpts.OutputWriter, stdout)
	go io.Copy(runcOpts.OutputWriter, stderr)

	if err := <-errors; err != nil {
		return -1, err
	}

	return <-exitCode, nil
}

func (c *CedanaClient) Checkpoint(ctx context.Context, containerId string) (string, error) {
	args := &cedanadaemon.DumpReq{
		Name:    containerId,
		Type:    "job",
		Details: &cedanadaemon.Details{JID: &containerId},
	}

	resp, profilingData, err := c.Client.Dump(ctx, args)
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

func (c *CedanaClient) Restore(ctx context.Context, restoreOpts *cedanaRestoreOpts, runcOpts *runc.CreateOpts) (int, error) {
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

	args := &cedanadaemon.RestoreReq{
		Path:       restoreOpts.checkpointPath,
		Type:       "job",
		Attachable: true,
		Details: &cedanadaemon.Details{
			JID: &restoreOpts.jobId,
			Runc: &cedanarunc.Runc{
				ID:     restoreOpts.containerId,
				Bundle: bundle,
				Root:   runcRoot,
			},
		},
	}

	resp, profilingData, err := c.Client.Restore(ctx, args)
	if err != nil {
		return -1, fmt.Errorf("failed to restore runc container: %w", err)
	}

	_ = profilingData

	_, stdout, stderr, exitCode, errors, err := c.AttachIO(ctx, &cedanadaemon.AttachReq{PID: resp.PID})
	if err != nil {
		return -1, fmt.Errorf("failed to attach to runc container: %w", err)
	}

	go io.Copy(runcOpts.OutputWriter, stdout)
	go io.Copy(runcOpts.OutputWriter, stderr)

	if err := <-errors; err != nil {
		return -1, err
	}

	return <-exitCode, nil
}
