package vast

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/agent"
	"github.com/beam-cloud/beta9/pkg/types"
)

func RunGPUAgent(ctx context.Context, opts GPUAgentOptions) error {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	opts.GatewayURL = strings.TrimRight(strings.TrimSpace(opts.GatewayURL), "/")
	if opts.GatewayURL == "" {
		return fmt.Errorf("gateway is required")
	}
	if strings.TrimSpace(opts.JoinToken) == "" && strings.TrimSpace(opts.JoinTokenFile) == "" {
		return fmt.Errorf("join-token or join-token-file is required")
	}
	opts.StateDir = firstNonEmpty(strings.TrimSpace(opts.StateDir), DefaultStateDir)
	opts.GPUIndex = strings.TrimSpace(opts.GPUIndex)
	if opts.GPUIndex == "" {
		return fmt.Errorf("gpu-index is required")
	}
	if opts.DetectGPUs == nil {
		opts.DetectGPUs = DetectNvidiaGPUs
	}
	gpus, err := opts.DetectGPUs(ctx)
	if err != nil {
		return err
	}
	gpu, ok := gpuByIndex(gpus, opts.GPUIndex)
	if !ok {
		return fmt.Errorf("gpu index %q was not detected", opts.GPUIndex)
	}

	stateDir := gpuStateDir(opts.StateDir, gpu)
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return err
	}
	if err := os.Setenv(types.AgentStateDirEnv, stateDir); err != nil {
		return err
	}
	if err := os.Setenv(types.AgentFingerprintEnv, hostFingerprint()+":vast-gpu:"+gpu.UUID); err != nil {
		return err
	}
	if opts.WorkerImage != "" {
		if err := os.Setenv(types.AgentWorkerImageEnv, opts.WorkerImage); err != nil {
			return err
		}
	}

	fmt.Fprintf(opts.Stdout, "starting vast gpu agent index=%s uuid=%s state_dir=%s\n", gpu.Index, gpu.UUID, filepath.Clean(stateDir))
	return agent.RunJoin(ctx, types.AgentJoinOptions{
		GatewayURL:                opts.GatewayURL,
		JoinToken:                 opts.JoinToken,
		JoinTokenFile:             opts.JoinTokenFile,
		GPUIDs:                    gpu.UUID,
		MaxCPU:                    opts.MaxCPU,
		MaxMemory:                 opts.MaxMemory,
		WorkerImage:               opts.WorkerImage,
		NetworkSlots:              opts.NetworkSlots,
		ContainerStartConcurrency: opts.ContainerStartConcurrency,
		Stdout:                    opts.Stdout,
		Stderr:                    opts.Stderr,
	})
}
