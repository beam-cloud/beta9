package vast

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
	opts.MaxCPU, opts.MaxMemory = defaultPerGPUCapacity(opts.MaxCPU, opts.MaxMemory, len(gpus))

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
	if opts.MaxCPU != "" || opts.MaxMemory != "" {
		fmt.Fprintf(opts.Stdout, "advertising max_cpu=%s max_memory=%s\n", firstNonEmpty(opts.MaxCPU, "unset"), firstNonEmpty(opts.MaxMemory, "unset"))
	}
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

// defaultPerGPUCapacity fills in MaxCPU/MaxMemory with an equal share of the
// detected host capacity when the operator did not set explicit limits.
// Without this, every per-GPU agent on an N-GPU host would advertise the full
// host CPU and memory, over-advertising N× total capacity. Explicit
// --max-cpu/--max-memory values (from install's --max-cpu-per-gpu and
// --max-memory-per-gpu flags) always win.
func defaultPerGPUCapacity(maxCPU, maxMemory string, gpuCount int) (string, string) {
	if gpuCount < 1 {
		gpuCount = 1
	}
	if strings.TrimSpace(maxCPU) == "" {
		cores := agent.SystemCPUCount() / gpuCount
		if cores < 1 {
			cores = 1
		}
		maxCPU = strconv.Itoa(cores)
	}
	if strings.TrimSpace(maxMemory) == "" {
		if memoryMB := agent.SystemMemoryMB() / uint64(gpuCount); memoryMB > 0 {
			maxMemory = strconv.FormatUint(memoryMB, 10) + "Mi"
		}
	}
	return maxCPU, maxMemory
}
