package vast

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

func DetectNvidiaGPUs(ctx context.Context) ([]GPU, error) {
	cmd := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=index,uuid,name", "--format=csv,noheader")
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("detect nvidia gpus: %w", err)
	}
	gpus := parseNvidiaSMI(out)
	if len(gpus) == 0 {
		return nil, fmt.Errorf("no nvidia gpus detected")
	}
	return gpus, nil
}

func DetectVisibleGPUUUID(ctx context.Context) (string, error) {
	if values := splitCSV(os.Getenv(types.NvidiaVisibleDevicesEnv)); len(values) > 0 && strings.HasPrefix(values[0], "GPU-") {
		if len(values) != 1 {
			return "", fmt.Errorf("expected exactly one visible GPU, %s lists %d", types.NvidiaVisibleDevicesEnv, len(values))
		}
		return values[0], nil
	}
	gpus, err := DetectNvidiaGPUs(ctx)
	if err != nil {
		return "", err
	}
	if len(gpus) != 1 {
		return "", fmt.Errorf("expected exactly one visible GPU, detected %d", len(gpus))
	}
	return gpus[0].UUID, nil
}

func parseNvidiaSMI(out []byte) []GPU {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	gpus := make([]GPU, 0, len(lines))
	for _, line := range lines {
		fields := strings.SplitN(line, ",", 3)
		if len(fields) != 3 {
			continue
		}
		gpu := GPU{
			Index: strings.TrimSpace(fields[0]),
			UUID:  strings.TrimSpace(fields[1]),
			Name:  strings.TrimSpace(fields[2]),
		}
		if gpu.Index == "" || gpu.UUID == "" {
			continue
		}
		gpus = append(gpus, gpu)
	}
	return gpus
}

func gpuByIndex(gpus []GPU, index string) (GPU, bool) {
	for _, gpu := range gpus {
		if gpu.Index == index {
			return gpu, true
		}
	}
	return GPU{}, false
}

func gpuByUUID(gpus []GPU, uuid string) (GPU, bool) {
	for _, gpu := range gpus {
		if gpu.UUID == uuid {
			return gpu, true
		}
	}
	return GPU{}, false
}

func hostFingerprint() string {
	for _, path := range []string{"/etc/machine-id", "/var/lib/dbus/machine-id"} {
		if data, err := os.ReadFile(path); err == nil {
			value := strings.TrimSpace(string(data))
			if value != "" {
				return value
			}
		}
	}
	hostname, _ := os.Hostname()
	sum := sha256.Sum256([]byte(runtime.GOOS + "\x00" + runtime.GOARCH + "\x00" + hostname))
	return hex.EncodeToString(sum[:])
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
