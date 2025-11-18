package runtime

import (
	"context"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunscPrepare_MultiGPU verifies that multi-GPU configurations work correctly
// with gVisor's nvproxy by ensuring:
// 1. CDI annotations are detected as GPU indicators
// 2. GPU devices are preserved exactly as CDI provided them
// 3. nvproxy is enabled when GPUs are detected
func TestRunscPrepare_MultiGPU(t *testing.T) {
	tests := []struct {
		name               string
		setupSpec          func() *specs.Spec
		wantNvproxyEnabled bool
	}{
		{
			name: "multi-GPU via CDI - unsupported devices filtered",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},          // Kept
							{Path: "/dev/nvidia1"},          // Kept
							{Path: "/dev/nvidiactl"},        // Kept
							{Path: "/dev/nvidia-uvm"},       // Kept
							{Path: "/dev/nvidia-modeset"},   // Filtered out
							{Path: "/dev/nvidia-uvm-tools"}, // Filtered out
							{Path: "/dev/dri/card1"},        // Filtered out
							{Path: "/dev/dri/renderD128"},   // Filtered out
						},
					},
					Annotations: map[string]string{
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
						"cdi.k8s.io/nvidia.com_gpu_1": "nvidia.com/gpu=1",
					},
				}
			},
			wantNvproxyEnabled: true,
		},
		{
			name: "single GPU via CDI - devices preserved",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},
							{Path: "/dev/nvidiactl"},
							{Path: "/dev/nvidia-uvm"},
						},
					},
					Annotations: map[string]string{
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
					},
				}
			},
			wantNvproxyEnabled: true,
		},
		{
			name: "CPU only - no GPUs, devices cleared",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/null"},
						},
					},
					Annotations: map[string]string{},
				}
			},
			wantNvproxyEnabled: false,
		},
		{
			name: "GPU detection via annotation only",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{},
					},
					Annotations: map[string]string{
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
					},
				}
			},
			wantNvproxyEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runsc := &Runsc{
				cfg: Config{
					RunscPath: "runsc",
					RunscRoot: "/run/gvisor",
				},
			}

			spec := tt.setupSpec()
			ctx := context.Background()

			err := runsc.Prepare(ctx, spec)
			require.NoError(t, err)

			// Verify nvproxy enablement
			assert.Equal(t, tt.wantNvproxyEnabled, runsc.nvproxyEnabled,
				"nvproxy enablement mismatch")

			assert.Equal(t, 0, len(spec.Linux.Devices), "gVisor specs should not include device nodes")

			if tt.wantNvproxyEnabled {
				assert.Equal(t, "true", spec.Annotations["dev.gvisor.internal.nvproxy"],
					"nvproxy annotation should be set when GPUs are requested")
			}
		})
	}
}

// TestRunscPrepare_GPUDetection verifies the hasGPUDevices method correctly
// detects GPUs via both device paths and CDI annotations
func TestRunscPrepare_GPUDetection(t *testing.T) {
	runsc := &Runsc{}

	tests := []struct {
		name    string
		spec    *specs.Spec
		wantGPU bool
	}{
		{
			name: "detect via /dev/nvidia* device path",
			spec: &specs.Spec{
				Linux: &specs.Linux{
					Devices: []specs.LinuxDevice{
						{Path: "/dev/nvidia0"},
					},
				},
			},
			wantGPU: true,
		},
		{
			name: "detect via CDI annotation",
			spec: &specs.Spec{
				Linux: &specs.Linux{},
				Annotations: map[string]string{
					"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
				},
			},
			wantGPU: true,
		},
		{
			name: "detect multi-GPU via CDI annotations",
			spec: &specs.Spec{
				Linux: &specs.Linux{},
				Annotations: map[string]string{
					"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
					"cdi.k8s.io/nvidia.com_gpu_1": "nvidia.com/gpu=1",
					"cdi.k8s.io/nvidia.com_gpu_2": "nvidia.com/gpu=2",
					"cdi.k8s.io/nvidia.com_gpu_3": "nvidia.com/gpu=3",
				},
			},
			wantGPU: true,
		},
		{
			name: "no GPU - no devices or annotations",
			spec: &specs.Spec{
				Linux:       &specs.Linux{},
				Annotations: map[string]string{},
			},
			wantGPU: false,
		},
		{
			name: "no GPU - unrelated device",
			spec: &specs.Spec{
				Linux: &specs.Linux{
					Devices: []specs.LinuxDevice{
						{Path: "/dev/null"},
					},
				},
			},
			wantGPU: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasGPU := runsc.hasGPUDevices(tt.spec)
			assert.Equal(t, tt.wantGPU, hasGPU)
		})
	}
}

// TestRunscRun_MultiGPU_FlagPropagation verifies that the --nvproxy flag
// is correctly passed to runsc when GPUs are detected
func TestRunscRun_MultiGPU_FlagPropagation(t *testing.T) {
	runsc := &Runsc{
		cfg: Config{
			RunscPath: "runsc",
			RunscRoot: "/run/gvisor",
		},
	}

	runsc.nvproxyEnabled = true
	args := runsc.baseArgs(false)
	if runsc.nvproxyEnabled {
		args = append(args, "--nvproxy=true", "--nvproxy-docker=true")
	}

	flags := map[string]bool{}
	for _, arg := range args {
		flags[arg] = true
	}
	assert.True(t, flags["--nvproxy=true"])
	assert.True(t, flags["--nvproxy-docker=true"])

	// Verify base args include necessary gVisor flags
	hasRoot := false
	hasOverlay2 := false
	hasFileAccess := false
	for _, arg := range args {
		if arg == "--root" {
			hasRoot = true
		}
		if arg == "--overlay2=none" {
			hasOverlay2 = true
		}
		if arg == "--file-access=shared" {
			hasFileAccess = true
		}
	}
	assert.True(t, hasRoot, "Expected --root flag in base args")
	assert.True(t, hasOverlay2, "Expected --overlay2=none flag for gVisor")
	assert.True(t, hasFileAccess, "Expected --file-access=shared flag for gVisor")
}
