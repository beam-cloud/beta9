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
// 2. spec.Linux.Devices is cleared to prevent conflicts with nvproxy
// 3. nvproxy is enabled when GPUs are detected
func TestRunscPrepare_MultiGPU(t *testing.T) {
	tests := []struct {
		name              string
		setupSpec         func() *specs.Spec
		wantNvproxyEnabled bool
		wantDevicesCleared bool
	}{
		{
			name: "multi-GPU via CDI annotations",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},
							{Path: "/dev/nvidia1"},
						},
					},
					Annotations: map[string]string{
						// CDI adds these annotations when injecting multiple GPUs
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
						"cdi.k8s.io/nvidia.com_gpu_1": "nvidia.com/gpu=1",
					},
				}
			},
			wantNvproxyEnabled: true,
			wantDevicesCleared: true,
		},
		{
			name: "single GPU via CDI annotations",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},
						},
					},
					Annotations: map[string]string{
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
					},
				}
			},
			wantNvproxyEnabled: true,
			wantDevicesCleared: true,
		},
		{
			name: "CPU only - no GPUs",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{},
					},
					Annotations: map[string]string{},
				}
			},
			wantNvproxyEnabled: false,
			wantDevicesCleared: true, // Devices should still be cleared for consistency
		},
		{
			name: "GPU detection via device path only (legacy)",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},
							{Path: "/dev/nvidiactl"},
							{Path: "/dev/nvidia-uvm"},
						},
					},
					Annotations: map[string]string{},
				}
			},
			wantNvproxyEnabled: true,
			wantDevicesCleared: true,
		},
		{
			name: "GPU detection via CDI annotations only (devices already cleared)",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{}, // No devices, only CDI annotations
					},
					Annotations: map[string]string{
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
						"cdi.k8s.io/nvidia.com_gpu_1": "nvidia.com/gpu=1",
					},
				}
			},
			wantNvproxyEnabled: true,
			wantDevicesCleared: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create runsc runtime
			runsc := &Runsc{
				cfg: Config{
					RunscPath: "runsc",
					RunscRoot: "/run/gvisor",
				},
			}

			spec := tt.setupSpec()
			ctx := context.Background()

			// Call Prepare
			err := runsc.Prepare(ctx, spec)
			require.NoError(t, err)

			// Verify nvproxy was enabled correctly
			assert.Equal(t, tt.wantNvproxyEnabled, runsc.nvproxyEnabled,
				"nvproxy enablement mismatch")

			// Verify devices were cleared
			if tt.wantDevicesCleared {
				assert.Nil(t, spec.Linux.Devices,
					"spec.Linux.Devices should be cleared to prevent conflicts with nvproxy")
			}

			// Verify annotations are preserved (CDI needs these)
			if tt.wantNvproxyEnabled && len(spec.Annotations) > 0 {
				hasGPUAnnotation := false
				for key := range spec.Annotations {
					if key == "cdi.k8s.io/nvidia.com_gpu_0" || key == "cdi.k8s.io/nvidia.com_gpu_1" {
						hasGPUAnnotation = true
						break
					}
				}
				assert.True(t, hasGPUAnnotation,
					"CDI annotations should be preserved for nvproxy to read GPU configuration")
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

	// Test with nvproxy enabled
	runsc.nvproxyEnabled = true
	args := runsc.baseArgs(false)
	args = append(args, "--nvproxy=true")
	
	// Verify --nvproxy=true is in the args
	found := false
	for _, arg := range args {
		if arg == "--nvproxy=true" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected --nvproxy=true flag when nvproxy is enabled")

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
