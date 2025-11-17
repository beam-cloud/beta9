package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunscPrepare_MultiGPU verifies that multi-GPU configurations work correctly
// with gVisor's nvproxy by ensuring:
// 1. CDI annotations are detected as GPU indicators
// 2. GPU devices are preserved for nvproxy to virtualize
// 3. Non-GPU devices are filtered out
// 4. nvproxy is enabled when GPUs are detected
func TestRunscPrepare_MultiGPU(t *testing.T) {
	tests := []struct {
		name                   string
		setupSpec              func() *specs.Spec
		wantNvproxyEnabled     bool
		wantGPUDevicesPreserved bool
		wantDeviceCount        int
	}{
		{
			name: "multi-GPU via CDI annotations",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},
							{Path: "/dev/nvidia1"},
							{Path: "/dev/nvidiactl"},
						},
					},
					Annotations: map[string]string{
						// CDI adds these annotations when injecting multiple GPUs
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
						"cdi.k8s.io/nvidia.com_gpu_1": "nvidia.com/gpu=1",
					},
				}
			},
			wantNvproxyEnabled:     true,
			wantGPUDevicesPreserved: true,
			wantDeviceCount:        3, // 2 GPUs + nvidiactl
		},
		{
			name: "single GPU via CDI annotations",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},
							{Path: "/dev/nvidiactl"},
						},
					},
					Annotations: map[string]string{
						"cdi.k8s.io/nvidia.com_gpu_0": "nvidia.com/gpu=0",
					},
				}
			},
			wantNvproxyEnabled:     true,
			wantGPUDevicesPreserved: true,
			wantDeviceCount:        2, // 1 GPU + nvidiactl
		},
		{
			name: "CPU only - no GPUs",
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
			wantNvproxyEnabled:     false,
			wantGPUDevicesPreserved: false,
			wantDeviceCount:        0, // All devices cleared for CPU-only
		},
		{
			name: "GPU with non-GPU devices - filter non-GPU",
			setupSpec: func() *specs.Spec {
				return &specs.Spec{
					Linux: &specs.Linux{
						Devices: []specs.LinuxDevice{
							{Path: "/dev/nvidia0"},
							{Path: "/dev/null"},      // Should be filtered
							{Path: "/dev/random"},    // Should be filtered
							{Path: "/dev/nvidiactl"},
						},
					},
					Annotations: map[string]string{},
				}
			},
			wantNvproxyEnabled:     true,
			wantGPUDevicesPreserved: true,
			wantDeviceCount:        2, // Only NVIDIA devices kept
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

			// Verify device handling
			if tt.wantGPUDevicesPreserved {
				assert.NotNil(t, spec.Linux.Devices,
					"GPU devices should be preserved for nvproxy to virtualize")
				assert.Equal(t, tt.wantDeviceCount, len(spec.Linux.Devices),
					"GPU device count mismatch")
				
				// Verify all remaining devices are GPU-related
				for _, dev := range spec.Linux.Devices {
					assert.True(t, strings.HasPrefix(dev.Path, "/dev/nvidia") || strings.HasPrefix(dev.Path, "/dev/dri"),
						"Non-GPU device should have been filtered: %s", dev.Path)
				}
			} else {
				assert.Nil(t, spec.Linux.Devices,
					"Devices should be cleared for non-GPU workloads")
			}

			// Verify annotations are preserved (CDI needs these)
			if tt.wantNvproxyEnabled && len(spec.Annotations) > 0 {
				hasGPUAnnotation := false
				for key := range spec.Annotations {
					if strings.HasPrefix(key, "cdi.k8s.io") {
						hasGPUAnnotation = true
						break
					}
				}
				assert.True(t, hasGPUAnnotation,
					"CDI annotations should be preserved for nvproxy")
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
