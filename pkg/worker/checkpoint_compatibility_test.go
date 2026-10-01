package worker

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	cdispecs "tags.cncf.io/container-device-interface/specs-go"
)

const checkpointTestCPU = "vendor_id: AuthenticAMD\ncpu family: 25\nmodel: 1\nstepping: 1\nmodel name: AMD EPYC 7B13\nflags: fpu fxsr xsave xsaves avx avx2\n"

func TestCheckpointHostProfile(t *testing.T) {
	cpu, err := checkpointCPUProfile(checkpointTestCPU)
	require.NoError(t, err)
	gpu, err := checkpointGPUProfile("NVIDIA GeForce RTX 4090, 580.126.18\n")
	require.NoError(t, err)
	p := checkpointHostProfile{CPU: cpu, GPU: gpu, Binaries: map[string]string{"worker": "worker-a", "runsc": "runtime-a"}}
	key := p.key()
	// CPU numbering/count and flag enumeration order are not capabilities.
	reordered := strings.Replace(checkpointTestCPU, "fpu fxsr xsave xsaves avx avx2", "avx2 avx xsaves xsave fxsr fpu", 1)
	p.CPU, err = checkpointCPUProfile("processor: 5\n" + reordered + "\nprocessor: 2\n" + reordered)
	require.NoError(t, err)
	p.GPU, err = checkpointGPUProfile(" NVIDIA GeForce RTX 4090 , 580.126.18\nNVIDIA GeForce RTX 4090,580.126.18\n")
	require.NoError(t, err)
	require.Equal(t, key, p.key())

	for name, change := range map[string]func(*checkpointHostProfile){
		"CPU features": func(p *checkpointHostProfile) {
			p.CPU, _ = checkpointCPUProfile(strings.Replace(checkpointTestCPU, "xsaves ", "", 1))
		},
		"CPU model": func(p *checkpointHostProfile) {
			p.CPU, _ = checkpointCPUProfile(strings.Replace(checkpointTestCPU, "model: 1", "model: 2", 1))
		},
		"driver":       func(p *checkpointHostProfile) { p.GPU, _ = checkpointGPUProfile("NVIDIA GeForce RTX 4090, 595.99.02") },
		"physical GPU": func(p *checkpointHostProfile) { p.GPU, _ = checkpointGPUProfile("NVIDIA A10G, 580.126.18") },
		"runtime build": func(p *checkpointHostProfile) {
			p.Binaries = map[string]string{"worker": "worker-a", "runsc": "runtime-b"}
		},
		"worker build": func(p *checkpointHostProfile) {
			p.Binaries = map[string]string{"worker": "worker-b", "runsc": "runtime-a"}
		},
		"worker image":  func(p *checkpointHostProfile) { p.WorkerImage = "sdk-update" },
		"worker config": func(p *checkpointHostProfile) { p.ResourceLimits.MemoryEnforced = true },
		"runtime":       func(p *checkpointHostProfile) { p.Runtime = "gvisor" },
		"platform":      func(p *checkpointHostProfile) { p.Platform = "kvm" },
		"runtime flags": func(p *checkpointHostProfile) { p.RunscArgs = []string{"--file-access=shared"} },
		"mounts":        func(p *checkpointHostProfile) { p.Mounts = []string{"/usr/lib/libcuda.so.595"} },
	} {
		t.Run(name, func(t *testing.T) {
			changed := p
			change(&changed)
			require.NotEqual(t, key, changed.key())
		})
	}
	_, err = checkpointCPUProfile("model name: unknown")
	require.Error(t, err)
	_, err = checkpointGPUProfile("")
	require.Error(t, err)
}

func TestCheckpointCDIMountProfile(t *testing.T) {
	mount := &cdispecs.Mount{HostPath: "/driver/libcuda", ContainerPath: "/usr/lib/libcuda", Type: "bind", Options: []string{"ro", "rbind"}}
	spec := &cdispecs.Spec{ContainerEdits: cdispecs.ContainerEdits{Mounts: []*cdispecs.Mount{mount}}}
	original := checkpointCDIMounts(spec)
	mount.HostPath = "/another-host/libcuda"
	mount.Options = []string{"rbind", "ro"}
	spec.Devices = []cdispecs.Device{{Name: "GPU-another-uuid", ContainerEdits: spec.ContainerEdits}}
	require.Equal(t, original, checkpointCDIMounts(spec))
	for _, change := range []func(*cdispecs.Mount){
		func(m *cdispecs.Mount) { m.ContainerPath = "/usr/lib/libcuda.so.595" },
		func(m *cdispecs.Mount) { m.Type = "tmpfs" },
		func(m *cdispecs.Mount) { m.Options = []string{"rw", "rbind"} },
	} {
		changed := *mount
		change(&changed)
		require.NotEqual(t, original, checkpointCDIMounts(&cdispecs.Spec{ContainerEdits: cdispecs.ContainerEdits{Mounts: []*cdispecs.Mount{&changed}}}))
	}
}

func TestCheckpointDiscoveryFailureIsProcessSpecific(t *testing.T) {
	t.Setenv("PATH", "")
	first, second := &Worker{}, &Worker{}
	first.initializeCheckpointCompatibility()
	second.initializeCheckpointCompatibility()
	require.True(t, strings.HasPrefix(first.checkpointCompatibilityKey, "unknown:"))
	require.True(t, strings.HasPrefix(second.checkpointCompatibilityKey, "unknown:"))
	require.NotEqual(t, first.checkpointCompatibilityKey, second.checkpointCompatibilityKey)
}
