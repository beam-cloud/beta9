package worker

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
)

func TestInjectCudaEnvVarsNoCudaInImage(t *testing.T) {
	manager := NewContainerCudaManager()
	initialEnv := []string{"INITIAL=1"}

	// Set some environment variables to simulate NVIDIA settings
	os.Setenv("NVIDIA_DRIVER_CAPABILITIES", "all")
	os.Setenv("NVIDIA_REQUIRE_CUDA", "cuda>=9.0")

	expectedEnv := []string{
		"INITIAL=1",
		"NVIDIA_DRIVER_CAPABILITIES=all",
		"NVIDIA_REQUIRE_CUDA=cuda>=9.0",
		"NVARCH=",
		"NV_CUDA_COMPAT_PACKAGE=",
		"NV_CUDA_CUDART_VERSION=",
		"NVIDIA_VISIBLE_DEVICES=",
		"CUDA_VERSION=",
		"GPU_TYPE=",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/cuda-12.3/bin:$PATH",
		"LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/usr/lib/worker/x86_64-linux-gnu:/usr/local/nvidia/lib64:/usr/local/cuda-12.3/targets/x86_64-linux/lib:$LD_LIBRARY_PATH",
	}

	resultEnv, _ := manager.InjectCudaEnvVars(initialEnv, &ContainerOptions{
		InitialSpec: &specs.Spec{
			Process: &specs.Process{},
		},
	})
	if !reflect.DeepEqual(expectedEnv, resultEnv) {
		t.Errorf("Expected %v, got %v", expectedEnv, resultEnv)
	}
}

func TestInjectCudaEnvVarsExistingCudaInImage(t *testing.T) {
	manager := NewContainerCudaManager()
	initialEnv := []string{"INITIAL=1"}

	// Set some environment variables to simulate NVIDIA settings
	os.Setenv("NVIDIA_DRIVER_CAPABILITIES", "all")
	os.Setenv("NVIDIA_REQUIRE_CUDA", "cuda>=9.0")
	os.Setenv("CUDA_VERSION", "12.3")

	expectedEnv := []string{
		"INITIAL=1",
		"NVIDIA_REQUIRE_CUDA=",
		"CUDA_VERSION=11.8.2",
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/cuda-11.8/bin:$PATH",
		"LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/usr/lib/worker/x86_64-linux-gnu:/usr/local/nvidia/lib64:/usr/local/cuda-11.8/targets/x86_64-linux/lib:$LD_LIBRARY_PATH",
	}

	resultEnv, _ := manager.InjectCudaEnvVars(initialEnv, &ContainerOptions{
		InitialSpec: &specs.Spec{
			Process: &specs.Process{Env: []string{"NVIDIA_REQUIRE_CUDA=", "CUDA_VERSION=11.8.2"}},
		},
	})

	expectedEnvStr := strings.Join(expectedEnv, "")
	resultEnvStr := strings.Join(resultEnv, "")

	expectedEnvStr = strings.ReplaceAll(expectedEnvStr, " ", "")
	resultEnvStr = strings.ReplaceAll(resultEnvStr, " ", "")

	if expectedEnvStr != resultEnvStr {
		t.Errorf("Expected %v, got %v", expectedEnv, resultEnv)
	}
}

func TestInjectCudaMounts(t *testing.T) {
	manager := NewContainerCudaManager()
	initialMounts := []specs.Mount{{Type: "bind", Source: "/src", Destination: "/dst"}}

	resultMounts := manager.InjectCudaMounts(initialMounts)
	if len(resultMounts) != len(initialMounts) {
		t.Errorf("Expected %d mounts, got %d", len(initialMounts)+2, len(resultMounts))
	}
}
