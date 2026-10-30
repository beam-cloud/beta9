package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func vfioFixture(t *testing.T, count int) []string {
	t.Helper()
	root := t.TempDir()
	previous := vfioPCIRoot
	vfioPCIRoot = filepath.Join(root, "pci")
	t.Cleanup(func() { vfioPCIRoot = previous })
	driver := filepath.Join(root, "drivers", "vfio-pci")
	if err := os.MkdirAll(driver, 0755); err != nil {
		t.Fatal(err)
	}
	var gpus, functions []string
	for i := range count {
		group := filepath.Join(root, "iommu_groups", fmt.Sprint(i))
		if err := os.MkdirAll(filepath.Join(group, "devices"), 0755); err != nil {
			t.Fatal(err)
		}
		for function, class := range []string{"0x030000", "0x040300"} {
			device := fmt.Sprintf("0000:%02x:00.%d", i+1, function)
			path := filepath.Join(vfioPCIRoot, device)
			if err := os.MkdirAll(path, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, "class"), []byte(class), 0644); err != nil {
				t.Fatal(err)
			}
			for link, target := range map[string]string{
				filepath.Join(path, "driver"):           driver,
				filepath.Join(path, "iommu_group"):      group,
				filepath.Join(group, "devices", device): path,
			} {
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			}
			functions = append(functions, device)
			if function == 0 {
				gpus = append(gpus, device)
			}
		}
	}
	t.Setenv(types.MicroVMGPUTypeEnv, "NVIDIA GeForce RTX 4090")
	t.Setenv(types.MicroVMVFIODevicesEnv, strings.Join(functions, ","))
	return gpus
}

func TestVFIOPreflightAccountsForPhysicalGroupsWithoutNvidia(t *testing.T) {
	gpus := vfioFixture(t, 8)
	previous := queryNvidiaGPUDevices
	queryNvidiaGPUDevices = func() ([]byte, error) { panic("VFIO preflight must not query nvidia-smi") }
	t.Cleanup(func() { queryNvidiaGPUDevices = previous })
	preflight := runPreflight(true, "native")
	capacity, checks, schedulable := resolveAgentCapacity(types.AgentJoinOptions{}, preflight)
	if !schedulable || capacity.GPUCount != 8 || !reflect.DeepEqual(capacity.GPUIDs, gpus) {
		t.Fatalf("VFIO capacity = %#v, checks = %#v", capacity, checks)
	}
	for _, gpu := range capacity.GPUs {
		if gpu != types.GPU_RTX4090.String() {
			t.Fatalf("GPU model = %q", gpu)
		}
	}
	for _, check := range checks {
		if strings.HasPrefix(check.Name, "nvidia-") || check.Name == "cuda-driver-init" {
			t.Fatalf("host NVIDIA check in VFIO preflight: %#v", check)
		}
	}
	limited, _, ok := resolveAgentCapacity(types.AgentJoinOptions{GPUIDs: gpus[3]}, preflight)
	if !ok || limited.GPUCount != 1 || !reflect.DeepEqual(limited.GPUIDs, []string{gpus[3]}) {
		t.Fatalf("selected VFIO capacity = %#v", limited)
	}
}

func TestVFIOPreflightFailsClosed(t *testing.T) {
	for _, name := range []string{"missing model", "unknown model", "any model", "no GPU model", "empty inventory", "invalid address", "duplicate address", "missing driver", "missing group", "invalid class", "missing GPU", "two GPUs", "incomplete group"} {
		t.Run(name, func(t *testing.T) {
			gpus := vfioFixture(t, 1)
			gpuPath := filepath.Join(vfioPCIRoot, gpus[0])
			class := filepath.Join(gpuPath, "class")
			switch name {
			case "missing model":
				t.Setenv(types.MicroVMGPUTypeEnv, "")
			case "unknown model":
				t.Setenv(types.MicroVMGPUTypeEnv, "unsupported")
			case "any model":
				t.Setenv(types.MicroVMGPUTypeEnv, "any")
			case "no GPU model":
				t.Setenv(types.MicroVMGPUTypeEnv, "none")
			case "empty inventory":
				t.Setenv(types.MicroVMVFIODevicesEnv, "")
			case "invalid address":
				t.Setenv(types.MicroVMVFIODevicesEnv, "../../elsewhere")
			case "duplicate address":
				t.Setenv(types.MicroVMVFIODevicesEnv, os.Getenv(types.MicroVMVFIODevicesEnv)+","+gpus[0])
			case "missing driver":
				if err := os.Remove(filepath.Join(gpuPath, "driver")); err != nil {
					t.Fatal(err)
				}
			case "missing group":
				if err := os.Remove(filepath.Join(gpuPath, "iommu_group")); err != nil {
					t.Fatal(err)
				}
			case "invalid class":
				if err := os.WriteFile(class, []byte("invalid"), 0644); err != nil {
					t.Fatal(err)
				}
			case "missing GPU":
				if err := os.WriteFile(class, []byte("0x040300"), 0644); err != nil {
					t.Fatal(err)
				}
			case "two GPUs":
				if err := os.WriteFile(filepath.Join(vfioPCIRoot, "0000:01:00.1", "class"), []byte("0x030000"), 0644); err != nil {
					t.Fatal(err)
				}
			case "incomplete group":
				t.Setenv(types.MicroVMVFIODevicesEnv, gpus[0])
			}
			previous := queryNvidiaGPUDevices
			queryNvidiaGPUDevices = func() ([]byte, error) { panic("invalid VFIO inventory must not fall back to nvidia-smi") }
			t.Cleanup(func() { queryNvidiaGPUDevices = previous })
			preflight := runPreflight(true, "native")
			capacity, _, ok := resolveAgentCapacity(types.AgentJoinOptions{}, preflight)
			if ok || capacity.GPUCount != 0 {
				t.Fatalf("invalid VFIO inventory was schedulable: %#v", preflight)
			}
		})
	}
}

func TestDockerMicroVMGPUAssignmentUsesOnlyAuthorizedFunctions(t *testing.T) {
	gpus := vfioFixture(t, 2)
	slot := &pb.AgentWorkerSlot{ContainerRuntime: types.ContainerRuntimeMicroVM.String(), Gpu: "RTX4090", GpuCount: 1, GpuAssignment: gpus[1]}
	args, err := dockerRunArgs("worker", "worker:dev", "", "/tmp/config.json", bootstrapConfig{}, slot, workerDirs{}, workerContainerResourceLimits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range []string{
		types.MicroVMVFIODevicesEnv + "=0000:02:00.0,0000:02:00.1",
		types.MicroVMGPUTypeEnv + "=RTX4090",
		types.WorkerGPUDevicesEnv + "=" + gpus[1],
		types.NvidiaVisibleDevicesEnv + "=void",
	} {
		if !containsArg(args, "-e", env) {
			t.Fatalf("missing %q: %#v", env, args)
		}
	}
	if slices.Contains(args, "--gpus") || strings.Contains(strings.Join(args, " "), "0000:01:00") {
		t.Fatalf("worker received host NVIDIA injection or unassigned functions: %#v", args)
	}
	slot.GpuCount = 0
	slot.GpuAssignment = ""
	args, err = dockerRunArgs("cpu", "worker:dev", "", "/tmp/config.json", bootstrapConfig{}, slot, workerDirs{}, workerContainerResourceLimits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{types.MicroVMVFIODevicesEnv, types.MicroVMGPUTypeEnv, types.WorkerGPUDevicesEnv} {
		if !containsArg(args, "-e", key+"=") {
			t.Fatalf("CPU worker inherited %s", key)
		}
	}
}

func TestDockerVFIOAssignmentRejectsInvalidSlots(t *testing.T) {
	gpus := vfioFixture(t, 2)
	for _, slot := range []*pb.AgentWorkerSlot{
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 1},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 1, GpuAssignment: "0000:03:00.0"},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 2, GpuAssignment: gpus[0] + "," + gpus[0]},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 2, GpuAssignment: gpus[0]},
		{ContainerRuntime: "microvm", Gpu: "H100", GpuCount: 1, GpuAssignment: gpus[0]},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 1, GpuAssignment: gpus[0], PoolConfig: &pb.AgentPoolRuntimeConfig{GpuVirtualized: true}},
		{ContainerRuntime: "runc", Gpu: "RTX4090", GpuCount: 1, GpuAssignment: gpus[0]},
	} {
		if args, err := dockerRunArgs("worker", "worker:dev", "", "/tmp/config.json", bootstrapConfig{}, slot, workerDirs{}, workerContainerResourceLimits{}); err == nil {
			t.Fatalf("invalid slot %#v produced arguments %#v", slot, args)
		}
	}
}
