package agent

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func setNonVFIOEnv(t *testing.T, mode string) {
	t.Helper()
	value := ""
	if mode == "whitespace" {
		value = " \t "
	}
	for _, key := range []string{types.MicroVMVFIODevicesEnv, types.MicroVMGPUTypeEnv} {
		t.Setenv(key, value)
		if mode == "absent" || (mode == "devices-empty" && key == types.MicroVMGPUTypeEnv) || (mode == "model-empty" && key == types.MicroVMVFIODevicesEnv) {
			require.NoError(t, os.Unsetenv(key))
		}
	}
}

func TestAgentGPUDiscoveryWithoutVFIO(t *testing.T) {
	for _, mode := range []string{"absent", "empty", "whitespace", "devices-empty", "model-empty"} {
		for _, gpu := range []bool{false, true} {
			t.Run(mode+"/gpu="+strconv.FormatBool(gpu), func(t *testing.T) {
				setNonVFIOEnv(t, mode)
				bin := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(bin, "nvidia-ctk"), []byte("#!/bin/sh\nexit 0\n"), 0755))
				t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
				previousQuery, previousProbe, previousProcRoot := queryNvidiaGPUDevices, cudaDriverInitProbe, nvidiaProcGPUInfoRoot
				t.Cleanup(func() {
					queryNvidiaGPUDevices, cudaDriverInitProbe, nvidiaProcGPUInfoRoot = previousQuery, previousProbe, previousProcRoot
				})
				nvidiaProcGPUInfoRoot = filepath.Join(t.TempDir(), "absent")
				queries, probes := 0, 0
				queryNvidiaGPUDevices = func() ([]byte, error) {
					queries++
					if gpu {
						return []byte("0, GPU-legacy, NVIDIA GeForce RTX 4090, 0x0000, 00000000:01:00.0"), nil
					}
					return nil, errors.New("nvidia-smi is unavailable on this CPU host")
				}
				cudaDriverInitProbe = func() (bool, string, bool) {
					probes++
					return true, "normal host CUDA probe", true
				}

				preflight := runPreflight(true, "native")
				capacity, _, ready := resolveAgentCapacity(types.AgentJoinOptions{}, preflight)
				require.True(t, ready, "%+v", preflight.checks)
				require.Equal(t, 1, queries, "non-VFIO agents must retain NVIDIA discovery")
				for _, check := range preflight.checks {
					require.NotEqual(t, "vfio-gpus", check.Name)
				}
				if !gpu {
					require.Zero(t, capacity.GPUCount)
					require.Empty(t, capacity.GPUIDs)
					require.Zero(t, probes)
					return
				}
				require.Equal(t, uint32(1), capacity.GPUCount)
				require.Equal(t, []string{"RTX4090"}, capacity.GPUs)
				require.Equal(t, []string{"0"}, capacity.GPUIDs)
				require.Equal(t, 1, probes)
				for _, name := range []string{"nvidia-runtime", "nvidia-driver-state", "cuda-driver-init"} {
					require.True(t, slices.ContainsFunc(preflight.checks, func(check check) bool { return check.Name == name && check.Ok }), name)
				}
				selected, _, ready := resolveAgentCapacity(types.AgentJoinOptions{GPUIDs: "GPU-legacy"}, preflight)
				require.True(t, ready)
				require.Equal(t, []string{"GPU-legacy"}, selected.GPUIDs)
			})
		}
	}
}

func TestAgentWorkerOptionsWithoutVFIO(t *testing.T) {
	for _, mode := range []string{"absent", "empty", "whitespace", "devices-empty", "model-empty"} {
		t.Run(mode, func(t *testing.T) {
			setNonVFIOEnv(t, mode)
			for _, test := range []struct {
				name, runtime, mode, assignment string
				count                           uint32
				virtualized                     bool
				wantError                       string
			}{
				{name: "default CPU"},
				{name: "runc CPU", runtime: "runc"},
				{name: "gvisor CPU", runtime: "gvisor"},
				{name: "microvm CPU", runtime: "microvm"},
				{name: "microvm GPU without inventory", runtime: "microvm", count: 1, wantError: "configured VFIO inventory"},
				{name: "default NVIDIA", count: 1},
				{name: "runc assigned NVIDIA", runtime: "runc", assignment: "0,1", count: 2},
				{name: "gvisor assigned NVIDIA", runtime: "gvisor", assignment: "GPU-legacy", count: 1},
				{name: "managed NVIDIA", runtime: "runc", mode: string(types.PoolModeExternal), assignment: "0", count: 1, virtualized: true},
				{name: "provider NVIDIA", runtime: "gvisor", mode: string(types.PoolModeProvider), assignment: "GPU-legacy", count: 1},
			} {
				t.Run(test.name, func(t *testing.T) {
					slot := &pb.AgentWorkerSlot{PoolName: "regular", ContainerRuntime: test.runtime, Mode: test.mode, GpuCount: test.count, GpuAssignment: test.assignment, PoolConfig: &pb.AgentPoolRuntimeConfig{GpuVirtualized: test.virtualized}}
					args, err := dockerRunArgs("worker", "worker:stable", "", "/tmp/config.json", bootstrapConfig{}, slot, workerDirs{}, workerContainerResourceLimits{})
					if test.wantError != "" {
						require.ErrorContains(t, err, test.wantError)
						return
					}
					require.NoError(t, err)
					require.True(t, containsArg(args, "-e", types.WorkerGPUCountEnv+"="+strconv.Itoa(int(test.count))))
					for _, key := range []string{types.MicroVMVFIODevicesEnv, types.MicroVMGPUTypeEnv} {
						for _, arg := range args {
							if strings.HasPrefix(arg, key+"=") {
								require.Equal(t, key+"=", arg, "normal workers must not inherit VFIO inventory")
							}
						}
					}
					if test.count == 0 {
						require.NotContains(t, args, "--gpus")
						for _, arg := range args {
							if strings.HasPrefix(arg, types.NvidiaVisibleDevicesEnv+"=") {
								require.Equal(t, types.NvidiaVisibleDevicesEnv+"=void", arg)
							}
						}
						if test.runtime == "microvm" {
							for _, key := range []string{types.MicroVMVFIODevicesEnv, types.MicroVMGPUTypeEnv, types.WorkerGPUDevicesEnv} {
								require.True(t, containsArg(args, "-e", key+"="))
							}
							require.True(t, containsArg(args, "-e", types.NvidiaVisibleDevicesEnv+"=void"))
						}
						return
					}
					assignment, option := test.assignment, types.NvidiaVisibleDevicesAll
					if assignment == "" {
						assignment = types.NvidiaVisibleDevicesAll
					} else {
						option = strconv.Quote("device=" + assignment)
					}
					require.True(t, containsArg(args, "--gpus", option))
					require.True(t, containsArg(args, "-e", types.NvidiaVisibleDevicesEnv+"="+assignment))
					require.True(t, containsArg(args, "-e", types.WorkerGPUDevicesEnv+"="+assignment))
					require.True(t, containsArg(args, "-e", types.WorkerGPUVirtualizedEnv+"="+strconv.FormatBool(test.virtualized)))
				})
			}
		})
	}
}
