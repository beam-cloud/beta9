package agent

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func dockerRunArgs(name, image, configPath string, bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot, dirs workerDirs) []string {
	localTargetHost := firstNonEmpty(os.Getenv(types.AgentTargetHostEnv), types.LoopbackHost)
	args := []string{
		"run", "--rm",
		"--name", name,
		"--privileged",
		"--network", "host",
		"--cgroupns", "host",
		"--label", types.AgentDockerLabelManaged + "=true",
		"--label", types.AgentDockerLabelWorkerID + "=" + slot.WorkerId,
		"--label", types.AgentDockerLabelMachineID + "=" + slot.MachineId,
		"--label", types.AgentDockerLabelPoolName + "=" + slot.PoolName,
	}
	if platform := strings.TrimSpace(os.Getenv(types.AgentWorkerPlatformEnv)); platform != "" {
		args = append(args, "--platform", platform)
	}
	for _, alias := range agentDockerHostAliases() {
		args = append(args, "--add-host", alias)
	}

	if slot.Cpu > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%.3f", float64(slot.Cpu)/1000.0))
	}
	if slot.Memory > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dm", slot.Memory))
		args = append(args, "--shm-size", fmt.Sprintf("%dm", max(slot.Memory/2, 64)))
	}
	if slot.GpuCount > 0 {
		if slot.GpuAssignment != "" {
			args = append(args, "--gpus", "device="+slot.GpuAssignment)
		} else {
			args = append(args, "--gpus", types.NvidiaVisibleDevicesAll)
		}
	}

	volumeArgs := []string{
		dirs.Images + ":" + types.AgentImagesPath,
		dirs.Tmp + ":" + types.AgentTmpPath,
		dirs.Data + ":" + types.AgentDataPath,
		dirs.Workspace + ":" + types.AgentWorkspacePath,
		dirs.Cache + ":" + types.AgentCachePath,
		dirs.Checkpoints + ":" + types.AgentCheckpointPath,
		dirs.Logs + ":" + types.AgentLogsPath,
		configPath + ":" + types.AgentConfigPath + ":ro",
	}
	if pathExists(types.HostKubeletDevicePluginsPath) {
		volumeArgs = append(volumeArgs, types.HostKubeletDevicePluginsPath+":"+types.HostKubeletDevicePluginsPath+":ro")
	}
	if pathExists(types.HostNetnsPath) {
		volumeArgs = append(volumeArgs, types.HostNetnsPath+":"+types.HostNetnsPath)
	}
	if pathExists(types.HostCgroupPath) {
		volumeArgs = append(volumeArgs, types.HostCgroupPath+":"+types.HostCgroupPath+":rw")
	}
	for _, volume := range volumeArgs {
		args = append(args, "-v", volume)
	}
	if pathExists(types.HostFuseDevicePath) {
		args = append(args, "--device", types.HostFuseDevicePath)
	}

	env := map[string]string{
		types.WorkerConfigPathEnv:     types.AgentConfigPath,
		types.WorkerIDEnv:             slot.WorkerId,
		types.WorkerTokenEnv:          slot.WorkerToken,
		types.WorkerPoolEnv:           slot.PoolName,
		types.WorkerMachineEnv:        slot.MachineId,
		types.WorkerCPUEnv:            strconv.FormatInt(slot.Cpu, 10),
		types.WorkerMemoryEnv:         strconv.FormatInt(slot.Memory, 10),
		types.WorkerGPUEnv:            slot.Gpu,
		types.WorkerGPUCountEnv:       strconv.FormatUint(uint64(slot.GpuCount), 10),
		types.WorkerPodHostEnv:        types.LoopbackHost,
		types.WorkerPodIPEnv:          types.LoopbackHost,
		types.WorkerNetworkPrefixEnv:  slot.NetworkPrefix,
		types.CacheLocalityEnv:        slot.PoolName,
		types.CacheNodeEnv:            slot.MachineId,
		types.CacheHostNetworkEnv:     "true",
		types.WorkerPersistentEnv:     "true",
		types.WorkerRouteTransportEnv: normalizeTransport(bootstrap.Transport),
		types.WorkerRouteTargetEnv:    localTargetHost,
		types.AgentGatewayURLEnv:      strings.TrimRight(bootstrap.GatewayHTTPURL, "/"),
	}
	if slot.ContainerStartConcurrency > 0 {
		env[types.WorkerStartConcurrencyEnv] = strconv.FormatUint(uint64(slot.ContainerStartConcurrency), 10)
	}
	if slot.NetworkSlotPoolSize > 0 {
		env[types.WorkerNetworkSlotsEnv] = strconv.FormatUint(uint64(slot.NetworkSlotPoolSize), 10)
	}
	for key, value := range agentGatewayEnv(bootstrap) {
		env[key] = value
	}
	if slot.GpuCount > 0 && slot.GpuAssignment != "" {
		env[types.NvidiaVisibleDevicesEnv] = slot.GpuAssignment
	} else if slot.GpuCount > 0 {
		env[types.NvidiaVisibleDevicesEnv] = types.NvidiaVisibleDevicesAll
	}
	envKeys := make([]string, 0, len(env))
	for key := range env {
		envKeys = append(envKeys, key)
	}
	sort.Strings(envKeys)
	for _, key := range envKeys {
		value := env[key]
		args = append(args, "-e", key+"="+value)
	}

	args = append(args, image, types.AgentWorkerEntrypoint)
	return args
}
