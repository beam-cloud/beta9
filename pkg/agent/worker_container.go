package agent

import (
	"encoding/json"
	"fmt"
	"os"
	pathpkg "path"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func dockerRunArgs(name, image, configPath string, bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot, dirs map[string]string) []string {
	localTargetHost := firstNonEmpty(os.Getenv("BEAM_AGENT_LOCAL_TARGET_HOST"), "127.0.0.1")
	args := []string{
		"run", "--rm",
		"--name", name,
		"--privileged",
		"--network", "host",
		"--cgroupns", "host",
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
			args = append(args, "--gpus", "all")
		}
	}

	volumeArgs := []string{
		dirs["images"] + ":" + agentContainerImagesPath,
		dirs["tmp"] + ":" + agentContainerTmpPath,
		dirs["data"] + ":" + agentContainerDataPath,
		dirs["workspace"] + ":" + agentContainerWorkspaceStoragePath,
		dirs["cache"] + ":" + agentContainerCachePath,
		dirs["checkpoints"] + ":" + agentContainerCheckpointPath,
		dirs["logs"] + ":" + agentContainerLogsPath,
		configPath + ":" + agentContainerConfigPath + ":ro",
	}
	if pathExists("/var/lib/kubelet/device-plugins") {
		volumeArgs = append(volumeArgs, "/var/lib/kubelet/device-plugins:/var/lib/kubelet/device-plugins:ro")
	}
	if pathExists("/var/run/netns") {
		volumeArgs = append(volumeArgs, "/var/run/netns:/var/run/netns")
	}
	if pathExists("/sys/fs/cgroup") {
		volumeArgs = append(volumeArgs, "/sys/fs/cgroup:/sys/fs/cgroup:rw")
	}
	for _, volume := range volumeArgs {
		args = append(args, "-v", volume)
	}
	if pathExists("/dev/fuse") {
		args = append(args, "--device", "/dev/fuse")
	}

	env := map[string]string{
		"CONFIG_PATH":                       agentContainerConfigPath,
		types.WorkerEnvID:                   slot.WorkerId,
		types.WorkerEnvToken:                slot.WorkerToken,
		types.WorkerEnvPoolName:             slot.PoolName,
		types.WorkerEnvMachineID:            slot.MachineId,
		"CPU_LIMIT":                         strconv.FormatInt(slot.Cpu, 10),
		"MEMORY_LIMIT":                      strconv.FormatInt(slot.Memory, 10),
		"GPU_TYPE":                          slot.Gpu,
		"GPU_COUNT":                         strconv.FormatUint(uint64(slot.GpuCount), 10),
		"POD_HOSTNAME":                      "127.0.0.1",
		"POD_IP":                            "127.0.0.1",
		"NETWORK_PREFIX":                    slot.NetworkPrefix,
		"CACHE_LOCALITY":                    slot.PoolName,
		"CACHE_NODE_ID":                     slot.MachineId,
		"CACHE_HOST_NETWORK":                "true",
		types.WorkerEnvPersistent:           "true",
		types.WorkerEnvRouteTransport:       normalizeTransport(bootstrap.Transport),
		types.WorkerEnvRouteLocalTargetHost: localTargetHost,
		"BEAM_GATEWAY_HTTP_URL":             strings.TrimRight(bootstrap.GatewayHTTPURL, "/"),
	}
	if slot.ContainerStartConcurrency > 0 {
		env["WORKER_CONTAINER_START_CONCURRENCY"] = strconv.FormatUint(uint64(slot.ContainerStartConcurrency), 10)
	}
	if slot.NetworkSlotPoolSize > 0 {
		env["CONTAINER_NETWORK_SLOT_POOL_SIZE"] = strconv.FormatUint(uint64(slot.NetworkSlotPoolSize), 10)
	}
	for key, value := range agentGatewayEnv(bootstrap) {
		env[key] = value
	}
	if slot.GpuCount > 0 && slot.GpuAssignment != "" {
		env["NVIDIA_VISIBLE_DEVICES"] = slot.GpuAssignment
	} else if slot.GpuCount > 0 {
		env["NVIDIA_VISIBLE_DEVICES"] = "all"
	}
	for key, value := range env {
		args = append(args, "-e", key+"="+value)
	}

	args = append(args, image, "/usr/local/bin/worker")
	return args
}

func writeWorkerConfig(path string, bootstrap bootstrapConfig, slot *pb.AgentWorkerSlot, dirs map[string]string) error {
	workspaceStorageMode := firstNonEmpty(os.Getenv("BEAM_AGENT_WORKSPACE_STORAGE_MODE"), storage.StorageModeGeese)
	cacheDir := pathpkg.Join(agentContainerCachePath, sanitizeDockerName(slot.PoolName), sanitizeDockerName(slot.MachineId))
	httpHost, httpPort, httpTLS := agentGatewayHTTPParts(bootstrap)
	config := map[string]any{
		"clusterName": "agent",
		"debugMode":   false,
		"prettyLogs":  true,
		"gateway": map[string]any{
			"grpc": map[string]any{
				"externalHost": bootstrap.GatewayGRPCHost,
				"externalPort": bootstrap.GatewayGRPCPort,
				"tls":          bootstrap.GatewayGRPCTLS,
			},
			"http": map[string]any{
				"externalHost": httpHost,
				"externalPort": httpPort,
				"tls":          httpTLS,
			},
		},
		"storage": map[string]any{
			"mode":       storage.StorageModeLocal,
			"fsName":     "agent",
			"fsPath":     agentContainerDataPath,
			"objectPath": pathpkg.Join(agentContainerDataPath, "objects"),
			"workspaceStorage": map[string]any{
				"baseMountPath":      agentContainerWorkspaceStoragePath,
				"defaultStorageMode": workspaceStorageMode,
			},
		},
		"monitoring": map[string]any{
			"metricsCollector":         string(types.MetricsCollectorNone),
			"containerMetricsInterval": "3s",
			"prometheus": map[string]any{
				"scrapeWorkers": false,
				"port":          0,
			},
		},
		"worker": map[string]any{
			"hostNetwork":                true,
			"useHostResolvConf":          true,
			"containerRuntime":           "runc",
			"cacheEnabled":               true,
			"terminationGracePeriod":     30,
			"containerLogLinesPerHour":   6000,
			"defaultWorkerCPURequest":    slot.Cpu,
			"defaultWorkerMemoryRequest": slot.Memory,
			"failover": map[string]any{
				"maxSchedulingLatencyMs": 300000,
			},
			"pools": map[string]any{
				slot.PoolName: map[string]any{
					"mode":                      string(types.PoolModePrivate),
					"gpuType":                   slot.Gpu,
					"containerRuntime":          "runc",
					"containerStartConcurrency": int(slot.ContainerStartConcurrency),
					"networkPreallocation":      true,
					"networkSlotPoolSize":       int(slot.NetworkSlotPoolSize),
					"requiresPoolSelector":      true,
					"priority":                  1000,
					"criuEnabled":               false,
					"tmpSizeLimit":              "30Gi",
					"storageMode":               workspaceStorageMode,
					"checkpointPath":            agentContainerCheckpointPath,
					"cache": map[string]any{
						"enabled": true,
						"disk": map[string]any{
							"enabled":     true,
							"hostPath":    agentContainerCachePath,
							"mountPath":   agentContainerCachePath,
							"maxUsagePct": 0.95,
						},
					},
				},
			},
		},
		"cache": map[string]any{
			"enabled": true,
			"disk": map[string]any{
				"enabled":     true,
				"hostPath":    agentContainerCachePath,
				"mountPath":   agentContainerCachePath,
				"maxUsagePct": 0.95,
			},
			"memory": map[string]any{
				"enabled": false,
			},
			"global": map[string]any{
				"defaultLocality": firstNonEmpty(slot.PoolName, "agent"),
			},
			"server": map[string]any{
				"diskCacheDir": cacheDir,
			},
			"client": map[string]any{
				"cachefs": map[string]any{
					"enabled":    true,
					"mountPoint": agentContainerCacheFSMountPath,
				},
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}
