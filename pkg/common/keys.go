package common

import (
	"fmt"
)

var (
	schedulerPrefix                  string = "scheduler:"
	schedulerContainerRequests       string = "scheduler:container_requests"
	schedulerWorkerLock              string = "scheduler:worker:lock:%s"
	schedulerWorkerRequests          string = "scheduler:worker:requests:%s"
	schedulerWorkerIndex             string = "scheduler:worker:worker_index"
	schedulerWorkerState             string = "scheduler:worker:state:%s"
	schedulerContainerConfig         string = "scheduler:container:config:%s"
	schedulerContainerState          string = "scheduler:container:state:%s"
	schedulerContainerAddress        string = "scheduler:container:container_addr:%s"
	schedulerContainerAddressMap     string = "scheduler:container:container_addr_map:%s"
	schedulerContainerRequestStatus  string = "scheduler:container:request_status:%s"
	schedulerContainerIndex          string = "scheduler:container:index:%s"
	schedulerContainerWorkerIndex    string = "scheduler:container:worker:index:%s"
	schedulerContainerWorkspaceIndex string = "scheduler:container:workspace:index:%s"
	schedulerWorkerAddress           string = "scheduler:container:worker_addr:%s"
	schedulerContainerLock           string = "scheduler:container:lock:%s"
	schedulerContainerExitCode       string = "scheduler:container:exit_code:%s"
	schedulerCheckpointState         string = "scheduler:checkpoint_state:%s:%s"
	schedulerServeLock               string = "scheduler:serve:lock:%s:%s"
	schedulerStubState               string = "scheduler:stub:state:%s"
)

var (
	gatewayPrefix                      string = "gateway"
	gatewayDefaultDeployment           string = "gateway:default_deployment:%s"
	gatewayDeploymentMinContainerCount string = "gateway:min_containers:%s"
	gatewayAuthKey                     string = "gateway:auth:%s:%s"
)

var (
	workerPrefix                 string = "worker"
	workerImageLock              string = "worker:%s:image:%s:lock"
	workerContainerResourceUsage string = "worker:%s:container:%s:resource_usage"
	workerNetworkLock            string = "worker:network:%s:lock"
	workerNetworkIpIndex         string = "worker:network:%s:ip_index"
	workerNetworkContainerIp     string = "worker:network:%s:container_ip:%s"
)

var (
	workerPoolPrefix      string = "workerpool"
	workerPoolState       string = "workerpool:%s:state"
	workerPoolStateLock   string = "workerpool:%s:state:lock"
	workerPoolSizerLock   string = "workerpool:%s:sizer:lock"
	workerPoolCleanerLock string = "workerpool:%s:cleaner:lock"
)

var (
	taskPrefix      string = "task"
	taskIndex       string = "task:index"
	taskIndexByStub string = "task:%s:%s:stub_index"
	taskClaimIndex  string = "task:%s:%s:claim_index"
	taskEntry       string = "task:%s:%s:%s"
	taskClaim       string = "task:%s:%s:%s:claim"
	taskCancel      string = "task:%s:%s:%s:cancel"
	taskRetryLock   string = "task:%s:%s:%s:retry_lock"
)

var (
	workspacePrefix string = "workspace"

	workspaceVolumePathDownloadToken string = "workspace:volume_path_download_token:%s"
	workspaceConcurrencyLimit        string = "workspace:concurrency_limit:%s"
	workspaceConcurrencyLimitLock    string = "workspace:concurrency_limit:lock:%s"
	workspaceAuthorizedToken         string = "workspace:authorization:token:%s"
)

var (
	providerPrefix         string = "provider"
	providerMachineState   string = "provider:machine:%s:%s:%s"
	providerMachineMetrics string = "provider:machine:%s:%s:%s:metrics"
	providerMachineIndex   string = "provider:machine:%s:%s:machine_index"
	providerMachineLock    string = "provider:machine:%s:%s:%s:lock"
)

var (
	tailscalePrefix          string = "tailscale"
	tailscaleServiceHostname string = "tailscale:%s:%s"
)

var (
	containerName string = "%s-%s-%s" // prefix, stub-id, containerId
)

var (
	imageBuildContainerTTL string = "image:build_container_ttl:%s"
)

var (
	imageChunkPriority string = "image:chunk_priority:%s"
)

var RedisKeys = &redisKeys{}

type redisKeys struct{}

// Scheduler scheduling keys
func (rk *redisKeys) SchedulerPrefix() string {
	return schedulerPrefix
}

func (rk *redisKeys) SchedulerWorkerIndex() string {
	return schedulerWorkerIndex
}

func (rk *redisKeys) SchedulerContainerRequests() string {
	return schedulerContainerRequests
}

func (rk *redisKeys) SchedulerWorkerLock(workerId string) string {
	return fmt.Sprintf(schedulerWorkerLock, workerId)
}

func (rk *redisKeys) SchedulerWorkerRequests(workerId string) string {
	return fmt.Sprintf(schedulerWorkerRequests, workerId)
}

func (rk *redisKeys) SchedulerWorkerState(workerId string) string {
	return fmt.Sprintf(schedulerWorkerState, workerId)
}

func (rk *redisKeys) SchedulerContainerLock(containerId string) string {
	return fmt.Sprintf(schedulerContainerLock, containerId)
}

func (rk *redisKeys) SchedulerServeLock(workspaceName, stubId string) string {
	return fmt.Sprintf(schedulerServeLock, workspaceName, stubId)
}

func (rk *redisKeys) SchedulerContainerState(containerId string) string {
	return fmt.Sprintf(schedulerContainerState, containerId)
}

func (rk *redisKeys) SchedulerContainerConfig(containerId string) string {
	return fmt.Sprintf(schedulerContainerConfig, containerId)
}

func (rk *redisKeys) SchedulerContainerIndex(stubId string) string {
	return fmt.Sprintf(schedulerContainerIndex, stubId)
}

func (rk *redisKeys) SchedulerContainerWorkerIndex(workerId string) string {
	return fmt.Sprintf(schedulerContainerWorkerIndex, workerId)
}

func (rk *redisKeys) SchedulerContainerWorkspaceIndex(workspaceId string) string {
	return fmt.Sprintf(schedulerContainerWorkspaceIndex, workspaceId)
}

func (rk *redisKeys) SchedulerContainerAddress(containerId string) string {
	return fmt.Sprintf(schedulerContainerAddress, containerId)
}

func (rk *redisKeys) SchedulerContainerAddressMap(containerId string) string {
	return fmt.Sprintf(schedulerContainerAddressMap, containerId)
}

func (rk *redisKeys) SchedulerContainerRequestStatus(containerId string) string {
	return fmt.Sprintf(schedulerContainerRequestStatus, containerId)
}

func (rk *redisKeys) SchedulerWorkerAddress(containerId string) string {
	return fmt.Sprintf(schedulerWorkerAddress, containerId)
}

func (rk *redisKeys) SchedulerContainerExitCode(containerId string) string {
	return fmt.Sprintf(schedulerContainerExitCode, containerId)
}

func (rk *redisKeys) SchedulerCheckpointState(workspaceName, checkpointId string) string {
	return fmt.Sprintf(schedulerCheckpointState, workspaceName, checkpointId)
}

func (rk *redisKeys) SchedulerStubState(stubId string) string {
	return fmt.Sprintf(schedulerStubState, stubId)
}

// Gateway keys
func (rk *redisKeys) GatewayPrefix() string {
	return gatewayPrefix
}

func (rk *redisKeys) GatewayDefaultDeployment(appId string) string {
	return fmt.Sprintf(gatewayDefaultDeployment, appId)
}

func (rk *redisKeys) GatewayAuthKey(appId string, encodedAuthToken string) string {
	return fmt.Sprintf(gatewayAuthKey, appId, encodedAuthToken)
}

func (rk *redisKeys) GatewayDeploymentMinContainerCount(appId string) string {
	return fmt.Sprintf(gatewayDeploymentMinContainerCount, appId)
}

// Worker keys
func (rk *redisKeys) WorkerPrefix() string {
	return workerPrefix
}

func (rk *redisKeys) WorkerContainerResourceUsage(workerId string, containerId string) string {
	return fmt.Sprintf(workerContainerResourceUsage, workerId, containerId)
}

func (rk *redisKeys) WorkerImageLock(workerId string, imageId string) string {
	return fmt.Sprintf(workerImageLock, workerId, imageId)
}

func (rk *redisKeys) WorkerNetworkLock(networkPrefix string) string {
	return fmt.Sprintf(workerNetworkLock, networkPrefix)
}

func (rk *redisKeys) WorkerNetworkIpIndex(networkPrefix string) string {
	return fmt.Sprintf(workerNetworkIpIndex, networkPrefix)
}

func (rk *redisKeys) WorkerNetworkContainerIp(networkPrefix, containerId string) string {
	return fmt.Sprintf(workerNetworkContainerIp, networkPrefix, containerId)
}

// Worker Pool keys
func (rk *redisKeys) WorkerPoolPrefix() string {
	return workerPoolPrefix
}

func (rk *redisKeys) WorkerPoolState(poolName string) string {
	return fmt.Sprintf(workerPoolState, poolName)
}

func (rk *redisKeys) WorkerPoolStateLock(poolName string) string {
	return fmt.Sprintf(workerPoolStateLock, poolName)
}

func (rk *redisKeys) WorkerPoolSizerLock(poolName string) string {
	return fmt.Sprintf(workerPoolSizerLock, poolName)
}

func (rk *redisKeys) WorkerPoolCleanerLock(poolName string) string {
	return fmt.Sprintf(workerPoolCleanerLock, poolName)
}

// Task keys
func (rk *redisKeys) TaskPrefix() string {
	return taskPrefix
}

func (rk *redisKeys) TaskIndex() string {
	return taskIndex
}

func (rk *redisKeys) TaskCancel(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskCancel, workspaceName, stubId, taskId)
}

func (rk *redisKeys) TaskIndexByStub(workspaceName, stubId string) string {
	return fmt.Sprintf(taskIndexByStub, workspaceName, stubId)
}

func (rk *redisKeys) TaskClaimIndex(workspaceName, stubId string) string {
	return fmt.Sprintf(taskClaimIndex, workspaceName, stubId)
}

func (rk *redisKeys) TaskEntry(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskEntry, workspaceName, stubId, taskId)
}

func (rk *redisKeys) TaskClaim(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskClaim, workspaceName, stubId, taskId)
}

func (rk *redisKeys) TaskRetryLock(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskRetryLock, workspaceName, stubId, taskId)
}

// Workspace keys
func (rk *redisKeys) WorkspacePrefix() string {
	return workspacePrefix
}

func (rk *redisKeys) WorkspaceConcurrencyLimit(workspaceId string) string {
	return fmt.Sprintf(workspaceConcurrencyLimit, workspaceId)
}

func (rk *redisKeys) WorkspaceConcurrencyLimitLock(workspaceId string) string {
	return fmt.Sprintf(workspaceConcurrencyLimitLock, workspaceId)
}

func (rk *redisKeys) WorkspaceVolumePathDownloadToken(token string) string {
	return fmt.Sprintf(workspaceVolumePathDownloadToken, token)
}

func (rl *redisKeys) WorkspaceAuthorizedToken(token string) string {
	return fmt.Sprintf(workspaceAuthorizedToken, token)
}

// Tailscale keys
func (rk *redisKeys) TailscalePrefix() string {
	return tailscalePrefix
}

func (rk *redisKeys) TailscaleServiceHostname(serviceName, hostName string) string {
	return fmt.Sprintf(tailscaleServiceHostname, serviceName, hostName)
}

// Provider keys
func (rk *redisKeys) ProviderPrefix() string {
	return providerPrefix
}

func (rk *redisKeys) ProviderMachineState(providerName, poolName, machineId string) string {
	return fmt.Sprintf(providerMachineState, providerName, poolName, machineId)
}

func (rk *redisKeys) ProviderMachineMetrics(providerName, poolName, machineId string) string {
	return fmt.Sprintf(providerMachineMetrics, providerName, poolName, machineId)
}

func (rk *redisKeys) ProviderMachineIndex(providerName, poolName string) string {
	return fmt.Sprintf(providerMachineIndex, providerName, poolName)
}

func (rk *redisKeys) ProviderMachineLock(providerName, poolName, machineId string) string {
	return fmt.Sprintf(providerMachineLock, providerName, poolName, machineId)
}

func (rk *redisKeys) ContainerName(prefix string, stubId string, containerId string) string {
	return fmt.Sprintf(containerName, prefix, stubId, containerId)
}

func (rk *redisKeys) ImageBuildContainerTTL(containerId string) string {
	return fmt.Sprintf(imageBuildContainerTTL, containerId)
}

func (rk *redisKeys) ImageChunkPriority(imageId string) string {
	return fmt.Sprintf(imageChunkPriority, imageId)
}
