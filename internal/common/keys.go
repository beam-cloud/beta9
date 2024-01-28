package common

import (
	"fmt"
)

var (
	schedulerPrefix              string = "scheduler:"
	schedulerContainerRequests   string = "scheduler:container_requests"
	schedulerWorkerLock          string = "scheduler:worker:lock:%s"
	schedulerWorkerRequests      string = "scheduler:worker:requests:%s"
	schedulerWorkerState         string = "scheduler:worker:state:%s"
	schedulerContainerConfig     string = "scheduler:container:config:%s"
	schedulerContainerState      string = "scheduler:container:state:%s"
	schedulerContainerHost       string = "scheduler:container:host:%s"
	schedulerWorkerContainerHost string = "scheduler:container:worker_host:%s"
	schedulerContainerLock       string = "scheduler:container:lock:%s"
	schedulerContainerExitCode   string = "scheduler:container:exit_code:%s"
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
	workerContainerRequest       string = "worker:%s:container:%s:request"
	workerContainerResourceUsage string = "worker:%s:container:%s:resource_usage"
)

var (
	workerPoolLock  string = "workerpool:lock:%s"
	workerPoolState string = "workerpool:state:%s"
)

var (
	workspacePrefix               string = "workspace"
	workspaceConcurrencyQuota     string = "workspace:concurrency_quota:%s"
	workspaceActiveContainer      string = "workspace:container:active:%s:%s:%s"
	workspaceActiveContainersLock string = "workspace:container:active:lock:%s"
)

var (
	machinePrefix   string = "machine"
	machineCapacity string = "machine:capacity:%s:%s:%s"
	machineHost     string = "machine:host:%s:%s:%s"
)

var RedisKeys = &redisKeys{}

type redisKeys struct{}

// Scheduler scheduling keys
func (rk *redisKeys) SchedulerPrefix() string {
	return schedulerPrefix
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

func (rk *redisKeys) SchedulerContainerState(containerId string) string {
	return fmt.Sprintf(schedulerContainerState, containerId)
}

func (rk *redisKeys) SchedulerContainerConfig(containerId string) string {
	return fmt.Sprintf(schedulerContainerConfig, containerId)
}

func (rk *redisKeys) SchedulerContainerHost(containerId string) string {
	return fmt.Sprintf(schedulerContainerHost, containerId)
}

func (rk *redisKeys) SchedulerWorkerContainerHost(containerId string) string {
	return fmt.Sprintf(schedulerWorkerContainerHost, containerId)
}

func (rk *redisKeys) SchedulerContainerExitCode(containerId string) string {
	return fmt.Sprintf(schedulerContainerExitCode, containerId)
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

func (rk *redisKeys) WorkerContainerRequest(workerId string, containerId string) string {
	return fmt.Sprintf(workerContainerRequest, workerId, containerId)
}

func (rk *redisKeys) WorkerContainerResourceUsage(workerId string, containerId string) string {
	return fmt.Sprintf(workerContainerResourceUsage, workerId, containerId)
}

func (rk *redisKeys) WorkerImageLock(workerId string, imageId string) string {
	return fmt.Sprintf(workerImageLock, workerId, imageId)
}

// Workspace keys
func (rk *redisKeys) WorkspacePrefix() string {
	return workspacePrefix
}

func (rk *redisKeys) WorkspaceConcurrencyQuota(workspaceId string) string {
	return fmt.Sprintf(workspaceConcurrencyQuota, workspaceId)
}

func (rk *redisKeys) WorkspaceActiveContainer(workspaceId string, containerId string, gpuType string) string {
	return fmt.Sprintf(workspaceActiveContainer, workspaceId, containerId, gpuType)
}

func (rk *redisKeys) WorkspaceActiveContainerLock(workspaceId string) string {
	return fmt.Sprintf(workspaceActiveContainersLock, workspaceId)
}

// WorkerPool keys
func (rk *redisKeys) WorkerPoolLock(poolId string) string {
	return fmt.Sprintf(workerPoolLock, poolId)
}

func (rk *redisKeys) WorkerPoolState(poolId string) string {
	return fmt.Sprintf(workerPoolState, poolId)
}

// Machine keys
func (rk *redisKeys) MachinePrefix() string {
	return machinePrefix
}

func (rk *redisKeys) MachineCapacity(workspaceId, providerName, machineId string) string {
	return fmt.Sprintf(machineCapacity, workspaceId, providerName, machineId)
}

func (rk *redisKeys) MachineHost(workspaceId, providerName, machineId string) string {
	return fmt.Sprintf(machineHost, workspaceId, providerName, machineId)
}

func (rk *redisKeys) MachineHourlyCost(workspaceId, providerName, machineId string) string {
	return fmt.Sprintf(machineHost, workspaceId, providerName, machineId)
}
