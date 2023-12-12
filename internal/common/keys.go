package common

import (
	"fmt"
)

var (
	schedulerPrefix            string = "scheduler:"
	schedulerContainerRequests string = "scheduler:container_requests"
	schedulerWorkerLock        string = "scheduler:worker:lock:%s"
	schedulerWorkerRequests    string = "scheduler:worker:requests:%s"
	schedulerWorkerState       string = "scheduler:worker:state:%s"
	schedulerContainerConfig   string = "scheduler:container:config:%s"
	schedulerContainerState    string = "scheduler:container:state:%s"
	schedulerContainerHost     string = "scheduler:container:host:%s"
	schedulerContainerServer   string = "scheduler:container:server:%s"
	schedulerContainerLock     string = "scheduler:container:lock:%s"
	schedulerContainerExitCode string = "scheduler:container:exit_code:%s"
)

var (
	gatewayPrefix                      string = "gateway"
	gatewayBucketLock                  string = "gateway:bucket_lock:%s"
	gatewayDefaultDeployment           string = "gateway:default_deployment:%s"
	gatewayDeploymentMinContainerCount string = "gateway:min_containers:%s"
	gatewayAuthKey                     string = "gateway:auth:%s:%s"
)

var (
	queuePrefix              string = "queue"
	queueList                string = "queue:%s:%s"
	queueTaskDuration        string = "queue:%s:%s:task_duration"
	queueAverageTaskDuration string = "queue:%s:%s:avg_task_duration"
	queueTasksInFlight       string = "queue:%s:%s:task:in_flight"
	queueTaskClaim           string = "queue:%s:%s:task:claim:%s"
	queueTaskRetries         string = "queue:%s:%s:task:retries:%s"
	queueTaskHeartbeat       string = "queue:%s:%s:task:heartbeat:%s"
	queueTaskCompleteEvent   string = "queue:%s:%s:task:complete:%s"
	queueTaskCancel          string = "queue:%s:%s:task:cancel:%s"
	queueProcessingLock      string = "queue:%s:%s:processing_lock:%s"
	queueKeepWarmLock        string = "queue:%s:%s:keep_warm_lock:%s"
	queueTaskRunningLock     string = "queue:%s:%s:task_running:%s:%s"
)

var (
	workerPrefix                 string = "worker"
	workerContainerRequest       string = "worker:%s:container:%s:request"
	workerContainerResourceUsage string = "worker:%s:container:%s:resource_usage"
)

var (
	workerPoolLock  string = "workerpool:lock:%s"
	workerPoolState string = "workerpool:state:%s"
)

var (
	identityPrefix               string = "identity"
	identityConcurrencyQuota     string = "identity:concurrency_quota:%s"
	identityActiveContainer      string = "identity:container:active:%s:%s:%s" // identityId, containerId, gpuType
	identityActiveContainersLock string = "identity:container:active:lock:%s"  // identityId
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

func (rk *redisKeys) SchedulerContainerServer(containerId string) string {
	return fmt.Sprintf(schedulerContainerServer, containerId)
}

func (rk *redisKeys) SchedulerContainerExitCode(containerId string) string {
	return fmt.Sprintf(schedulerContainerExitCode, containerId)
}

// Queue keys
func (rk *redisKeys) QueuePrefix() string {
	return queuePrefix
}

func (rk *redisKeys) QueueList(identityId, queueName string) string {
	return fmt.Sprintf(queueList, identityId, queueName)
}

func (rk *redisKeys) QueueTaskClaim(identityId, queueName, taskId string) string {
	return fmt.Sprintf(queueTaskClaim, identityId, queueName, taskId)
}

func (rk *redisKeys) QueueTasksInFlight(identityId, queueName string) string {
	return fmt.Sprintf(queueTasksInFlight, identityId, queueName)
}

func (rk *redisKeys) QueueTaskHeartbeat(identityId, queueName, taskId string) string {
	return fmt.Sprintf(queueTaskHeartbeat, identityId, queueName, taskId)
}

func (rk *redisKeys) QueueTaskRetries(identityId, queueName, taskId string) string {
	return fmt.Sprintf(queueTaskRetries, identityId, queueName, taskId)
}

func (rk *redisKeys) QueueTaskDuration(identityId, queueName string) string {
	return fmt.Sprintf(queueTaskDuration, identityId, queueName)
}

func (rk *redisKeys) QueueAverageTaskDuration(identityId, queueName string) string {
	return fmt.Sprintf(queueAverageTaskDuration, identityId, queueName)
}

func (rk *redisKeys) QueueTaskCompleteEvent(identityId, bucketName, taskId string) string {
	return fmt.Sprintf(queueTaskCompleteEvent, identityId, bucketName, taskId)
}

func (rk *redisKeys) QueueTaskRunningLock(identityId, bucketName, containerId, taskId string) string {
	return fmt.Sprintf(queueTaskRunningLock, identityId, bucketName, containerId, taskId)
}

func (rk *redisKeys) QueueProcessingLock(identityId, bucketName, containerId string) string {
	return fmt.Sprintf(queueProcessingLock, identityId, bucketName, containerId)
}

func (rk *redisKeys) QueueKeepWarmLock(identityId, bucketName, containerId string) string {
	return fmt.Sprintf(queueKeepWarmLock, identityId, bucketName, containerId)
}

func (rk *redisKeys) QueueTaskCancel(identityId, bucketName, taskId string) string {
	return fmt.Sprintf(queueTaskCancel, identityId, bucketName, taskId)
}

// Gateway keys
func (rk *redisKeys) GatewayPrefix() string {
	return gatewayPrefix
}

func (rk *redisKeys) GatewayBucketLock(bucketName string) string {
	return fmt.Sprintf(gatewayBucketLock, bucketName)
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

// WorkerPool keys
func (rk *redisKeys) WorkerPoolLock(poolId string) string {
	return fmt.Sprintf(workerPoolLock, poolId)
}

func (rk *redisKeys) WorkerPoolState(poolId string) string {
	return fmt.Sprintf(workerPoolState, poolId)
}

func (rk *redisKeys) IdentityPrefix() string {
	return identityPrefix
}

func (rk *redisKeys) IdentityConcurrencyQuota(identityId string) string {
	return fmt.Sprintf(identityConcurrencyQuota, identityId)
}

func (rk *redisKeys) IdentityActiveContainer(identityId string, containerId string, gpuType string) string {
	return fmt.Sprintf(identityActiveContainer, identityId, containerId, gpuType)
}

func (rk *redisKeys) IdentityActiveContainerLock(identityId string) string {
	return fmt.Sprintf(identityActiveContainersLock, identityId)
}
