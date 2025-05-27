package taskqueue

import "fmt"

// Redis keys
var (
	taskQueueList                  string = "taskqueue:%s:%s"
	taskQueueInstanceLock          string = "taskqueue:%s:%s:instance_lock"
	taskQueueTaskDuration          string = "taskqueue:%s:%s:task_duration"
	taskQueueAverageTaskDuration   string = "taskqueue:%s:%s:avg_task_duration"
	taskQueueTaskHeartbeat         string = "taskqueue:%s:%s:task:heartbeat:%s"
	taskQueueProcessingLock        string = "taskqueue:%s:%s:processing_lock:%s"
	taskQueueKeepWarmLock          string = "taskqueue:%s:%s:keep_warm_lock:%s"
	taskQueueTaskRunningLockIndex  string = "taskqueue:%s:%s:task_running:%s:index"
	taskQueueTaskRunningLock       string = "taskqueue:%s:%s:task_running:%s:%s"
	taskQueueTaskExternalWorkspace string = "taskqueue:%s:%s:task:external_workspace:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) taskQueueInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueInstanceLock, workspaceName, stubId)
}

func (k *keys) taskQueueList(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueList, workspaceName, stubId)
}

func (k *keys) taskQueueTaskHeartbeat(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskHeartbeat, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskDuration(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueTaskDuration, workspaceName, stubId)
}

func (k *keys) taskQueueTaskExternalWorkspace(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskExternalWorkspace, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueAverageTaskDuration(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueAverageTaskDuration, workspaceName, stubId)
}

func (k *keys) taskQueueTaskRunningLockIndex(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(taskQueueTaskRunningLockIndex, workspaceName, stubId, containerId)
}

func (k *keys) taskQueueTaskRunningLock(workspaceName, stubId, containerId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskRunningLock, workspaceName, stubId, containerId, taskId)
}

func (k *keys) taskQueueProcessingLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(taskQueueProcessingLock, workspaceName, stubId, containerId)
}

func (k *keys) taskQueueKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(taskQueueKeepWarmLock, workspaceName, stubId, containerId)
}
