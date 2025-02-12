package pod

import "fmt"

// Redis keys
var (
	podKeepWarmLock string = "pod:%s:%s:keep_warm_lock:%s"
	podInstanceLock string = "pod:%s:%s:instance_lock"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) podKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(podKeepWarmLock, workspaceName, stubId, containerId)
}

func (k *keys) podInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(podInstanceLock, workspaceName, stubId)
}
