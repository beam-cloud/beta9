package pod

import (
	"fmt"

	"github.com/beam-cloud/beta9/pkg/common"
)

// Redis keys
var (
	podInstanceLock         string = "pod:%s:%s:instance_lock"
	podContainerConnections string = "pod:%s:%s:container_connections:%s"
	podTotalConnections     string = "pod:%s:%s:total_connections"
	podProxyConnections     string = "pod:%s:%s:proxy_connections:%s:%s"
	podProxyConnectionIndex string = "pod:%s:%s:proxy_connection_proxies"
	podLLMPressure          string = "pod:%s:%s:llm_pressure:%s"
	podLLMEngineMetrics     string = "pod:%s:%s:llm_engine_metrics:%s"
	podLLMAffinity          string = "pod:%s:%s:llm_affinity:%s"
	podLLMPrefixAffinity    string = "pod:%s:%s:llm_prefix_affinity:%s"
	podRunTask              string = "pod:run_task:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) podKeepWarmLock(workspaceName, stubId, containerId string) string {
	return common.RedisKeys.PodKeepWarmLock(workspaceName, stubId, containerId)
}

func (k *keys) podInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(podInstanceLock, workspaceName, stubId)
}

func (k *keys) podContainerConnections(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(podContainerConnections, workspaceName, stubId, containerId)
}

func (k *keys) podTotalConnections(workspaceName, stubId string) string {
	return fmt.Sprintf(podTotalConnections, workspaceName, stubId)
}

func (k *keys) podProxyConnections(workspaceName, stubId, proxyId, target string) string {
	return fmt.Sprintf(podProxyConnections, workspaceName, stubId, proxyId, target)
}

func (k *keys) podProxyConnectionIndex(workspaceName, stubId string) string {
	return fmt.Sprintf(podProxyConnectionIndex, workspaceName, stubId)
}

func (k *keys) podLLMPressure(workspaceName, stubId, target string) string {
	return fmt.Sprintf(podLLMPressure, workspaceName, stubId, target)
}

func (k *keys) podLLMEngineMetrics(workspaceName, stubId, target string) string {
	return fmt.Sprintf(podLLMEngineMetrics, workspaceName, stubId, target)
}

func (k *keys) podLLMAffinity(workspaceName, stubId, affinityKey string) string {
	return fmt.Sprintf(podLLMAffinity, workspaceName, stubId, affinityKey)
}

func (k *keys) podLLMPrefixAffinity(workspaceName, stubId, prefixKey string) string {
	return fmt.Sprintf(podLLMPrefixAffinity, workspaceName, stubId, prefixKey)
}

func (k *keys) podRunTask(containerId string) string {
	return fmt.Sprintf(podRunTask, containerId)
}
