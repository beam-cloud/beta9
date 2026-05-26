package common

import "strings"

const (
	workerNetworkPrefixClusterKey = "cluster"
	workerNetworkPrefixNodeKey    = "node"
)

type WorkerNetworkScope struct {
	ClusterName string
	NodeName    string
}

func WorkerNetworkPrefix(clusterName, nodeName string) string {
	parts := []string{
		workerNetworkPrefixClusterKey, workerNetworkPrefixPart(clusterName),
		workerNetworkPrefixNodeKey, workerNetworkPrefixPart(nodeName),
	}
	return strings.Join(parts, ":")
}

func ParseWorkerNetworkPrefix(prefix string) (WorkerNetworkScope, bool) {
	parts := strings.Split(strings.TrimSpace(prefix), ":")
	if len(parts) < 4 || len(parts)%2 != 0 || parts[0] != workerNetworkPrefixClusterKey {
		return WorkerNetworkScope{}, false
	}

	scope := WorkerNetworkScope{}
	for i := 0; i < len(parts)-1; i += 2 {
		switch parts[i] {
		case workerNetworkPrefixClusterKey:
			scope.ClusterName = parts[i+1]
		case workerNetworkPrefixNodeKey:
			scope.NodeName = parts[i+1]
		}
	}

	return scope, scope.ClusterName != "" && scope.NodeName != ""
}

func NormalizeWorkerNetworkPrefix(clusterName, networkPrefix string) string {
	if scope, ok := ParseWorkerNetworkPrefix(networkPrefix); ok {
		return WorkerNetworkPrefix(scope.ClusterName, scope.NodeName)
	}
	return WorkerNetworkPrefix(clusterName, networkPrefix)
}

func workerNetworkPrefixPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}

	replacer := strings.NewReplacer(
		":", "_",
		"/", "_",
		"\\", "_",
		" ", "_",
		"\t", "_",
		"\n", "_",
	)
	return replacer.Replace(value)
}
