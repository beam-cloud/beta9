package common

import "strings"

const workerNetworkPrefixMarker = "cluster:"

func WorkerNetworkPrefix(clusterName, namespace, poolName, nodeName string) string {
	parts := []string{
		"cluster", workerNetworkPrefixPart(clusterName),
		"namespace", workerNetworkPrefixPart(namespace),
		"pool", workerNetworkPrefixPart(poolName),
		"node", workerNetworkPrefixPart(nodeName),
	}
	return strings.Join(parts, ":")
}

func IsScopedWorkerNetworkPrefix(prefix string) bool {
	return strings.HasPrefix(strings.TrimSpace(prefix), workerNetworkPrefixMarker)
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
