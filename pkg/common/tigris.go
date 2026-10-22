package common

import (
	"net/url"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	TigrisConsistencyHeader = "X-Tigris-Consistent"
	TigrisRegionsHeader     = "X-Tigris-Regions"
)

// IsImmutableStorageArtifact bounds recovery to uniquely named publications.
// Mutable volume files and coordination metadata must retain consistent reads,
// even when an ordinary lookup still has a cached copy after deletion.
func IsImmutableStorageArtifact(key string) bool {
	for _, prefix := range []string{types.DefaultOutputsPrefix, types.DefaultObjectPrefix, types.DefaultCheckpointsPrefix} {
		if strings.HasPrefix(key, prefix+"/") {
			return true
		}
	}
	parts := strings.Split(key, "/")
	return len(parts) > 3 && parts[0] == types.DefaultDurableDisksPrefix &&
		(parts[2] == types.DurableDiskChunksPrefix || parts[2] == types.DurableDiskSnapshotsPrefix)
}

// IsTigrisEndpoint recognizes Tigris's S3 endpoints, including virtual-hosted buckets.
func IsTigrisEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	for _, domain := range []string{"t3.storage.dev", "fly.storage.tigris.dev"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}
