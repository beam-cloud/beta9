package common

import (
	"net/url"
	"strings"
)

const TigrisConsistencyHeader = "X-Tigris-Consistent"

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
