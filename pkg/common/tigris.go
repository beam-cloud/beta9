package common

import (
	"net/url"
	"strings"
)

const TigrisConsistencyHeader = "X-Tigris-Consistent"

// IsStrongTigrisBucketLocation accepts documented single-region locations and
// multi-region geographies, which guarantee globally consistent conditionals.
// Global, dual-region, and unknown locations cannot safely fence journal writers.
// https://www.tigrisdata.com/docs/buckets/locations/
// https://www.tigrisdata.com/docs/concepts/regions/
func IsStrongTigrisBucketLocation(location string) bool {
	switch strings.ToLower(strings.TrimSpace(location)) {
	case "usa", "eur", "sjc", "ord", "iad", "ams", "fra", "lhr", "sin", "nrt", "syd", "gru", "jnb":
		return true
	default:
		return false
	}
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
