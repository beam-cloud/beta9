package common

import (
	"fmt"
	"net"
)

// ValidateCIDR validates CIDR notation and returns:
// - normalized CIDR string
// - boolean indicating if it's IPv6
// - error if invalid
func ValidateCIDR(entry string) (string, bool, error) {
	_, ipNet, err := net.ParseCIDR(entry)
	if err != nil {
		return "", false, fmt.Errorf("not a valid CIDR notation (e.g., 8.8.8.8/32 or 10.0.0.0/8)")
	}

	isIPv6 := ipNet.IP.To4() == nil
	return ipNet.String(), isIPv6, nil
}

// ValidateAllowList validates a list of CIDR entries and returns an error if any are invalid
func ValidateAllowList(allowList []string) error {
	for _, entry := range allowList {
		_, _, err := ValidateCIDR(entry)
		if err != nil {
			return fmt.Errorf("invalid allowlist entry %q: %w", entry, err)
		}
	}
	return nil
}