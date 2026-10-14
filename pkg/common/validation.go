package common

import (
	"fmt"
	"net"
	"regexp"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

const MaxAllowListEntries = 64

// ValidateCIDR normalizes a network or a single IP address.
func ValidateCIDR(entry string) (string, bool, error) {
	if ip := net.ParseIP(entry); ip != nil {
		bits := 128
		if ip.To4() != nil {
			bits = 32
		}
		entry = fmt.Sprintf("%s/%d", ip, bits)
	}
	_, network, err := net.ParseCIDR(entry)
	if err != nil {
		return "", false, fmt.Errorf("not a valid IP or CIDR")
	}
	return network.String(), network.IP.To4() == nil, nil
}

// ValidateHostPattern accepts RFC 1123 hostnames and subdomain wildcards.
var hostPattern = regexp.MustCompile(`^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func ValidateHostPattern(pattern string) error {
	host := strings.TrimPrefix(strings.ToLower(pattern), "*.")
	if len(host) > 253 || net.ParseIP(host) != nil || !hostPattern.MatchString(host) {
		return fmt.Errorf("expected a hostname or *.hostname, without ports")
	}
	return nil
}

func ValidateAllowList(list []string) error {
	if len(list) > MaxAllowListEntries {
		return fmt.Errorf("allowlist exceeds maximum of %d entries (got %d)", MaxAllowListEntries, len(list))
	}
	for _, entry := range list {
		if _, _, err := ValidateCIDR(entry); err == nil {
			continue
		}
		if err := ValidateHostPattern(entry); err != nil {
			return fmt.Errorf("invalid allowlist entry %q: %w", entry, err)
		}
	}
	return nil
}

// SplitAllowList separates normalized IP networks from hostname patterns.
func SplitAllowList(list []string) (cidrs, hosts []string) {
	for _, entry := range list {
		if cidr, _, err := ValidateCIDR(entry); err == nil {
			cidrs = append(cidrs, cidr)
		} else {
			hosts = append(hosts, strings.ToLower(entry))
		}
	}
	return
}

// MatchDomain wildcards match subdomains, never the apex or a partial label.
func MatchDomain(host, pattern string) bool {
	host, pattern = strings.TrimSuffix(strings.ToLower(host), "."), strings.ToLower(pattern)
	if suffix, wildcard := strings.CutPrefix(pattern, "*."); wildcard {
		return host != suffix && strings.HasSuffix(host, "."+suffix)
	}
	return host == pattern
}

// ValidateSecretHosts shares the hostname grammar with egress, excluding IPs.
func ValidateSecretHosts(hosts []string, value string) error {
	if len(hosts) > MaxAllowListEntries {
		return fmt.Errorf("secret hosts exceed maximum of %d entries", MaxAllowListEntries)
	}
	for _, host := range hosts {
		if err := ValidateHostPattern(host); err != nil {
			return fmt.Errorf("invalid secret host %q: %w", host, err)
		}
	}
	if len(hosts) > 0 && (value == "" || len(value) > 16384) {
		return fmt.Errorf("brokered secret values must contain 1–16384 bytes")
	}
	return nil
}

// HostPatternCovered requires the whole credential rule to fit within egress.
func HostPatternCovered(pattern string, allowList []string) bool {
	for _, allowed := range allowList {
		if strings.HasPrefix(pattern, "*.") && !strings.HasPrefix(allowed, "*.") {
			continue
		}
		if MatchDomain(pattern, allowed) {
			return true
		}
	}
	return false
}

func ValidateSandboxSecrets(secrets []types.Secret, allowList []string) error {
	for _, secret := range secrets {
		if secret.Name == "BETA9_TOKEN" || secret.Name == "BEAM_TOKEN" {
			continue
		}
		if len(secret.Hosts) == 0 || secret.Placeholder == "" {
			return fmt.Errorf("sandbox secret %q requires allowed hosts", secret.Name)
		}
		for _, host := range secret.Hosts {
			if len(allowList) > 0 && !HostPatternCovered(host, allowList) {
				return fmt.Errorf("secret %q host %q is not allowed by egress", secret.Name, host)
			}
		}
	}
	return nil
}
