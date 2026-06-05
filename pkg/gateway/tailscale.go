package gateway

import (
	"os"
	"strings"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
)

func gatewayTailscaleConfig(config types.AppConfig) network.TailscaleConfig {
	return network.TailscaleConfig{
		ControlURL: config.Tailscale.ControlURL,
		AuthKey:    config.Tailscale.AuthKey,
		Hostname:   gatewayTailscaleHostname(),
		Debug:      config.Tailscale.Debug,
		Ephemeral:  true,
	}
}

func gatewayTailscaleHostname() string {
	if hostname := sanitizedGatewayTailscaleHostname(os.Getenv(types.GatewayTSNetHostnameEnv)); hostname != "" {
		return hostname
	}
	for _, envKey := range []string{types.PodNameEnv, types.HostnameEnv} {
		if hostname := sanitizedGatewayTailscaleHostname(os.Getenv(envKey)); hostname != "" {
			return hostname
		}
	}
	if hostname, err := os.Hostname(); err == nil {
		if hostname := sanitizedGatewayTailscaleHostname(hostname); hostname != "" {
			return hostname
		}
	}
	return "beam-gateway"
}

func sanitizedGatewayTailscaleHostname(seed string) string {
	seed = strings.TrimSpace(strings.ToLower(seed))
	if seed == "" {
		return ""
	}

	var b strings.Builder
	lastDash := false
	for _, r := range seed {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	part := strings.Trim(b.String(), "-")
	if part == "" {
		return ""
	}

	const (
		prefix          = "beam-gateway-"
		maxDNSLabelSize = 63
	)
	if len(prefix)+len(part) > maxDNSLabelSize {
		part = part[len(part)-(maxDNSLabelSize-len(prefix)):]
		part = strings.TrimLeft(part, "-")
	}
	if part == "" {
		return "beam-gateway"
	}
	return prefix + part
}
