package agent

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"time"
)

const (
	DefaultGatewayPort         = 1994
	DefaultKeepaliveInterval   = 60 * time.Second
	DefaultRegistrationTimeout = 30 * time.Second
	DefaultProviderName        = "generic"
	DefaultPoolName            = "external"
)

// AgentConfig holds all configuration for the agent
type AgentConfig struct {
	// Required - from beta9 machine create
	Token     string
	MachineID string
	PoolName  string

	// Gateway connection
	GatewayHost   string
	GatewayPort   int
	GatewayScheme string

	// Provider info
	ProviderName string

	// Machine hostname (for gateway to reach k3s API)
	Hostname string

	// k3s configuration
	K3sToken string

	// Timing
	KeepaliveInterval   time.Duration
	RegistrationTimeout time.Duration

	// Agent behavior
	Debug  bool
	DryRun bool
	Once   bool
}

// GatewayURL returns the full gateway URL for API calls
func (c *AgentConfig) GatewayURL() string {
	host := c.GatewayHost
	// Wrap IPv6 addresses in brackets for URL compatibility
	if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("%s://%s:%d", c.GatewayScheme, host, c.GatewayPort)
}

// RegisterURL returns the URL for machine registration endpoint
func (c *AgentConfig) RegisterURL() string {
	return fmt.Sprintf("%s/api/v1/machine/register", c.GatewayURL())
}

// KeepaliveURL returns the URL for machine keepalive endpoint
func (c *AgentConfig) KeepaliveURL() string {
	return fmt.Sprintf("%s/api/v1/machine/keepalive", c.GatewayURL())
}

// Validate checks configuration for errors
func (c *AgentConfig) Validate() error {
	if c.Token == "" {
		return &ErrConfigValidation{Field: "token", Message: "is required (from 'beta9 machine create')"}
	}
	if c.MachineID == "" {
		return &ErrConfigValidation{Field: "machine_id", Message: "is required"}
	}
	if len(c.MachineID) != 8 {
		return &ErrConfigValidation{Field: "machine_id", Message: fmt.Sprintf("must be exactly 8 hex chars, got %d chars: %s", len(c.MachineID), c.MachineID)}
	}
	matched, _ := regexp.MatchString("^[a-fA-F0-9]{8}$", c.MachineID)
	if !matched {
		return &ErrConfigValidation{Field: "machine_id", Message: fmt.Sprintf("must be hex characters only, got: %s", c.MachineID)}
	}
	if c.PoolName == "" {
		return &ErrConfigValidation{Field: "pool_name", Message: "is required"}
	}
	if c.GatewayPort < 1 || c.GatewayPort > 65535 {
		return &ErrConfigValidation{Field: "gateway_port", Message: fmt.Sprintf("must be 1-65535, got: %d", c.GatewayPort)}
	}
	// Security: never ship the k3s cluster bearer token over plaintext HTTP.
	// The token grants cluster-admin-equivalent access; anyone with passive
	// network capture on the Tailnet or access to the gateway's logs would
	// otherwise be able to harvest it.
	if c.K3sToken != "" && c.GatewayScheme != "https" {
		return &ErrConfigValidation{Field: "gateway_scheme", Message: "must be 'https' when a k3s token is configured (set BETA9_GATEWAY_SCHEME=https)"}
	}
	return nil
}

// NewConfigFromEnv creates config from environment variables
func NewConfigFromEnv() *AgentConfig {
	port := getEnvIntOrDefault("BETA9_GATEWAY_PORT", DefaultGatewayPort)
	keepaliveInterval := getEnvIntOrDefault("BETA9_KEEPALIVE_INTERVAL", int(DefaultKeepaliveInterval.Seconds()))

	// Default to http for local dev, but promote to https if a k3s token is
	// present (it is an admin-equivalent credential and must not travel in
	// plaintext). Callers can always override via BETA9_GATEWAY_SCHEME.
	scheme := getEnvOrDefault("BETA9_GATEWAY_SCHEME", "http")
	k3sToken := os.Getenv("BETA9_K3S_TOKEN")
	if k3sToken != "" && scheme == "http" {
		scheme = "https"
	}

	return &AgentConfig{
		Token:               os.Getenv("BETA9_TOKEN"),
		MachineID:           os.Getenv("BETA9_MACHINE_ID"),
		PoolName:            getEnvOrDefault("BETA9_POOL_NAME", DefaultPoolName),
		GatewayHost:         getEnvOrDefault("BETA9_GATEWAY_HOST", "localhost"),
		GatewayPort:         port,
		GatewayScheme:       scheme,
		ProviderName:        getEnvOrDefault("BETA9_PROVIDER_NAME", DefaultProviderName),
		Hostname:            os.Getenv("BETA9_HOSTNAME"),
		K3sToken:            k3sToken,
		KeepaliveInterval:   time.Duration(keepaliveInterval) * time.Second,
		RegistrationTimeout: DefaultRegistrationTimeout,
		Debug:               getEnvBool("BETA9_DEBUG"),
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvIntOrDefault(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvBool(key string) bool {
	val := os.Getenv(key)
	return val == "1" || val == "true" || val == "yes"
}
