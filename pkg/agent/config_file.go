package agent

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ConfigFile represents the persistent config file structure
type ConfigFile struct {
	Gateway  GatewayConfig `yaml:"gateway"`
	Machine  MachineConfig `yaml:"machine"`
	Pool     string        `yaml:"pool"`
	Provider string        `yaml:"provider,omitempty"`
	K3s      K3sConfig     `yaml:"k3s,omitempty"`
	Debug    bool          `yaml:"debug,omitempty"`
}

// GatewayConfig holds gateway connection settings
type GatewayConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// MachineConfig holds machine identity
type MachineConfig struct {
	ID       string `yaml:"id"`
	Token    string `yaml:"token"`
	Hostname string `yaml:"hostname,omitempty"`
}

// K3sConfig holds k3s settings
type K3sConfig struct {
	Token string `yaml:"token,omitempty"`
}

// DefaultConfigDir returns the default config directory path
func DefaultConfigDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".b9agent"
	}
	return filepath.Join(home, ".b9agent")
}

// DefaultConfigPath returns the default config file path
func DefaultConfigPath() string {
	if envPath := os.Getenv("B9AGENT_CONFIG"); envPath != "" {
		return envPath
	}
	return filepath.Join(DefaultConfigDir(), "config.yaml")
}

// ConfigExists checks if a config file exists
func ConfigExists() bool {
	path := DefaultConfigPath()
	_, err := os.Stat(path)
	return err == nil
}

// LoadConfigFile loads config from YAML file
func LoadConfigFile() (*ConfigFile, error) {
	path := DefaultConfigPath()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg ConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// SaveConfigFile saves config to YAML file
func SaveConfigFile(cfg *ConfigFile) error {
	configDir := DefaultConfigDir()
	path := DefaultConfigPath()

	// Respect B9AGENT_CONFIG override
	if envPath := os.Getenv("B9AGENT_CONFIG"); envPath != "" {
		path = envPath
		configDir = filepath.Dir(path)
	}

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Write with restrictive permissions (contains token)
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	// Enforce permissions on existing files
	if err := os.Chmod(path, 0600); err != nil {
		return fmt.Errorf("failed to set config file permissions: %w", err)
	}

	return nil
}

// ToAgentConfig converts ConfigFile to AgentConfig
func (c *ConfigFile) ToAgentConfig() *AgentConfig {
	port := c.Gateway.Port
	if port == 0 {
		port = DefaultGatewayPort
	}

	pool := c.Pool
	if pool == "" {
		pool = DefaultPoolName
	}

	provider := c.Provider
	if provider == "" {
		provider = DefaultProviderName
	}

	return &AgentConfig{
		Token:               c.Machine.Token,
		MachineID:           c.Machine.ID,
		PoolName:            pool,
		GatewayHost:         c.Gateway.Host,
		GatewayPort:         port,
		GatewayScheme:       "http",
		ProviderName:        provider,
		Hostname:            c.Machine.Hostname,
		K3sToken:            c.K3s.Token,
		KeepaliveInterval:   DefaultKeepaliveInterval,
		RegistrationTimeout: DefaultRegistrationTimeout,
		Debug:               c.Debug,
	}
}

// NewConfigFileFromAgentConfig creates a ConfigFile from AgentConfig
func NewConfigFileFromAgentConfig(cfg *AgentConfig) *ConfigFile {
	return &ConfigFile{
		Gateway: GatewayConfig{
			Host: cfg.GatewayHost,
			Port: cfg.GatewayPort,
		},
		Machine: MachineConfig{
			ID:       cfg.MachineID,
			Token:    cfg.Token,
			Hostname: cfg.Hostname,
		},
		Pool:     cfg.PoolName,
		Provider: cfg.ProviderName,
		K3s: K3sConfig{
			Token: cfg.K3sToken,
		},
		Debug: cfg.Debug,
	}
}

// LoadOrCreateConfig loads config from file, or returns defaults if not exists
func LoadOrCreateConfig() (*AgentConfig, bool, error) {
	if !ConfigExists() {
		return nil, false, nil
	}

	fileCfg, err := LoadConfigFile()
	if err != nil {
		return nil, true, err
	}

	return fileCfg.ToAgentConfig(), true, nil
}
