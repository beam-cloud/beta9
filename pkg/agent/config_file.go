package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"gopkg.in/yaml.v3"
)

// ConfigFile represents the persistent config file structure
type ConfigFile struct {
	Gateway  GatewayConfig `yaml:"gateway"`
	Machine  MachineConfig `yaml:"machine"`
	Pool     string        `yaml:"pool"`
	Provider string        `yaml:"provider,omitempty"`
	K3s      K3sConfig     `yaml:"k3s,omitempty"`
	Scheme   string        `yaml:"scheme,omitempty"`
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

// SaveConfigFile saves config to YAML file.
//
// The write path is hardened against symlink-TOCTOU attacks:
//   - The parent directory must be owned by the current user on POSIX
//     systems (checked after MkdirAll so that the directory we created
//     ourselves passes trivially).
//   - We remove an existing regular file at the target (never following a
//     symlink to remove something else) before creating the new file.
//   - The file is opened with O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW so we
//     refuse to follow a symlink planted between the unlink and the open.
//   - Permissions are enforced via fchmod on the open file descriptor,
//     never chmod-by-path (which follows symlinks).
//
// On Windows, O_NOFOLLOW and owner checks are not enforced by the kernel;
// we fall back to WriteFile + chmod with the existing semantics because
// Windows is not a supported privileged-service deployment target for
// b9agent anyway.
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

	return writeConfigAtomically(path, configDir, data)
}

// writeConfigAtomically performs the hardened write. Extracted for testability.
func writeConfigAtomically(path, configDir string, data []byte) error {
	if runtime.GOOS == "windows" {
		// Windows fallback: best-effort writeFile + chmod (see SaveConfigFile docs).
		if err := os.WriteFile(path, data, 0600); err != nil {
			return fmt.Errorf("failed to write config file: %w", err)
		}
		if err := os.Chmod(path, 0600); err != nil {
			return fmt.Errorf("failed to set config file permissions: %w", err)
		}
		return nil
	}

	// Ensure the parent directory is owned by the current user. If we're
	// root writing into /tmp (say, sudo ./beta9_setup.sh with
	// B9AGENT_CONFIG=/tmp/foo) refusing here prevents a local user from
	// racing a symlink swap between unlink and open.
	if err := checkParentDirOwned(configDir); err != nil {
		return fmt.Errorf("refusing to write config: %w", err)
	}

	// If a regular file already exists at path, remove it so O_EXCL can
	// succeed. Use Lstat (not Stat) so that a symlink is detected and
	// rejected rather than dereferenced.
	if info, err := os.Lstat(path); err == nil {
		mode := info.Mode()
		if mode&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to write config: %s is a symlink", path)
		}
		if !mode.IsRegular() {
			return fmt.Errorf("refusing to write config: %s is not a regular file", path)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("failed to remove existing config file: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to stat config path: %w", err)
	}

	// O_NOFOLLOW guarantees that if an attacker planted a symlink between
	// the Remove above and this Open, the open fails with ELOOP rather
	// than silently redirecting the write to a root-owned target.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	// Enforce mode via fchmod on the fd (never chmod by path, which would
	// follow symlinks).
	if err := f.Chmod(0600); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("failed to set config file permissions: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("failed to write config file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close config file: %w", err)
	}
	return nil
}

// checkParentDirOwned verifies that the config's parent directory is owned
// by the current effective UID on POSIX systems. A no-op on Windows.
func checkParentDirOwned(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("stat parent dir %q: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("parent %q is not a directory", dir)
	}
	sys, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		// Unknown FS — err on the side of caution and allow, since without
		// Stat_t we have no reliable UID to compare.
		return nil
	}
	me := os.Geteuid()
	if int(sys.Uid) != me {
		return fmt.Errorf("parent dir %q not owned by current user (uid=%d, want=%d)", dir, sys.Uid, me)
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

	// Default scheme to http for back-compat, but if a k3s token is present
	// we force https: the k3s bearer token is admin-equivalent and must not
	// travel in plaintext.
	scheme := c.Scheme
	if scheme == "" {
		scheme = "http"
	}
	if c.K3s.Token != "" && scheme == "http" {
		scheme = "https"
	}

	return &AgentConfig{
		Token:               c.Machine.Token,
		MachineID:           c.Machine.ID,
		PoolName:            pool,
		GatewayHost:         c.Gateway.Host,
		GatewayPort:         port,
		GatewayScheme:       scheme,
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
		Scheme: cfg.GatewayScheme,
		Debug:  cfg.Debug,
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
