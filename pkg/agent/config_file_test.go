package agent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigDir(t *testing.T) {
	dir := DefaultConfigDir()

	// Should end with .b9agent
	if filepath.Base(dir) != ".b9agent" {
		t.Errorf("expected config dir to end with .b9agent, got %s", dir)
	}
}

func TestDefaultConfigPath(t *testing.T) {
	path := DefaultConfigPath()

	// Should end with config.yaml
	if filepath.Base(path) != "config.yaml" {
		t.Errorf("expected config path to end with config.yaml, got %s", path)
	}

	// Parent should be .b9agent
	if filepath.Base(filepath.Dir(path)) != ".b9agent" {
		t.Errorf("expected parent dir to be .b9agent, got %s", filepath.Dir(path))
	}
}

func TestConfigExistsWithEnvOverride(t *testing.T) {
	// Create temp file
	tmpDir := t.TempDir()
	tmpConfig := filepath.Join(tmpDir, "test-config.yaml")
	if err := os.WriteFile(tmpConfig, []byte("gateway:\n  host: test\n"), 0600); err != nil {
		t.Fatal(err)
	}

	// Set env var
	os.Setenv("B9AGENT_CONFIG", tmpConfig)
	defer os.Unsetenv("B9AGENT_CONFIG")

	if !ConfigExists() {
		t.Error("expected ConfigExists to return true with env override")
	}

	// Test with non-existent path
	os.Setenv("B9AGENT_CONFIG", "/nonexistent/path/config.yaml")
	if ConfigExists() {
		t.Error("expected ConfigExists to return false for non-existent path")
	}
}

func TestSaveAndLoadConfigFile(t *testing.T) {
	// Create temp dir for config
	tmpDir := t.TempDir()
	tmpConfig := filepath.Join(tmpDir, "config.yaml")
	os.Setenv("B9AGENT_CONFIG", tmpConfig)
	defer os.Unsetenv("B9AGENT_CONFIG")

	// Create a config
	cfg := &ConfigFile{
		Gateway: GatewayConfig{
			Host: "100.72.101.23",
			Port: 1994,
		},
		Machine: MachineConfig{
			ID:       "abc12345",
			Token:    "secret-token-123",
			Hostname: "100.1.2.3",
		},
		Pool:     "external",
		Provider: "generic",
		K3s: K3sConfig{
			Token: "k3s-token",
		},
		Debug: true,
	}

	// Save it (need to override the default path)
	data, err := os.ReadFile(tmpConfig)
	if err == nil && len(data) > 0 {
		t.Log("Config already exists, skipping save test")
	}

	// Manually save since SaveConfigFile uses DefaultConfigPath
	err = saveConfigToPath(cfg, tmpConfig)
	if err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	// Load it back
	loaded, err := LoadConfigFile()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Verify fields
	if loaded.Gateway.Host != cfg.Gateway.Host {
		t.Errorf("Gateway.Host mismatch: expected %s, got %s", cfg.Gateway.Host, loaded.Gateway.Host)
	}
	if loaded.Gateway.Port != cfg.Gateway.Port {
		t.Errorf("Gateway.Port mismatch: expected %d, got %d", cfg.Gateway.Port, loaded.Gateway.Port)
	}
	if loaded.Machine.ID != cfg.Machine.ID {
		t.Errorf("Machine.ID mismatch: expected %s, got %s", cfg.Machine.ID, loaded.Machine.ID)
	}
	if loaded.Machine.Token != cfg.Machine.Token {
		t.Errorf("Machine.Token mismatch: expected %s, got %s", cfg.Machine.Token, loaded.Machine.Token)
	}
	if loaded.Machine.Hostname != cfg.Machine.Hostname {
		t.Errorf("Machine.Hostname mismatch: expected %s, got %s", cfg.Machine.Hostname, loaded.Machine.Hostname)
	}
	if loaded.Pool != cfg.Pool {
		t.Errorf("Pool mismatch: expected %s, got %s", cfg.Pool, loaded.Pool)
	}
	if loaded.Debug != cfg.Debug {
		t.Errorf("Debug mismatch: expected %v, got %v", cfg.Debug, loaded.Debug)
	}
}

// Helper to save config to a specific path
func saveConfigToPath(cfg *ConfigFile, path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	data := []byte(`gateway:
  host: ` + cfg.Gateway.Host + `
  port: ` + string(rune('0'+cfg.Gateway.Port/1000)) + string(rune('0'+(cfg.Gateway.Port/100)%10)) + string(rune('0'+(cfg.Gateway.Port/10)%10)) + string(rune('0'+cfg.Gateway.Port%10)) + `
machine:
  id: ` + cfg.Machine.ID + `
  token: ` + cfg.Machine.Token + `
  hostname: ` + cfg.Machine.Hostname + `
pool: ` + cfg.Pool + `
provider: ` + cfg.Provider + `
k3s:
  token: ` + cfg.K3s.Token + `
debug: true
`)
	return os.WriteFile(path, data, 0600)
}

func TestToAgentConfig(t *testing.T) {
	cfg := &ConfigFile{
		Gateway: GatewayConfig{
			Host: "gateway.example.com",
			Port: 2000,
		},
		Machine: MachineConfig{
			ID:       "machine-id",
			Token:    "auth-token",
			Hostname: "worker.local",
		},
		Pool:     "gpu-pool",
		Provider: "aws",
		K3s: K3sConfig{
			Token: "k3s-bearer-token",
		},
		Debug: true,
	}

	agentCfg := cfg.ToAgentConfig()

	if agentCfg.GatewayHost != "gateway.example.com" {
		t.Errorf("expected GatewayHost 'gateway.example.com', got '%s'", agentCfg.GatewayHost)
	}
	if agentCfg.GatewayPort != 2000 {
		t.Errorf("expected GatewayPort 2000, got %d", agentCfg.GatewayPort)
	}
	if agentCfg.MachineID != "machine-id" {
		t.Errorf("expected MachineID 'machine-id', got '%s'", agentCfg.MachineID)
	}
	if agentCfg.Token != "auth-token" {
		t.Errorf("expected Token 'auth-token', got '%s'", agentCfg.Token)
	}
	if agentCfg.PoolName != "gpu-pool" {
		t.Errorf("expected PoolName 'gpu-pool', got '%s'", agentCfg.PoolName)
	}
	if agentCfg.ProviderName != "aws" {
		t.Errorf("expected ProviderName 'aws', got '%s'", agentCfg.ProviderName)
	}
	// Security: ToAgentConfig now forces https when a k3s token is present,
	// because the k3s bearer token is cluster-admin-equivalent and must not
	// be shipped over plaintext HTTP.
	if agentCfg.GatewayScheme != "https" {
		t.Errorf("expected GatewayScheme 'https' (auto-promoted due to k3s token), got '%s'", agentCfg.GatewayScheme)
	}
}

func TestToAgentConfigDefaults(t *testing.T) {
	// Test with empty/zero values
	cfg := &ConfigFile{
		Gateway: GatewayConfig{
			Host: "localhost",
			Port: 0, // Should default
		},
		Machine: MachineConfig{
			ID:    "test",
			Token: "token",
		},
		Pool:     "", // Should default
		Provider: "", // Should default
	}

	agentCfg := cfg.ToAgentConfig()

	if agentCfg.GatewayPort != DefaultGatewayPort {
		t.Errorf("expected default port %d, got %d", DefaultGatewayPort, agentCfg.GatewayPort)
	}
	if agentCfg.PoolName != DefaultPoolName {
		t.Errorf("expected default pool '%s', got '%s'", DefaultPoolName, agentCfg.PoolName)
	}
	if agentCfg.ProviderName != DefaultProviderName {
		t.Errorf("expected default provider '%s', got '%s'", DefaultProviderName, agentCfg.ProviderName)
	}
}

func TestNewConfigFileFromAgentConfig(t *testing.T) {
	agentCfg := &AgentConfig{
		GatewayHost:  "gw.example.com",
		GatewayPort:  3000,
		MachineID:    "m-123",
		Token:        "secret",
		Hostname:     "host.local",
		PoolName:     "workers",
		ProviderName: "gcp",
		K3sToken:     "k3s-secret",
		Debug:        true,
	}

	cfg := NewConfigFileFromAgentConfig(agentCfg)

	if cfg.Gateway.Host != "gw.example.com" {
		t.Errorf("expected Gateway.Host 'gw.example.com', got '%s'", cfg.Gateway.Host)
	}
	if cfg.Gateway.Port != 3000 {
		t.Errorf("expected Gateway.Port 3000, got %d", cfg.Gateway.Port)
	}
	if cfg.Machine.ID != "m-123" {
		t.Errorf("expected Machine.ID 'm-123', got '%s'", cfg.Machine.ID)
	}
	if cfg.Machine.Token != "secret" {
		t.Errorf("expected Machine.Token 'secret', got '%s'", cfg.Machine.Token)
	}
	if cfg.Pool != "workers" {
		t.Errorf("expected Pool 'workers', got '%s'", cfg.Pool)
	}
}

func TestLoadOrCreateConfig(t *testing.T) {
	// Test when config doesn't exist
	os.Setenv("B9AGENT_CONFIG", "/nonexistent/path/config.yaml")
	defer os.Unsetenv("B9AGENT_CONFIG")

	cfg, exists, err := LoadOrCreateConfig()
	if err != nil {
		t.Errorf("expected no error for non-existent config, got %v", err)
	}
	if exists {
		t.Error("expected exists to be false for non-existent config")
	}
	if cfg != nil {
		t.Error("expected nil config for non-existent file")
	}
}

func TestConfigFilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	tmpConfig := filepath.Join(tmpDir, "config.yaml")

	cfg := &ConfigFile{
		Gateway: GatewayConfig{Host: "test", Port: 1994},
		Machine: MachineConfig{ID: "test", Token: "secret"},
	}

	err := saveConfigToPath(cfg, tmpConfig)
	if err != nil {
		t.Fatalf("failed to save config: %v", err)
	}

	info, err := os.Stat(tmpConfig)
	if err != nil {
		t.Fatalf("failed to stat config: %v", err)
	}

	// Check permissions are restricted (0600)
	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected permissions 0600, got %o", perm)
	}
}
