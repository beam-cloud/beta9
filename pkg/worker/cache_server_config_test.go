package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadCacheServerConfig(t *testing.T) {
	t.Setenv("CONFIG_JSON", `{"providers":{"aws":{"secret_access_key":"ignored"}}}`)
	path := writeCacheServerTestConfig(t, `
gateway:
  grpc:
    externalHost: gateway.example
    externalPort: 443
worker:
  pools:
    gpu:
      gpuType: A10G
      cache:
        disk:
          mountPath: /var/cache
cache:
  coordinator:
    token: test-cache-token
  reconciliation:
    enabled: true
database:
  s2:
    basin: test-basin
    apiKey: test-cache-events-token
imageService:
  registryStore: s3
monitoring:
  victoriametrics:
    pushURL: https://metrics.example
    pushSecs: 10
`)
	config, err := loadCacheServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if grpc := config.GatewayService.GRPC; grpc.ExternalHost != "gateway.example" || grpc.ExternalPort != 443 {
		t.Fatalf("gateway address was not loaded: %#v", grpc)
	}
	if pool := config.Worker.Pools["gpu"]; pool.GPUType != "A10G" || pool.Cache.Disk.MountPath != "/var/cache" {
		t.Fatal("cache placement settings were not loaded")
	}
	if !config.Cache.Enabled || !config.Cache.Disk.Enabled || !config.Cache.Client.CacheFS.Enabled || !config.Worker.CacheEnabled {
		t.Fatal("cache server defaults were not preserved")
	}
	if !config.Cache.Reconciliation.Enabled || config.Database.S2.Basin != "test-basin" || config.Database.S2.StreamPrefix != "events" {
		t.Fatal("cache reconciliation settings were not loaded")
	}
	if config.ImageService.RegistryStore != "s3" || config.Monitoring.VictoriaMetrics.PushSecs != 10 {
		t.Fatal("image origin or monitoring settings were not loaded")
	}
	if config.Providers.AWS.SecretAccessKey != "" {
		t.Fatal("unexpected credential from CONFIG_JSON")
	}
}

func TestLoadCacheServerConfigRejectsSharedAndInvalidConfig(t *testing.T) {
	const secret = "must-not-appear-in-errors"
	const sharedGatewayConfig = `
clusterName: beta9
gateway:
  grpc:
    externalHost: gateway.example
database:
  postgres:
    password: must-not-appear-in-errors
cache:
  coordinator:
    token: test-cache-token
providers:
  aws:
    secretAccessKey: must-not-appear-in-errors
`
	for name, contents := range map[string]string{
		"copy of shared config": sharedGatewayConfig,
		"provider":              "providers:\n  aws:\n    secretAccessKey: " + secret,
		"postgres":              "database:\n  postgres:\n    password: " + secret,
		"webhook":               "events:\n  callbacks:\n  - headers:\n      Authorization: " + secret,
		"redis":                 "cache:\n  server:\n    metadata:\n      redisPasswd: " + secret + "\n  coordinator:\n    token: test-cache-token",
		"mount source":          "cache:\n  server:\n    sources:\n    - mountpoint:\n        secretKey: " + secret + "\n  coordinator:\n    token: test-cache-token",
		"write access":          "monitoring:\n  victoriametrics:\n    writePassword: " + secret,
		"multi-doc":             "cache:\n  coordinator:\n    token: test-cache-token\n---\nproviders:\n  aws:\n    secretAccessKey: " + secret,
		"bad YAML":              "gateway: [" + secret,
		"no token":              "cache: {}",
		"no S2 key":             "cache:\n  coordinator:\n    token: test-cache-token\n  reconciliation:\n    enabled: true",
		"no push interval":      "cache:\n  coordinator:\n    token: test-cache-token\nmonitoring:\n  victoriametrics:\n    pushURL: https://metrics.example",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadCacheServerConfig(writeCacheServerTestConfig(t, contents))
			if err == nil || strings.Contains(err.Error(), secret) {
				t.Fatal("cache server must reject this config without logging values")
			}
		})
	}
	if _, err := loadCacheServerConfig(""); err == nil {
		t.Fatal("cache server started without a config path")
	}
}

func writeCacheServerTestConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
