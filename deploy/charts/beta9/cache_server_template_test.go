package chart_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCacheServerDaemonSetTemplate(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}

	tempDir := t.TempDir()
	valuesPath := filepath.Join(tempDir, "values.yaml")
	values := []byte(`
serviceAccount:
  name: beta9-control-plane
images:
  worker:
    repository: public.ecr.aws/n4e0e1y0/beta9-worker
    tag: latest
cacheServer:
  enabled: true
  tokenSecretName: beta9-cache-server-token
  configSecretName: beta9-config
  hostPath: /var/lib/beta9/cache
  mountPath: /var/lib/beta9/cache
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: k8s.beam.cloud/node-type
            operator: In
            values:
            - user-workload
          - key: kubernetes.io/arch
            operator: In
            values:
            - amd64
  tolerations:
  - key: nvidia.com/gpu
    operator: Exists
    effect: NoSchedule
`)
	if err := os.WriteFile(valuesPath, values, 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}

	cmd := exec.Command("helm", "template", "beta9", ".", "--values", valuesPath)
	cmd.Dir = "."
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	rendered := string(output)
	for _, want := range []string{
		"kind: DaemonSet",
		"name: beta9-cache-server",
		"serviceAccountName: beta9-control-plane",
		"public.ecr.aws/n4e0e1y0/beta9-worker:latest",
		"name: CACHE_SERVER_ONLY",
		"name: WORKER_TOKEN",
		"name: beta9-cache-server-token",
		"value: /etc/beta9/config.yaml",
		"secretName: beta9-config",
		"path: /var/lib/beta9/cache",
		"mountPath: /var/lib/beta9/cache",
		"k8s.beam.cloud/node-type",
		"nvidia.com/gpu",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered chart missing %q\n%s", want, rendered)
		}
	}
}
