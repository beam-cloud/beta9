package scheduler

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestLocalWorkerPrometheusConfigurationSupportsHostNetworkBinPacking(t *testing.T) {
	prometheus := string(types.MetricsCollectorPrometheus)
	none := string(types.MetricsCollectorNone)
	openmeter := string(types.MetricsCollectorOpenMeter)
	tests := []struct {
		name, collector            string
		hostNetwork, scrapeWorkers bool
		wantPort, wantConfigScrape bool
		wantCollector              string
	}{
		{"host network", prometheus, true, true, false, false, none},
		{"pod network", prometheus, false, true, true, true, prometheus},
		{"scraping disabled", prometheus, false, false, false, false, prometheus},
		{"openmeter", openmeter, true, true, false, true, openmeter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &LocalKubernetesWorkerPoolController{
				name: "test-pool",
				config: types.AppConfig{
					Monitoring: types.MonitoringConfig{
						MetricsCollector: tt.collector,
						Prometheus:       types.PrometheusConfig{Port: 9090, ScrapeWorkers: tt.scrapeWorkers},
					},
					Worker: types.WorkerConfig{HostNetwork: tt.hostNetwork},
				},
			}

			job, _ := controller.createWorkerJob("worker-1", 0, 0, "", 0, "token")
			container := job.Spec.Template.Spec.Containers[0]
			if got := job.Spec.Template.Labels[PrometheusScrapeKey]; got != strconv.FormatBool(tt.wantPort) {
				t.Fatalf("scrape label = %q", got)
			}
			_, hasPort := job.Spec.Template.Labels[PrometheusPortKey]
			if hasPort != tt.wantPort || len(container.Ports) != 0 {
				t.Fatalf("port label = %t, ports = %+v", hasPort, container.Ports)
			}

			config := workerConfigFromEnvironment(t, container.Env)
			if got := config.Monitoring; got.MetricsCollector != tt.wantCollector || got.Prometheus.ScrapeWorkers != tt.wantConfigScrape {
				t.Fatalf("worker monitoring config = %+v", got)
			}
		})
	}
}

func TestLocalWorkerConfigUsesGatewayServiceHostname(t *testing.T) {
	controller := &LocalKubernetesWorkerPoolController{config: types.AppConfig{
		GatewayService: types.GatewayServiceConfig{Host: "beta9-gateway"},
		Worker:         types.WorkerConfig{UseGatewayServiceHostname: true},
	}}
	controller.config.GatewayService.GRPC = types.GRPCConfig{ExternalHost: "0.tcp.ngrok.io", ExternalPort: 12345, TLS: true, Port: 1993}
	controller.config.GatewayService.HTTP = types.HTTPConfig{ExternalHost: "public.example.com", ExternalPort: 443, TLS: true, Port: 1994}

	job, _ := controller.createWorkerJob("worker-1", 0, 0, "", 0, "token")
	config := workerConfigFromEnvironment(t, job.Spec.Template.Spec.Containers[0].Env)
	grpc := config.GatewayService.GRPC
	if grpc.ExternalHost != "beta9-gateway" || grpc.ExternalPort != 1993 || grpc.TLS {
		t.Fatalf("worker gRPC config = %+v", grpc)
	}
	http := config.GatewayService.HTTP
	if http.ExternalHost != "beta9-gateway" || http.ExternalPort != 1994 || http.TLS {
		t.Fatalf("worker HTTP config = %+v", http)
	}
}

func workerConfigFromEnvironment(t *testing.T, env []corev1.EnvVar) types.AppConfig {
	t.Helper()
	for _, variable := range env {
		if variable.Name != "CONFIG_JSON" {
			continue
		}
		var config types.AppConfig
		if err := json.Unmarshal([]byte(variable.Value), &config); err != nil {
			t.Fatalf("unmarshal worker config: %v", err)
		}
		return config
	}
	t.Fatal("CONFIG_JSON is missing")
	return types.AppConfig{}
}

func TestApplyJobResourceOverhead(t *testing.T) {
	schedulable := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4000m"),
		corev1.ResourceMemory: resource.MustParse("8192Mi"),
	}

	t.Run("adds overhead to the pod without touching schedulable capacity", func(t *testing.T) {
		out := applyJobResourceOverhead(schedulable, types.JobResourceOverheadConfig{CPU: "1000m", Memory: "1Gi"})
		if got := out[corev1.ResourceCPU]; got.MilliValue() != 5000 {
			t.Fatalf("cpu = %s, want 5000m", got.String())
		}
		if got := out[corev1.ResourceMemory]; got.Value() != 9<<30 {
			t.Fatalf("memory = %s, want 9Gi", got.String())
		}
		if got := schedulable[corev1.ResourceCPU]; got.MilliValue() != 4000 {
			t.Fatalf("schedulable cpu mutated to %s", got.String())
		}
	})

	t.Run("empty, zero and invalid overhead leave resources unchanged", func(t *testing.T) {
		for _, cfg := range []types.JobResourceOverheadConfig{
			{},
			{CPU: "0", Memory: "0"},
			{CPU: "lots", Memory: "-1Gi"},
			// Quantities from the other resource family parse but are nonsense.
			{CPU: "512Mi", Memory: "500m"},
		} {
			out := applyJobResourceOverhead(schedulable, cfg)
			if got := out[corev1.ResourceCPU]; got.MilliValue() != 4000 {
				t.Fatalf("%+v: cpu = %s, want 4000m", cfg, got.String())
			}
			if got := out[corev1.ResourceMemory]; got.Value() != 8<<30 {
				t.Fatalf("%+v: memory = %s, want 8Gi", cfg, got.String())
			}
		}
	})
}
