package scheduler

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	corev1 "k8s.io/api/core/v1"
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
