package scheduler

import (
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestLocalWorkerPrometheusConfigurationSupportsHostNetworkBinPacking(t *testing.T) {
	tests := []struct {
		name                string
		collector           string
		hostNetwork         bool
		scrapeWorkers       bool
		wantScrape          string
		wantPortLabel       bool
		wantWorkerCollector string
	}{
		{
			name:                "host network disables pull collector",
			collector:           string(types.MetricsCollectorPrometheus),
			hostNetwork:         true,
			scrapeWorkers:       true,
			wantScrape:          "false",
			wantPortLabel:       false,
			wantWorkerCollector: string(types.MetricsCollectorNone),
		},
		{
			name:                "pod network retains pull collector without declaring a port",
			collector:           string(types.MetricsCollectorPrometheus),
			scrapeWorkers:       true,
			wantScrape:          "true",
			wantPortLabel:       true,
			wantWorkerCollector: string(types.MetricsCollectorPrometheus),
		},
		{
			name:                "disabled scraping does not advertise a port",
			collector:           string(types.MetricsCollectorPrometheus),
			wantScrape:          "false",
			wantPortLabel:       false,
			wantWorkerCollector: string(types.MetricsCollectorPrometheus),
		},
		{
			name:                "openmeter remains unchanged",
			collector:           string(types.MetricsCollectorOpenMeter),
			hostNetwork:         true,
			scrapeWorkers:       true,
			wantScrape:          "false",
			wantPortLabel:       false,
			wantWorkerCollector: string(types.MetricsCollectorOpenMeter),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &LocalKubernetesWorkerPoolController{
				name: "test-pool",
				config: types.AppConfig{
					Monitoring: types.MonitoringConfig{
						MetricsCollector: tt.collector,
						Prometheus: types.PrometheusConfig{
							Port:          9090,
							ScrapeWorkers: tt.scrapeWorkers,
						},
					},
					Worker: types.WorkerConfig{HostNetwork: tt.hostNetwork},
				},
			}

			job, _ := controller.createWorkerJob("worker-1", 0, 0, "", 0, "token")
			if got := job.Spec.Template.Labels[PrometheusScrapeKey]; got != tt.wantScrape {
				t.Fatalf("scrape label = %q, want %q", got, tt.wantScrape)
			}
			_, hasPortLabel := job.Spec.Template.Labels[PrometheusPortKey]
			if hasPortLabel != tt.wantPortLabel {
				t.Fatalf("port label present = %t, want %t", hasPortLabel, tt.wantPortLabel)
			}

			container := job.Spec.Template.Spec.Containers[0]
			if len(container.Ports) != 0 {
				t.Fatalf("worker declares ports that constrain bin packing: %+v", container.Ports)
			}

			var configJSON string
			for _, env := range container.Env {
				if env.Name == "CONFIG_JSON" {
					configJSON = env.Value
					break
				}
			}
			if configJSON == "" {
				t.Fatal("worker CONFIG_JSON environment variable is missing")
			}
			var workerConfig types.AppConfig
			if err := json.Unmarshal([]byte(configJSON), &workerConfig); err != nil {
				t.Fatalf("unmarshal worker config: %v", err)
			}
			if got := workerConfig.Monitoring.MetricsCollector; got != tt.wantWorkerCollector {
				t.Fatalf("worker metrics collector = %q, want %q", got, tt.wantWorkerCollector)
			}
		})
	}
}

func TestLocalWorkerConfigUsesGatewayServiceHostname(t *testing.T) {
	controller := &LocalKubernetesWorkerPoolController{
		config: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				Host: "beta9-gateway",
				GRPC: types.GRPCConfig{
					ExternalHost: "0.tcp.ngrok.io",
					ExternalPort: 12345,
					TLS:          true,
					Port:         1993,
				},
				HTTP: types.HTTPConfig{
					ExternalHost: "public.example.com",
					ExternalPort: 443,
					TLS:          true,
					Port:         1994,
				},
			},
			Worker: types.WorkerConfig{UseGatewayServiceHostname: true},
		},
	}

	workerConfig := controller.workerPodConfig()
	if got := workerConfig.GatewayService.GRPC.ExternalHost; got != "beta9-gateway" {
		t.Fatalf("worker gRPC host = %q, want in-cluster gateway", got)
	}
	if got := workerConfig.GatewayService.GRPC.ExternalPort; got != 1993 {
		t.Fatalf("worker gRPC port = %d, want 1993", got)
	}
	if workerConfig.GatewayService.GRPC.TLS {
		t.Fatal("worker in-cluster gRPC endpoint unexpectedly enables TLS")
	}
	if got := workerConfig.GatewayService.HTTP.ExternalHost; got != "beta9-gateway" {
		t.Fatalf("worker HTTP host = %q, want in-cluster gateway", got)
	}
	if got := workerConfig.GatewayService.HTTP.ExternalPort; got != 1994 {
		t.Fatalf("worker HTTP port = %d, want 1994", got)
	}
	if workerConfig.GatewayService.HTTP.TLS {
		t.Fatal("worker in-cluster HTTP endpoint unexpectedly enables TLS")
	}
}
