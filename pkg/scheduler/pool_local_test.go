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
