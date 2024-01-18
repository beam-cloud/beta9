package repository

import (
	"context"
	"log"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var PrometheusMetricsAddress = "0.0.0.0:9090"

type PrometheusRepository struct {
	registry          *prometheus.Registry
	registeredMetrics map[string]prometheus.Collector
	ctx               *context.Context
	enabled           bool
}

func NewMetricsPrometheusRepository(enabled bool) *PrometheusRepository {
	if !enabled {
		return &PrometheusRepository{
			enabled: false,
		}
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),                                       // Metrics from Go runtime.
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), // Metrics about the current UNIX process.
	)

	return &PrometheusRepository{
		registry:          reg,
		registeredMetrics: map[string]prometheus.Collector{},
		enabled:           true,
	}
}

func (r *PrometheusRepository) Init() error {
	if !r.enabled {
		return nil
	}

	prometheusServer := echo.New()
	prometheusServer.HideBanner = true
	prometheusServer.HidePort = true

	prometheusServer.Use(middleware.Recover())

	prometheusServer.GET("/metrics", echo.WrapHandler(promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})))

	log.Printf("Starting Prometheus metrics server at %s\n", PrometheusMetricsAddress)

	return prometheusServer.Start(PrometheusMetricsAddress)
}

func (r *PrometheusRepository) ContainerDurationSeconds(containerId string, workerId string, duration time.Duration) {
	if !r.enabled {
		return
	}

	var metricName = "container_duration_seconds"
	var collector prometheus.Collector
	collector, exists := r.registeredMetrics[metricName]

	if !exists {
		collector = promauto.With(r.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: metricName,
			},
			[]string{"container_id", "worker_id"},
		)

		r.registeredMetrics[metricName] = collector
	}

	handle := collector.(*prometheus.GaugeVec)
	handle.WithLabelValues(containerId, workerId).Add(duration.Seconds())
}
