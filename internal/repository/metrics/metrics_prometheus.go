package metrics

import (
	"fmt"
	"log"
	"net/http"

	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/maps"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type PrometheusMetricsRepository struct {
	collectorRegistrar *prometheus.Registry
	port               int
	source             string

	// TODO: replace with safemaps
	counters      map[string]prometheus.Counter
	counterVecs   map[string]*prometheus.CounterVec
	gauges        map[string]prometheus.Gauge
	gaugeVecs     map[string]*prometheus.GaugeVec
	summaries     map[string]prometheus.Summary
	summaryVecs   map[string]*prometheus.SummaryVec
	histograms    map[string]prometheus.Histogram
	histogramVecs map[string]*prometheus.HistogramVec
}

func NewPrometheusMetricsRepository(promConfig types.PrometheusConfig) repository.MetricsRepository {
	collectorRegistrar := prometheus.NewRegistry()
	collectorRegistrar.MustRegister(
		collectors.NewGoCollector(),                                       // Metrics from Go runtime.
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), // Metrics about the current UNIX process.
	)

	return &PrometheusMetricsRepository{
		collectorRegistrar: collectorRegistrar,
		port:               promConfig.Port,
		source:             "",
		counters:           map[string]prometheus.Counter{},
		counterVecs:        map[string]*prometheus.CounterVec{},
		gauges:             map[string]prometheus.Gauge{},
		gaugeVecs:          map[string]*prometheus.GaugeVec{},
		summaries:          map[string]prometheus.Summary{},
		summaryVecs:        map[string]*prometheus.SummaryVec{},
		histograms:         map[string]prometheus.Histogram{},
		histogramVecs:      map[string]*prometheus.HistogramVec{},
	}
}

func (r *PrometheusMetricsRepository) Init(source string) error {
	go func() {
		if err := r.listenAndServe(); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	r.source = source
	return nil
}

func (pr *PrometheusMetricsRepository) IncrementCounter(name string, metadata map[string]interface{}, value float64) error {
	handler := pr.getCounterVec(
		prometheus.CounterOpts{
			Name: name,
		},
		maps.Keys(metadata),
	)

	values := make([]string, 0, len(metadata))
	for _, val := range maps.Values(metadata) {
		switch v := val.(type) {
		case string:
			values = append(values, v)
		case int, float64, bool:
			values = append(values, fmt.Sprintf("%v", v))
		default:
		}
	}

	handler.WithLabelValues(values...).Add(value)
	return nil
}

func (pr *PrometheusMetricsRepository) SetGauge(name string, metadata map[string]string, value float64) {
	handler := pr.getGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
		},
		maps.Keys(metadata),
	)

	values := maps.Values(metadata)
	handler.WithLabelValues(values...).Set(value)
}

// Internal methods

func (r *PrometheusMetricsRepository) listenAndServe() error {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(middleware.Recover())
	e.GET("/metrics", echo.WrapHandler(promhttp.HandlerFor(r.collectorRegistrar, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})))

	// Accept both HTTP/2 and HTTP/1
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%v", r.port),
		Handler: h2c.NewHandler(e, &http2.Server{}),
	}

	return httpServer.ListenAndServe()
}

// getCounter registers and returns a new counter metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusMetricsRepository) getCounter(opts prometheus.CounterOpts) prometheus.Counter {
	metricName := opts.Name
	if handler, exists := pr.counters[metricName]; exists {
		log.Printf("metric with name %s already exists", metricName)
		return handler
	}

	pr.counters[metricName] = promauto.With(pr.collectorRegistrar).NewCounter(
		opts,
	)

	return pr.counters[metricName]
}

// getCounterVec registers and returns a new counter vector metric
func (pr *PrometheusMetricsRepository) getCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	metricName := opts.Name
	if handler, exists := pr.counterVecs[metricName]; exists {
		return handler
	}

	pr.counterVecs[metricName] = promauto.With(pr.collectorRegistrar).NewCounterVec(
		opts,
		labels,
	)

	return pr.counterVecs[metricName]
}

// getGauge registers and returns a new gauge metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusMetricsRepository) getGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	metricName := opts.Name
	if handler, exists := pr.gauges[metricName]; exists {
		return handler
	}

	pr.gauges[metricName] = promauto.With(pr.collectorRegistrar).NewGauge(opts)
	return pr.gauges[metricName]
}

// getGaugeVec registers and returns a new gauge vector metric handler
func (pr *PrometheusMetricsRepository) getGaugeVec(opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	metricName := opts.Name
	if handler, exists := pr.gaugeVecs[metricName]; exists {
		return handler
	}

	pr.gaugeVecs[metricName] = promauto.With(pr.collectorRegistrar).NewGaugeVec(opts, labels)
	return pr.gaugeVecs[metricName]
}

// getSummary registers and returns a new summary metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusMetricsRepository) getSummary(opts prometheus.SummaryOpts) {
	metricName := opts.Name
	if _, exists := pr.summaries[metricName]; exists {
		return
	}

	pr.summaries[metricName] = promauto.With(pr.collectorRegistrar).NewSummary(opts)
}

// getSummaryVec registers and returns a new summary vector metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusMetricsRepository) getSummaryVec(opts prometheus.SummaryOpts, labels []string) {
	metricName := opts.Name
	if _, exists := pr.summaryVecs[metricName]; exists {
		return
	}

	pr.summaryVecs[metricName] = promauto.With(pr.collectorRegistrar).NewSummaryVec(opts, labels)
}

// getHistogram registers and returns a new histogram metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusMetricsRepository) getHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	metricName := opts.Name
	if handler, exists := pr.histograms[metricName]; exists {
		return handler
	}

	pr.histograms[metricName] = promauto.With(pr.collectorRegistrar).NewHistogram(opts)
	return pr.histograms[metricName]
}

// getHistogramVec registers and returns a new histogram vector metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusMetricsRepository) getHistogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	metricName := opts.Name
	if handler, exists := pr.histogramVecs[metricName]; exists {
		return handler
	}

	pr.histogramVecs[metricName] = promauto.With(pr.collectorRegistrar).NewHistogramVec(opts, labels)
	return pr.histogramVecs[metricName]
}
