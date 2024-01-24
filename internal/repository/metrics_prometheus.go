package repository

import (
	"log"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var PrometheusMetricsAddress = "0.0.0.0:9090"

type PrometheusRepository struct {
	collectorRegistrar *prometheus.Registry
	counters           map[string]prometheus.Counter
	counterVecs        map[string]*prometheus.CounterVec
	gauges             map[string]prometheus.Gauge
	gaugeVecs          map[string]*prometheus.GaugeVec
	summaries          map[string]prometheus.Summary
	summaryVecs        map[string]*prometheus.SummaryVec
	histograms         map[string]prometheus.Histogram
	histogramVecs      map[string]*prometheus.HistogramVec
}

func NewMetricsPrometheusRepository() PrometheusRepository {
	collectorRegistrar := prometheus.NewRegistry()
	collectorRegistrar.MustRegister(
		collectors.NewGoCollector(),                                       // Metrics from Go runtime.
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), // Metrics about the current UNIX process.
	)

	return PrometheusRepository{
		collectorRegistrar: collectorRegistrar,
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

func (r *PrometheusRepository) Init() error {
	prometheusServer := echo.New()
	prometheusServer.HideBanner = true
	prometheusServer.HidePort = true
	prometheusServer.Use(middleware.Recover())
	prometheusServer.GET("/metrics", echo.WrapHandler(promhttp.HandlerFor(r.collectorRegistrar, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})))

	log.Printf("Starting Prometheus metrics server at %s\n", PrometheusMetricsAddress)

	return prometheusServer.Start(PrometheusMetricsAddress)
}

// RegisterCounter registers a new counter metric
func (pr *PrometheusRepository) RegisterCounter(opts prometheus.CounterOpts) {
	metricName := opts.Name
	if _, exist := pr.counters[metricName]; exist == true {
		log.Printf("metric with name %s already exists", metricName)
		return
	}

	pr.counters[metricName] = promauto.With(pr.collectorRegistrar).NewCounter(
		opts,
	)
}

// RegisterCounterVec registers a new counter vector metric
func (pr *PrometheusRepository) RegisterCounterVec(opts prometheus.CounterOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.counterVecs[metricName]; exist == true {
		log.Printf("metric with name %s already exists", metricName)
		return
	}

	pr.counterVecs[metricName] = promauto.With(pr.collectorRegistrar).NewCounterVec(
		opts,
		labels,
	)
}

// RegisterGauge registers a new gauge metric
func (pr *PrometheusRepository) RegisterGauge(opts prometheus.GaugeOpts) {
	metricName := opts.Name
	if _, exist := pr.gauges[metricName]; exist {
		log.Fatalf("gauge with name %s already exists", metricName)
	}

	pr.gauges[metricName] = promauto.With(pr.collectorRegistrar).NewGauge(opts)
}

// RegisterGaugeVec registers a new gauge vector metric
func (pr *PrometheusRepository) RegisterGaugeVec(opts prometheus.GaugeOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.gaugeVecs[metricName]; exist {
		log.Fatalf("gauge vector with name %s already exists", metricName)
	}

	pr.gaugeVecs[metricName] = promauto.With(pr.collectorRegistrar).NewGaugeVec(opts, labels)
}

// RegisterSummary registers a new summary metric
func (pr *PrometheusRepository) RegisterSummary(opts prometheus.SummaryOpts) {
	metricName := opts.Name
	if _, exist := pr.summaries[metricName]; exist {
		log.Fatalf("summary with name %s already exists", metricName)
	}

	pr.summaries[metricName] = promauto.With(pr.collectorRegistrar).NewSummary(opts)
}

// RegisterSummaryVec registers a new summary vector metric
func (pr *PrometheusRepository) RegisterSummaryVec(opts prometheus.SummaryOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.summaryVecs[metricName]; exist {
		log.Fatalf("summary vector with name %s already exists", metricName)
	}

	pr.summaryVecs[metricName] = promauto.With(pr.collectorRegistrar).NewSummaryVec(opts, labels)
}

// RegisterHistogram registers a new histogram metric
func (pr *PrometheusRepository) RegisterHistogram(opts prometheus.HistogramOpts) {
	metricName := opts.Name
	if _, exist := pr.histograms[metricName]; exist {
		log.Fatalf("histogram with name %s already exists", metricName)
	}

	pr.histograms[metricName] = promauto.With(pr.collectorRegistrar).NewHistogram(opts)
}

// RegisterHistogramVec registers a new histogram vector metric
func (pr *PrometheusRepository) RegisterHistogramVec(opts prometheus.HistogramOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.histogramVecs[metricName]; exist {
		log.Fatalf("histogram vector with name %s already exists", metricName)
	}

	pr.histogramVecs[metricName] = promauto.With(pr.collectorRegistrar).NewHistogramVec(opts, labels)
}

// GetCounterHandler retrieves a counter by name
func (pr *PrometheusRepository) GetCounterHandler(metricName string) prometheus.Counter {
	handler, exists := pr.counters[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetGaugeHandler retrieves a gauge by name
func (pr *PrometheusRepository) GetGaugeHandler(metricName string) prometheus.Gauge {
	handler, exists := pr.gauges[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetCounterVecHandler retrieves a counter vector by name
func (pr *PrometheusRepository) GetCounterVecHandler(metricName string) *prometheus.CounterVec {
	handler, exists := pr.counterVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetGaugeVecHandler retrieves a gauge vector by name
func (pr *PrometheusRepository) GetGaugeVecHandler(metricName string) *prometheus.GaugeVec {
	handler, exists := pr.gaugeVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetSummaryHandler retrieves a summary by name
func (pr *PrometheusRepository) GetSummaryHandler(metricName string) prometheus.Summary {
	handler, exists := pr.summaries[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetSummaryVecHandler retrieves a summary vector by name
func (pr *PrometheusRepository) GetSummaryVecHandler(metricName string) *prometheus.SummaryVec {
	handler, exists := pr.summaryVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetHistogramHandler retrieves a histogram by name
func (pr *PrometheusRepository) GetHistogramHandler(metricName string) prometheus.Histogram {
	handler, exists := pr.histograms[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetHistogramVecHandler retrieves a histogram vector by name
func (pr *PrometheusRepository) GetHistogramVecHandler(metricName string) *prometheus.HistogramVec {
	handler, exists := pr.histogramVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}
