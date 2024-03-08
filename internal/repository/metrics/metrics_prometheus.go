package metrics

import (
	"fmt"
	"log"
	"net/http"
	"sync"

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
	once               sync.Once
	collectorRegistrar *prometheus.Registry
	port               int
	counters           map[string]prometheus.Counter
	counterVecs        map[string]*prometheus.CounterVec
	gauges             map[string]prometheus.Gauge
	gaugeVecs          map[string]*prometheus.GaugeVec
	summaries          map[string]prometheus.Summary
	summaryVecs        map[string]*prometheus.SummaryVec
	histograms         map[string]prometheus.Histogram
	histogramVecs      map[string]*prometheus.HistogramVec
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

func (r *PrometheusMetricsRepository) Init() error {
	go func() {
		if err := r.listenAndServe(); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	log.Println("Prometheus metrics server running @", r.port)
	return nil
}

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

func (pr *PrometheusMetricsRepository) AddToCounter(name string, metadata map[string]string, value float64) {
	pr.registerCounterVec(
		prometheus.CounterOpts{
			Name: name,
		},
		maps.Keys(metadata), // Labels
	)

	values := maps.Values(metadata)
	if handler := pr.getCounterVecHandler(name); handler != nil {
		handler.WithLabelValues(values...).Add(value)
	}
}

func (pr *PrometheusMetricsRepository) IncrementGauge(name string, metadata []string) {

}

// Internal methods

// RegisterCounter registers a new counter metric
func (pr *PrometheusMetricsRepository) registerCounter(opts prometheus.CounterOpts) {
	metricName := opts.Name
	if _, exist := pr.counters[metricName]; exist {
		log.Printf("metric with name %s already exists", metricName)
		return
	}

	pr.counters[metricName] = promauto.With(pr.collectorRegistrar).NewCounter(
		opts,
	)
}

// RegisterCounterVec registers a new counter vector metric
func (pr *PrometheusMetricsRepository) registerCounterVec(opts prometheus.CounterOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.counterVecs[metricName]; exist {
		log.Printf("metric with name %s already exists", metricName)
		return
	}

	pr.counterVecs[metricName] = promauto.With(pr.collectorRegistrar).NewCounterVec(
		opts,
		labels,
	)
}

// RegisterGauge registers a new gauge metric
func (pr *PrometheusMetricsRepository) registerGauge(opts prometheus.GaugeOpts) {
	metricName := opts.Name
	if _, exist := pr.gauges[metricName]; exist {
		log.Fatalf("gauge with name %s already exists", metricName)
	}

	pr.gauges[metricName] = promauto.With(pr.collectorRegistrar).NewGauge(opts)
}

// RegisterGaugeVec registers a new gauge vector metric
func (pr *PrometheusMetricsRepository) registerGaugeVec(opts prometheus.GaugeOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.gaugeVecs[metricName]; exist {
		log.Fatalf("gauge vector with name %s already exists", metricName)
	}

	pr.gaugeVecs[metricName] = promauto.With(pr.collectorRegistrar).NewGaugeVec(opts, labels)
}

// RegisterSummary registers a new summary metric
func (pr *PrometheusMetricsRepository) registerSummary(opts prometheus.SummaryOpts) {
	metricName := opts.Name
	if _, exist := pr.summaries[metricName]; exist {
		log.Fatalf("summary with name %s already exists", metricName)
	}

	pr.summaries[metricName] = promauto.With(pr.collectorRegistrar).NewSummary(opts)
}

// RegisterSummaryVec registers a new summary vector metric
func (pr *PrometheusMetricsRepository) registerSummaryVec(opts prometheus.SummaryOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.summaryVecs[metricName]; exist {
		log.Fatalf("summary vector with name %s already exists", metricName)
	}

	pr.summaryVecs[metricName] = promauto.With(pr.collectorRegistrar).NewSummaryVec(opts, labels)
}

// RegisterHistogram registers a new histogram metric
func (pr *PrometheusMetricsRepository) registerHistogram(opts prometheus.HistogramOpts) {
	metricName := opts.Name
	if _, exist := pr.histograms[metricName]; exist {
		log.Fatalf("histogram with name %s already exists", metricName)
	}

	pr.histograms[metricName] = promauto.With(pr.collectorRegistrar).NewHistogram(opts)
}

// RegisterHistogramVec registers a new histogram vector metric
func (pr *PrometheusMetricsRepository) registerHistogramVec(opts prometheus.HistogramOpts, labels []string) {
	metricName := opts.Name
	if _, exist := pr.histogramVecs[metricName]; exist {
		log.Fatalf("histogram vector with name %s already exists", metricName)
	}

	pr.histogramVecs[metricName] = promauto.With(pr.collectorRegistrar).NewHistogramVec(opts, labels)
}

// GetCounterHandler retrieves a counter by name
func (pr *PrometheusMetricsRepository) getCounterHandler(metricName string) prometheus.Counter {
	handler, exists := pr.counters[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetGaugeHandler retrieves a gauge by name
func (pr *PrometheusMetricsRepository) getGaugeHandler(metricName string) prometheus.Gauge {
	handler, exists := pr.gauges[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetCounterVecHandler retrieves a counter vector by name
func (pr *PrometheusMetricsRepository) getCounterVecHandler(metricName string) *prometheus.CounterVec {
	handler, exists := pr.counterVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetGaugeVecHandler retrieves a gauge vector by name
func (pr *PrometheusMetricsRepository) getGaugeVecHandler(metricName string) *prometheus.GaugeVec {
	handler, exists := pr.gaugeVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetSummaryHandler retrieves a summary by name
func (pr *PrometheusMetricsRepository) getSummaryHandler(metricName string) prometheus.Summary {
	handler, exists := pr.summaries[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetSummaryVecHandler retrieves a summary vector by name
func (pr *PrometheusMetricsRepository) getSummaryVecHandler(metricName string) *prometheus.SummaryVec {
	handler, exists := pr.summaryVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetHistogramHandler retrieves a histogram by name
func (pr *PrometheusMetricsRepository) getHistogramHandler(metricName string) prometheus.Histogram {
	handler, exists := pr.histograms[metricName]
	if !exists {
		return nil
	}
	return handler
}

// GetHistogramVecHandler retrieves a histogram vector by name
func (pr *PrometheusMetricsRepository) getHistogramVecHandler(metricName string) *prometheus.HistogramVec {
	handler, exists := pr.histogramVecs[metricName]
	if !exists {
		return nil
	}
	return handler
}
