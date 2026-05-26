package metrics

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	maps "golang.org/x/exp/maps"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type PrometheusUsageMetricsRepository struct {
	collectorRegistrar *prometheus.Registry
	port               int
	source             string
	metricMu           sync.Mutex

	counters      *common.SafeMap[prometheus.Counter]
	counterVecs   *common.SafeMap[*prometheus.CounterVec]
	gauges        *common.SafeMap[prometheus.Gauge]
	gaugeVecs     *common.SafeMap[*prometheus.GaugeVec]
	summaries     *common.SafeMap[prometheus.Summary]
	summaryVecs   *common.SafeMap[*prometheus.SummaryVec]
	histograms    *common.SafeMap[prometheus.Histogram]
	histogramVecs *common.SafeMap[*prometheus.HistogramVec]
}

func getOrCreateMetric[T any](mu *sync.Mutex, metrics *common.SafeMap[T], metricName string, create func() T) T {
	if handler, exists := metrics.Get(metricName); exists {
		return handler
	}

	mu.Lock()
	defer mu.Unlock()
	if handler, exists := metrics.Get(metricName); exists {
		return handler
	}

	handler := create()
	metrics.Set(metricName, handler)
	return handler
}

func NewPrometheusUsageMetricsRepository(promConfig types.PrometheusConfig) repository.UsageMetricsRepository {
	collectorRegistrar := prometheus.NewRegistry()
	collectorRegistrar.MustRegister(
		collectors.NewGoCollector(),                                       // Metrics from Go runtime.
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), // Metrics about the current UNIX process.
	)

	return &PrometheusUsageMetricsRepository{
		collectorRegistrar: collectorRegistrar,
		port:               promConfig.Port,
		source:             "",
		counters:           common.NewSafeMap[prometheus.Counter](),
		counterVecs:        common.NewSafeMap[*prometheus.CounterVec](),
		gauges:             common.NewSafeMap[prometheus.Gauge](),
		gaugeVecs:          common.NewSafeMap[*prometheus.GaugeVec](),
		summaries:          common.NewSafeMap[prometheus.Summary](),
		summaryVecs:        common.NewSafeMap[*prometheus.SummaryVec](),
		histograms:         common.NewSafeMap[prometheus.Histogram](),
		histogramVecs:      common.NewSafeMap[*prometheus.HistogramVec](),
	}
}

func (r *PrometheusUsageMetricsRepository) Init(source string) error {
	r.source = source

	go func() {
		if err := r.listenAndServe(); err != nil {
			log.Error().Err(err).Msg("failed to start metrics server")
			os.Exit(1)
		}
	}()

	return nil
}

func (pr *PrometheusUsageMetricsRepository) IncrementCounter(name string, metadata map[string]interface{}, value float64) error {
	keys, values := pr.parseMetadata(metadata)

	handler := pr.getCounterVec(
		prometheus.CounterOpts{
			Name: name,
		},
		keys,
	)

	handler.WithLabelValues(values...).Add(value)
	return nil
}

func (pr *PrometheusUsageMetricsRepository) SetGauge(name string, metadata map[string]interface{}, value float64) error {
	keys, values := pr.parseMetadata(metadata)

	handler := pr.getGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
		},
		keys,
	)

	handler.WithLabelValues(values...).Set(value)
	return nil
}

// Internal methods

func (r *PrometheusUsageMetricsRepository) listenAndServe() error {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(middleware.Recover())
	e.GET("/metrics", echo.WrapHandler(r.metricsHandler()))

	// Accept both HTTP/2 and HTTP/1
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%v", r.port),
		Handler: h2c.NewHandler(e, &http2.Server{}),
	}

	return httpServer.ListenAndServe()
}

func (r *PrometheusUsageMetricsRepository) metricsHandler() http.Handler {
	prometheusHandler := promhttp.HandlerFor(r.collectorRegistrar, promhttp.HandlerOpts{})

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		prometheusHandler.ServeHTTP(w, req)
		_, _ = io.WriteString(w, "\n")
		vmetrics.WritePrometheus(w, false)
	})
}

// getCounter registers and returns a new counter metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getCounter(opts prometheus.CounterOpts) prometheus.Counter {
	return getOrCreateMetric(&pr.metricMu, pr.counters, opts.Name, func() prometheus.Counter {
		return promauto.With(pr.collectorRegistrar).NewCounter(opts)
	})
}

// getCounterVec registers and returns a new counter vector metric
func (pr *PrometheusUsageMetricsRepository) getCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	return getOrCreateMetric(&pr.metricMu, pr.counterVecs, opts.Name, func() *prometheus.CounterVec {
		return promauto.With(pr.collectorRegistrar).NewCounterVec(opts, labels)
	})
}

// getGauge registers and returns a new gauge metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	return getOrCreateMetric(&pr.metricMu, pr.gauges, opts.Name, func() prometheus.Gauge {
		return promauto.With(pr.collectorRegistrar).NewGauge(opts)
	})
}

// getGaugeVec registers and returns a new gauge vector metric handler
func (pr *PrometheusUsageMetricsRepository) getGaugeVec(opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	return getOrCreateMetric(&pr.metricMu, pr.gaugeVecs, opts.Name, func() *prometheus.GaugeVec {
		return promauto.With(pr.collectorRegistrar).NewGaugeVec(opts, labels)
	})
}

// getSummary registers and returns a new summary metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getSummary(opts prometheus.SummaryOpts) prometheus.Summary {
	return getOrCreateMetric(&pr.metricMu, pr.summaries, opts.Name, func() prometheus.Summary {
		return promauto.With(pr.collectorRegistrar).NewSummary(opts)
	})
}

// getSummaryVec registers and returns a new summary vector metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getSummaryVec(opts prometheus.SummaryOpts, labels []string) *prometheus.SummaryVec {
	return getOrCreateMetric(&pr.metricMu, pr.summaryVecs, opts.Name, func() *prometheus.SummaryVec {
		return promauto.With(pr.collectorRegistrar).NewSummaryVec(opts, labels)
	})
}

// getHistogram registers and returns a new histogram metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	return getOrCreateMetric(&pr.metricMu, pr.histograms, opts.Name, func() prometheus.Histogram {
		return promauto.With(pr.collectorRegistrar).NewHistogram(opts)
	})
}

// getHistogramVec registers and returns a new histogram vector metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getHistogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	return getOrCreateMetric(&pr.metricMu, pr.histogramVecs, opts.Name, func() *prometheus.HistogramVec {
		return promauto.With(pr.collectorRegistrar).NewHistogramVec(opts, labels)
	})
}

func (pr *PrometheusUsageMetricsRepository) parseMetadata(metadata map[string]interface{}) (keys []string, values []string) {
	keys = maps.Keys(metadata)
	values = []string{}
	sort.Strings(keys)

	for _, key := range keys {
		val := metadata[key]

		switch v := val.(type) {
		case string:
			values = append(values, v)
		default:
			values = append(values, fmt.Sprintf("%v", v))
		}
	}

	return keys, values
}
