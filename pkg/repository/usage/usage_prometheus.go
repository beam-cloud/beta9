package metrics

import (
	"fmt"
	"net/http"
	"os"
	"sort"

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

	counters      *common.SafeMap[prometheus.Counter]
	counterVecs   *common.SafeMap[*prometheus.CounterVec]
	gauges        *common.SafeMap[prometheus.Gauge]
	gaugeVecs     *common.SafeMap[*prometheus.GaugeVec]
	summaries     *common.SafeMap[prometheus.Summary]
	summaryVecs   *common.SafeMap[*prometheus.SummaryVec]
	histograms    *common.SafeMap[prometheus.Histogram]
	histogramVecs *common.SafeMap[*prometheus.HistogramVec]
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
func (pr *PrometheusUsageMetricsRepository) getCounter(opts prometheus.CounterOpts) prometheus.Counter {
	metricName := opts.Name
	if handler, exists := pr.counters.Get(metricName); exists {
		return handler
	}

	pr.counters.Set(metricName, promauto.With(pr.collectorRegistrar).NewCounter(
		opts,
	))

	handler, _ := pr.counters.Get(metricName)
	return handler
}

// getCounterVec registers and returns a new counter vector metric
func (pr *PrometheusUsageMetricsRepository) getCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	metricName := opts.Name
	if handler, exists := pr.counterVecs.Get(metricName); exists {
		return handler
	}

	counterVec := promauto.With(pr.collectorRegistrar).NewCounterVec(
		opts,
		labels,
	)
	pr.counterVecs.Set(metricName, counterVec)

	handler, _ := pr.counterVecs.Get(metricName)
	return handler
}

// getGauge registers and returns a new gauge metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	metricName := opts.Name
	if handler, exists := pr.gauges.Get(metricName); exists {
		return handler
	}

	gauge := promauto.With(pr.collectorRegistrar).NewGauge(opts)
	pr.gauges.Set(metricName, gauge)

	handler, _ := pr.gauges.Get(metricName)
	return handler
}

// getGaugeVec registers and returns a new gauge vector metric handler
func (pr *PrometheusUsageMetricsRepository) getGaugeVec(opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	metricName := opts.Name
	if handler, exists := pr.gaugeVecs.Get(metricName); exists {
		return handler
	}

	gaugeVec := promauto.With(pr.collectorRegistrar).NewGaugeVec(opts, labels)
	pr.gaugeVecs.Set(metricName, gaugeVec)

	handler, _ := pr.gaugeVecs.Get(metricName)
	return handler
}

// getSummary registers and returns a new summary metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getSummary(opts prometheus.SummaryOpts) prometheus.Summary {
	metricName := opts.Name
	if handler, exists := pr.summaries.Get(metricName); exists {
		return handler
	}

	summary := promauto.With(pr.collectorRegistrar).NewSummary(opts)
	pr.summaries.Set(metricName, summary)

	handler, _ := pr.summaries.Get(metricName)
	return handler
}

// getSummaryVec registers and returns a new summary vector metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getSummaryVec(opts prometheus.SummaryOpts, labels []string) *prometheus.SummaryVec {
	metricName := opts.Name
	if handler, exists := pr.summaryVecs.Get(metricName); exists {
		return handler
	}

	summaryVec := promauto.With(pr.collectorRegistrar).NewSummaryVec(opts, labels)
	pr.summaryVecs.Set(metricName, summaryVec)

	handler, _ := pr.summaryVecs.Get(metricName)
	return handler
}

// getHistogram registers and returns a new histogram metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	metricName := opts.Name
	if handler, exists := pr.histograms.Get(metricName); exists {
		return handler
	}

	histogram := promauto.With(pr.collectorRegistrar).NewHistogram(opts)
	pr.histograms.Set(metricName, histogram)

	handler, _ := pr.histograms.Get(metricName)
	return handler
}

// getHistogramVec registers and returns a new histogram vector metric handler
//
//lint:ignore U1000 This function is reserved for future use.
func (pr *PrometheusUsageMetricsRepository) getHistogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	metricName := opts.Name
	if handler, exists := pr.histogramVecs.Get(metricName); exists {
		return handler
	}

	histogramVec := promauto.With(pr.collectorRegistrar).NewHistogramVec(opts, labels)
	pr.histogramVecs.Set(metricName, histogramVec)

	handler, _ := pr.histogramVecs.Get(metricName)
	return handler
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
