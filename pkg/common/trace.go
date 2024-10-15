package common

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	_trace "go.opentelemetry.io/otel/trace"
)

type Tracer struct {
	Ctx        context.Context
	tracerName string
	spanName   string
	debugMode  bool
	attributes []attribute.KeyValue
	Span       _trace.Span
}

func (t *Tracer) End() {
	if t.debugMode {
		t.Span.End()
	}
}

func TraceFunc(ctx context.Context, tracerName, spanName string, debugMode bool, attributes ...attribute.KeyValue) *Tracer {
	if debugMode {
		tracer := otel.Tracer(tracerName)
		ctx, span := tracer.Start(ctx, spanName)
		span.SetAttributes(attributes...)
		return &Tracer{Ctx: ctx, Span: span, tracerName: tracerName, spanName: spanName, debugMode: debugMode, attributes: attributes}
	}

	return &Tracer{
		Ctx: ctx,
	}
}

// SetupTelemetry bootstraps the OpenTelemetry pipeline
func SetupTelemetry(ctx context.Context, serviceName string, appConfig types.AppConfig) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	tracerProvider, err := newTraceProvider(res, appConfig)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	meterProvider, err := newMeterProvider(res, appConfig)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	loggerProvider, err := newLoggerProvider(res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTraceProvider(res *resource.Resource, appConfig types.AppConfig) (*trace.TracerProvider, error) {
	var err error
	var traceExporter *otlptrace.Exporter

	parsedURL, err := url.Parse(appConfig.Monitoring.TelemetryConfig.Endpoint)
	if err != nil {
		return nil, err
	}
	host := parsedURL.Hostname()
	port := parsedURL.Port()
	endpoint := host + ":" + port

	if strings.HasPrefix(endpoint, "https") {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}
		traceExporter, err = otlptracehttp.New(context.Background(),
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithTLSClientConfig(tlsConfig),
		)
	} else {
		traceExporter, err = otlptracehttp.New(context.Background(),
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithInsecure(),
		)
	}
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter,
			trace.WithBatchTimeout(appConfig.Monitoring.TelemetryConfig.TraceInterval)),
		trace.WithSampler(trace.TraceIDRatioBased(appConfig.Monitoring.TelemetryConfig.TraceSampleRatio)),
		trace.WithResource(res),
	)
	return traceProvider, nil
}

func newMeterProvider(res *resource.Resource, appConfig types.AppConfig) (*metric.MeterProvider, error) {
	metricExporter, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			metric.WithInterval(appConfig.Monitoring.TelemetryConfig.MeterInterval))),
		metric.WithResource(res),
	)
	return meterProvider, nil
}

func newLoggerProvider(res *resource.Resource) (*log.LoggerProvider, error) {
	logExporter, err := stdoutlog.New()
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
		log.WithResource(res),
	)
	return loggerProvider, nil
}
