package common

import (
	"context"
	"io"
	"log/slog"
)

type OutputMsg struct {
	Msg       string
	Done      bool
	Success   bool
	Archiving bool

	ImageId string
}

type OutputWriter struct {
	outputCallback func(string)
}

func NewOutputWriter(outputCallback func(string)) *OutputWriter {
	return &OutputWriter{
		outputCallback: outputCallback,
	}
}

func (w *OutputWriter) Write(p []byte) (n int, err error) {
	if w.outputCallback != nil {
		w.outputCallback(string(p))
	}
	return len(p), nil
}

func NewChannelSlogger() *slog.Logger {
	return slog.New(NewChannelHandler(make(chan LogRecord)))
}

type LogRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

// ChannelHandler implements slog.Handler to send logs to a channel
type ChannelHandler struct {
	logChan chan LogRecord
}

// NewChannelHandler creates a new handler that sends logs to the provided channel
func NewChannelHandler(logChan chan LogRecord) *ChannelHandler {
	return &ChannelHandler{
		logChan: logChan,
	}
}

// Handle implements slog.Handler
func (h *ChannelHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})

	h.logChan <- LogRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   attrs,
	}
	return nil
}

func (h *ChannelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *ChannelHandler) WithGroup(name string) slog.Handler {
	return h
}

func (h *ChannelHandler) Enabled(_ context.Context, level slog.Level) bool {
	return true
}

// OrderedJSONHandler ensures that the "msg" field is always last in the JSON output so that context
// like "event_type" and "container_id" come first.
type OrderedJSONHandler struct {
	wrapped *slog.JSONHandler
}

func NewOrderedJSONHandler(w io.Writer, opts *slog.HandlerOptions) *OrderedJSONHandler {
	return &OrderedJSONHandler{
		wrapped: slog.NewJSONHandler(w, &slog.HandlerOptions{
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == "msg" {
					return slog.Attr{}
				}
				return a
			},
		}),
	}
}

func (h *OrderedJSONHandler) Handle(ctx context.Context, r slog.Record) error {
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != "msg" {
			attrs = append(attrs, a)
		}
		return true
	})

	attrs = append(attrs, slog.String("message", r.Message))

	newRecord := slog.NewRecord(r.Time, r.Level, "", r.PC)
	for _, attr := range attrs {
		newRecord.AddAttrs(attr)
	}

	return h.wrapped.Handle(ctx, newRecord)
}

func (h *OrderedJSONHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &OrderedJSONHandler{wrapped: h.wrapped.WithAttrs(attrs).(*slog.JSONHandler)}
}

func (h *OrderedJSONHandler) WithGroup(name string) slog.Handler {
	return &OrderedJSONHandler{wrapped: h.wrapped.WithGroup(name).(*slog.JSONHandler)}
}

func (h *OrderedJSONHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.wrapped.Enabled(ctx, level)
}
