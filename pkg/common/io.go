package common

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/rs/zerolog"
)

type OutputMsg struct {
	Msg       string
	Done      bool
	Success   bool
	Archiving bool
	Warning   bool

	ImageId       string
	PythonVersion string
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

type ZerologIOWriter struct {
	LogFn func() *zerolog.Event
}

func (w *ZerologIOWriter) Write(p []byte) (n int, err error) {
	w.LogFn().Msg(strings.TrimSpace(string(p)))
	return len(p), nil
}

type ExecWriter struct {
	*slog.Logger
}

func (c *ExecWriter) Write(p []byte) (n int, err error) {
	c.Info(string(p))
	return len(p), nil
}

type SafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *SafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	return n, err
}

func (b *SafeBuffer) StringAndReset() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.buf.String()
	b.buf.Reset()
	return s
}
