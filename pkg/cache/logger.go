package cache

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type logger struct {
	logger zerolog.Logger
	debug  bool
}

var (
	Logger *logger
	once   sync.Once
)

func InitLogger(debugMode bool, prettyLogs bool) {
	once.Do(func() {
		var output io.Writer = os.Stderr

		// Configure Zerolog
		zerolog.TimeFieldFormat = time.RFC3339
		logLevel := zerolog.InfoLevel
		if debugMode {
			logLevel = zerolog.DebugLevel
		}

		if prettyLogs {
			output = zerolog.ConsoleWriter{Out: os.Stdout}
		}

		zerologLogger := zerolog.New(output).
			Level(logLevel).
			With().
			Timestamp().
			Logger()

		Logger = &logger{
			logger: zerologLogger,
			debug:  debugMode,
		}
	})
}

func GetLogger() *logger {
	if Logger == nil {
		panic("Logger is not initialized. Call InitLogger first.")
	}
	return Logger
}

func (l *logger) Debug(msg string, fields ...any) {
	if l.debug {
		event := l.logger.Debug()
		l.addFields(event, fields...).Msg(msg)
	}
}

func (l *logger) Debugf(template string, args ...interface{}) {
	if l.debug {
		l.logger.Debug().Msgf(template, args...)
	}
}

func (l *logger) Info(msg string, fields ...any) {
	event := l.logger.Info()
	l.addFields(event, fields...).Msg(msg)
}

func (l *logger) Infof(template string, args ...interface{}) {
	l.logger.Info().Msgf(template, args...)
}

func (l *logger) Warn(msg string, fields ...any) {
	event := l.logger.Warn()
	l.addFields(event, fields...).Msg(msg)
}

func (l *logger) Warnf(template string, args ...interface{}) {
	l.logger.Warn().Msgf(template, args...)
}

func (l *logger) Error(msg string, fields ...any) {
	event := l.logger.Error()
	l.addFields(event, fields...).Msg(msg)
}

func (l *logger) Errorf(template string, args ...interface{}) {
	if l == nil {
		return
	}
	l.logger.Error().Msgf(template, args...)
}

func (l *logger) Fatal(msg string, fields ...any) {
	event := l.logger.Fatal()
	l.addFields(event, fields...).Msg(msg)
}

func (l *logger) Fatalf(template string, args ...interface{}) {
	l.logger.Fatal().Msgf(template, args...)
}

func (l *logger) addFields(event *zerolog.Event, fields ...any) *zerolog.Event {
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key, ok := fields[i].(string)
			if !ok {
				continue
			}
			event = event.Interface(key, fields[i+1])
		}
	}
	return event
}
