package common

import (
	"sync"

	"github.com/beam-cloud/beta9/pkg/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TraceLogger struct {
	logger *zap.Logger
	sugar  *zap.SugaredLogger
	config types.AppConfig
}

var (
	Logger *TraceLogger
	once   sync.Once
)

func InitializeLogger(config types.AppConfig) error {
	var err error

	cfg := zap.Config{
		Encoding:         "console",
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}

	once.Do(func() {
		var e error
		logger, e := cfg.Build(zap.AddCaller(), zap.AddCallerSkip(1)) // Adjusted caller skip to 1
		if e != nil {
			err = e
			return
		}
		Logger = &TraceLogger{
			logger: logger,
			sugar:  logger.Sugar(),
			config: config,
		}
	})

	return err
}

func (tl *TraceLogger) Debugf(template string, args ...interface{}) {
	tl.sugar.Debugf(template, args...)
}

func (tl *TraceLogger) Infof(template string, args ...interface{}) {
	tl.sugar.Infof(template, args...)
}

func (tl *TraceLogger) Warnf(template string, args ...interface{}) {
	tl.sugar.Warnf(template, args...)
}

func (tl *TraceLogger) Errorf(template string, args ...interface{}) {
	tl.sugar.Errorf(template, args...)
}

func (tl *TraceLogger) Fatalf(template string, args ...interface{}) {
	tl.sugar.Fatalf(template, args...)
}
