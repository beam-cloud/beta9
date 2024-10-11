package common

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TraceLogger struct {
	logger    *zap.Logger
	sugar     *zap.SugaredLogger
	config    types.AppConfig
	debugRepo DebugRepository
}

var (
	Logger *TraceLogger
	once   sync.Once
)

func InitializeLogger(rdb *RedisClient, config types.AppConfig) error {
	var err error

	logLevel := zap.InfoLevel
	if config.DebugMode {
		logLevel = zap.DebugLevel
	}

	cfg := zap.Config{
		Encoding:         "console",
		Level:            zap.NewAtomicLevelAt(logLevel),
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
		logger, e := cfg.Build(zap.AddCaller(), zap.AddCallerSkip(1))
		if e != nil {
			err = e
			return
		}

		var debugRepo DebugRepository
		if config.DebugMode {
			debugRepo = NewDebugRedisRepository(rdb)
		}

		Logger = &TraceLogger{
			logger:    logger,
			sugar:     logger.Sugar(),
			config:    config,
			debugRepo: debugRepo,
		}
	})

	return err
}

type traceIdKey string

func (tl *TraceLogger) tracedContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, traceIdKey("traceId"), uuid.New().String())
}

func (tl *TraceLogger) Debugf(ctx context.Context, template string, args ...interface{}) context.Context {
	if tl.config.DebugMode {
		ctx = tl.tracedContext(ctx)

		tl.debugRepo.PushLog(ctx, ctx.Value(traceIdKey("traceId")).(string), map[string]interface{}{
			"msg": fmt.Sprintf(template, args...),
		})
	}

	tl.sugar.Debugf(template, args...)

	return ctx
}

func (tl *TraceLogger) Infof(ctx context.Context, template string, args ...interface{}) context.Context {
	if tl.config.DebugMode {
		ctx = tl.tracedContext(ctx)

		tl.debugRepo.PushLog(ctx, ctx.Value(traceIdKey("traceId")).(string), map[string]interface{}{
			"msg": fmt.Sprintf(template, args...),
		})
	}

	tl.sugar.Infof(template, args...)
	return ctx
}

func (tl *TraceLogger) Warnf(ctx context.Context, template string, args ...interface{}) context.Context {
	if tl.config.DebugMode {
		ctx = tl.tracedContext(ctx)

		tl.debugRepo.PushLog(ctx, ctx.Value(traceIdKey("traceId")).(string), map[string]interface{}{
			"msg": fmt.Sprintf(template, args...),
		})
	}

	tl.sugar.Warnf(template, args...)
	return ctx
}

func (tl *TraceLogger) Errorf(ctx context.Context, template string, args ...interface{}) context.Context {
	if tl.config.DebugMode {
		ctx = tl.tracedContext(ctx)

		tl.debugRepo.PushLog(ctx, ctx.Value(traceIdKey("traceId")).(string), map[string]interface{}{
			"msg": fmt.Sprintf(template, args...),
		})
	}

	tl.sugar.Errorf(template, args...)
	return ctx
}

func (tl *TraceLogger) Fatalf(ctx context.Context, template string, args ...interface{}) context.Context {
	if tl.config.DebugMode {
		ctx = tl.tracedContext(ctx)

		tl.debugRepo.PushLog(ctx, ctx.Value(traceIdKey("traceId")).(string), map[string]interface{}{
			"msg": fmt.Sprintf(template, args...),
		})
	}

	tl.sugar.Fatalf(template, args...)
	return ctx
}

type DebugRedisRepository struct {
	rdb  *RedisClient
	lock *RedisLock
}

type DebugRepository interface {
	PushLog(ctx context.Context, traceId string, data interface{}) error
}

func NewDebugRedisRepository(r *RedisClient) DebugRepository {
	lock := NewRedisLock(r)
	return &DebugRedisRepository{rdb: r, lock: lock}
}

type tracedLog struct {
	TraceId string      `json:"traceId"`
	Data    interface{} `json:"data"`
}

func (d *DebugRedisRepository) PushLog(ctx context.Context, traceId string, data interface{}) error {
	msg, err := json.Marshal(tracedLog{
		TraceId: traceId,
		Data:    data,
	})
	if err != nil {
		return err
	}

	err = d.rdb.RPush(ctx, Keys.debugTracedLogs(traceId), msg).Err()
	if err != nil {
		return err
	}

	return nil
}

var Keys = &keys{}

type keys struct{}

var (
	debugTracedLogs string = "debug:%s:traced_logs"
)

func (k *keys) debugTracedLogs(traceId string) string {
	return fmt.Sprintf(debugTracedLogs, traceId)
}
