package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"github.com/beam-cloud/beta9/pkg/common"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/trace"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	rateLimitMsg = "Rate limit exceeded, logging at reduced rate, some logs will be dropped"
)

type ContainerLogMessage struct {
	Level   string  `json:"level"`
	Message string  `json:"message"`
	TaskID  *string `json:"task_id"`
}

type ContainerLogger struct {
	containerInstances *common.SafeMap[*ContainerInstance]
	traceRepoClient    pb.TraceRepositoryServiceClient
	workerID           string
	logLinesPerHour    int
}

func (r *ContainerLogger) Read(containerId string, buffer []byte) (int64, error) {
	return 0, nil
}

func (r *ContainerLogger) Log(containerId, stubId string, format string, args ...any) error {
	logFile, err := openLogFile(containerId)
	if err != nil {
		return err
	}
	defer logFile.Close()

	// Create a new file logger
	f := logrus.New()
	f.SetOutput(logFile)
	f.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	log.Info().Str("container_id", containerId).Msg(fmt.Sprintf(format, args...))
	f.WithFields(logrus.Fields{
		"container_id": containerId,
		"stub_id":      stubId,
	},
	).Infof(format, args...)

	return nil
}

func (r *ContainerLogger) CaptureLogs(request *types.ContainerRequest, logChan chan common.LogRecord) error {
	logFile, err := openLogFile(request.ContainerId)
	if err != nil {
		return err
	}
	defer logFile.Close()

	// Create a new file logger
	f := logrus.New()
	f.SetOutput(logFile)
	f.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})

	instance, exists := r.containerInstances.Get(request.ContainerId)
	if !exists {
		return errors.New("container not found")
	}
	defer instance.LogBuffer.Close()
	defer r.recordLogTraceEvent(request, instance, trace.EventLogsFlushCompleted, nil, "container log capture flushed")

	limiter := rate.NewLimiter(rate.Limit(float64(r.logLinesPerHour)/3600.0), r.logLinesPerHour)
	rateLimitMessageLogged := false
	firstByteRecorded := false

	var msg ContainerLogMessage
	for o := range logChan {
		if !request.IsBuildRequest() && !limiter.Allow() {
			if !rateLimitMessageLogged {
				log.Info().Str("container_id", request.ContainerId).Msg(rateLimitMsg)
				f.WithFields(logrus.Fields{
					"container_id": request.ContainerId,
					"stub_id":      instance.StubId,
				}).Info(rateLimitMsg)
				if !instance.LogBuffer.Write([]byte(rateLimitMsg + "\n")) {
					r.recordLogTraceEvent(request, instance, trace.EventLogsDropped, nil, "container log buffer dropped a rate limit message")
				}
				r.recordLogLine(request, "", rateLimitMsg)
				r.recordFirstLogByte(request, instance, &firstByteRecorded)
				rateLimitMessageLogged = true
			}
			continue
		}

		rateLimitMessageLogged = false

		dec := json.NewDecoder(strings.NewReader(o.Message))
		msgDecoded := false

		for {
			// Clear the message struct to avoid carrying over previous values
			msg.Level = ""
			msg.Message = ""
			msg.TaskID = nil

			err := dec.Decode(&msg)
			if err != nil {
				/*
					Either the json parsing ends with an EOF error indicating that the
					JSON string is complete, or with a json decode error indicating that
					the JSON string is invalid. In either case, we can break out of the
					decode loop and continue processing the next message.
				*/
				break
			}

			msgDecoded = true

			f.WithFields(logrus.Fields{
				"container_id": request.ContainerId,
				"task_id":      msg.TaskID,
				"stub_id":      instance.StubId,
			}).Info(msg.Message)

			// Write logs to in-memory log buffer as well
			if msg.Message != "" {
				if !instance.LogBuffer.Write([]byte(msg.Message)) {
					r.recordLogTraceEvent(request, instance, trace.EventLogsDropped, map[string]string{"task_id": stringPtrValue(msg.TaskID)}, "container log buffer dropped a message")
				}
				r.recordLogLine(request, stringPtrValue(msg.TaskID), msg.Message)
				r.recordFirstLogByte(request, instance, &firstByteRecorded)
			}

			if msg.Message != "" {
				lines := strings.Split(msg.Message, "\n")
				for _, line := range lines {
					if line == "" {
						continue
					}

					if msg.TaskID != nil {
						log.Info().Str("container_id", request.ContainerId).Str("task_id", *msg.TaskID).Msg(line)
					} else {
						log.Info().Str("container_id", request.ContainerId).Msg(line)
					}
				}
			}
		}

		// Fallback in case the message was not JSON
		if !msgDecoded && o.Message != "" {
			f.WithFields(logrus.Fields{
				"container_id": request.ContainerId,
				"stub_id":      instance.StubId,
			}).Info(o.Message)

			lines := strings.Split(o.Message, "\n")
			for _, line := range lines {
				if line == "" {
					continue
				}

				log.Info().Str("container_id", request.ContainerId).Msg(line)
			}

			// Write logs to in-memory log buffer as well
			if !instance.LogBuffer.Write([]byte(o.Message)) {
				r.recordLogTraceEvent(request, instance, trace.EventLogsDropped, nil, "container log buffer dropped a raw message")
			}
			r.recordLogLine(request, "", o.Message)
			r.recordFirstLogByte(request, instance, &firstByteRecorded)
		}

		if done, ok := o.Attrs["done"].(bool); ok && done {
			break
		}
	}

	if firstByteRecorded {
		r.recordLogTraceEvent(request, instance, trace.EventLogsLastByte, nil, "container log capture received final byte")
	}
	return nil
}

func (r *ContainerLogger) recordFirstLogByte(request *types.ContainerRequest, instance *ContainerInstance, recorded *bool) {
	if *recorded {
		return
	}
	*recorded = true
	r.recordLogTraceEvent(request, instance, trace.EventLogsFirstByte, nil, "container log capture received first byte")
}

func (r *ContainerLogger) recordLogLine(request *types.ContainerRequest, taskId string, message string) {
	for _, line := range strings.Split(message, "\n") {
		if line == "" {
			continue
		}
		if err := recordTraceLog(context.Background(), r.traceRepoClient, trace.LogEntry{
			ContainerID: request.ContainerId,
			TaskID:      taskId,
			Stream:      "stdout",
			Line:        line,
		}); err != nil {
			log.Debug().Err(err).Str("container_id", request.ContainerId).Msg("failed to record startup trace log")
		}
	}
}

func (r *ContainerLogger) recordLogTraceEvent(request *types.ContainerRequest, instance *ContainerInstance, eventID trace.EventID, attrs map[string]string, message string) {
	if attrs == nil {
		attrs = map[string]string{}
	}
	if attrs["task_id"] == "" {
		attrs["task_id"] = taskIDFromEnv(request.Env)
	}
	event := trace.Event{
		ID:          eventID,
		ContainerID: request.ContainerId,
		StubID:      request.StubId,
		StubType:    string(request.Stub.Type.Kind()),
		TaskID:      attrs["task_id"],
		WorkspaceID: request.WorkspaceId,
		WorkerID:    r.workerID,
		Source:      "worker.logger",
		Message:     message,
		Attrs:       attrs,
	}
	if err := recordTraceEvent(context.Background(), r.traceRepoClient, event); err != nil {
		log.Debug().Err(err).Str("container_id", request.ContainerId).Str("event_id", string(eventID)).Msg("failed to record startup trace log event")
	}
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func openLogFile(containerId string) (*os.File, error) {
	logFilePath := path.Join(containerLogsPath, fmt.Sprintf("%s.log", containerId))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to create container log file: %w", err)
	}
	return logFile, nil
}
