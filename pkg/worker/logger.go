package worker

import (
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
	"github.com/beam-cloud/beta9/pkg/repository"
	types "github.com/beam-cloud/beta9/pkg/types"
)

const (
	rateLimitMsg = "Rate limit exceeded, logging at reduced rate, some logs will be dropped"
)

type ContainerLogMessage struct {
	// Only SDK envelopes carry beta9_log; arbitrary JSON belongs to the user.
	Internal bool `json:"beta9_log"`
	// SDK stdout and stderr envelopes share the same transport.
	Stream      string                      `json:"stream"`
	Level       string                      `json:"level"`
	Message     string                      `json:"message"`
	TaskID      *string                     `json:"task_id"`
	RunnerEvent *types.ContainerRunnerEvent `json:"beta9_event"`
}

type ContainerLogger struct {
	containerInstances *common.SafeMap[*ContainerInstance]
	eventRepo          repository.EventRepository
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
	pushLogLine := func(taskID, stream, line string) {
		if r.eventRepo != nil {
			r.eventRepo.PushContainerRequestLogLine(r.workerID, request, taskID, stream, line)
		}
	}
	eventsEnabled := r.eventRepo != nil
	if eventsEnabled {
		defer r.eventRepo.PushContainerLogFlushCompleted(r.workerID, request)
	}

	limiter := rate.NewLimiter(rate.Limit(float64(r.logLinesPerHour)/3600.0), r.logLinesPerHour)
	rateLimitMessageLogged := false
	firstByteRecorded := false

	for o := range logChan {
		if !request.IsBuildRequest() && !limiter.Allow() {
			if !rateLimitMessageLogged {
				log.Info().Str("container_id", request.ContainerId).Msg(rateLimitMsg)
				f.WithFields(logrus.Fields{
					"container_id": request.ContainerId,
					"stub_id":      instance.StubId,
				}).Info(rateLimitMsg)
				if !instance.LogBuffer.Write([]byte(rateLimitMsg+"\n")) && eventsEnabled {
					r.eventRepo.PushContainerLogDropped(r.workerID, request, types.EventMessageLogBufferDroppedRateLimit, "")
				}
				for _, line := range strings.Split(rateLimitMsg, "\n") {
					pushLogLine("", "system", line)
				}
				if !firstByteRecorded {
					firstByteRecorded = true
					if eventsEnabled {
						r.eventRepo.PushContainerLogFirstByte(r.workerID, request, "")
					}
				}
				rateLimitMessageLogged = true
			}
			continue
		}

		rateLimitMessageLogged = false

		for remaining := o.Message; remaining != ""; {
			stream, _ := o.Attrs["stream"].(string)
			if stream == "" {
				stream = "system"
			}
			msg := ContainerLogMessage{Message: remaining}
			var envelope ContainerLogMessage
			dec := json.NewDecoder(strings.NewReader(remaining))
			if err := dec.Decode(&envelope); err == nil && ((envelope.Internal && envelope.Message != "") || envelope.RunnerEvent != nil) {
				msg = envelope
				if msg.Stream == "stdout" || msg.Stream == "stderr" {
					stream = msg.Stream
				}
				remaining = remaining[dec.InputOffset():]
				// Discard trailing envelope whitespace while preserving raw user output.
				if strings.TrimSpace(remaining) == "" {
					remaining = ""
				}
			} else {
				// JSON printed by user code is output, not a runner envelope.
				remaining = ""
			}

			if msg.RunnerEvent != nil {
				if r.eventRepo != nil {
					r.eventRepo.PushContainerRunnerEvent(r.workerID, request, msg.RunnerEvent)
				}
				if msg.Message == "" {
					continue
				}
			}

			f.WithFields(logrus.Fields{
				"container_id": request.ContainerId,
				"task_id":      msg.TaskID,
				"stub_id":      instance.StubId,
			}).Info(msg.Message)

			// Write logs to in-memory log buffer as well
			if msg.Message != "" {
				if !instance.LogBuffer.Write([]byte(msg.Message)) && eventsEnabled {
					r.eventRepo.PushContainerLogDropped(r.workerID, request, types.EventMessageLogBufferDroppedMessage, stringPtrValue(msg.TaskID))
				}
				for _, line := range strings.Split(msg.Message, "\n") {
					pushLogLine(stringPtrValue(msg.TaskID), stream, line)
				}
				if !firstByteRecorded {
					firstByteRecorded = true
					if eventsEnabled {
						r.eventRepo.PushContainerLogFirstByte(r.workerID, request, stringPtrValue(msg.TaskID))
					}
				}
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

		if done, ok := o.Attrs["done"].(bool); ok && done {
			break
		}
	}

	if firstByteRecorded && eventsEnabled {
		r.eventRepo.PushContainerLogLastByte(r.workerID, request)
	}
	return nil
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
