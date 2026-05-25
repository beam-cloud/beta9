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
	pushLogLine := func(taskID string, line string) {
		if r.eventRepo != nil {
			r.eventRepo.PushContainerRequestLogLine(r.workerID, request, taskID, types.EventLogStreamStdout, line)
		}
	}
	eventsEnabled := r.eventRepo != nil
	if eventsEnabled {
		defer r.eventRepo.PushContainerLogFlushCompleted(r.workerID, request)
	}

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
				if !instance.LogBuffer.Write([]byte(rateLimitMsg+"\n")) && eventsEnabled {
					r.eventRepo.PushContainerLogDropped(r.workerID, request, types.EventMessageLogBufferDroppedRateLimit, "")
				}
				for _, line := range strings.Split(rateLimitMsg, "\n") {
					pushLogLine("", line)
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

		dec := json.NewDecoder(strings.NewReader(o.Message))
		msgDecoded := false

		for {
			// Clear the message struct to avoid carrying over previous values
			msg = ContainerLogMessage{}

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
					pushLogLine(stringPtrValue(msg.TaskID), line)
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
			if !instance.LogBuffer.Write([]byte(o.Message)) && eventsEnabled {
				r.eventRepo.PushContainerLogDropped(r.workerID, request, types.EventMessageLogBufferDroppedRawMessage, "")
			}
			for _, line := range strings.Split(o.Message, "\n") {
				pushLogLine("", line)
			}
			if !firstByteRecorded {
				firstByteRecorded = true
				if eventsEnabled {
					r.eventRepo.PushContainerLogFirstByte(r.workerID, request, "")
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
