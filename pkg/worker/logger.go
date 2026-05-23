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
	Level       string                `json:"level"`
	Message     string                `json:"message"`
	TaskID      *string               `json:"task_id"`
	RunnerEvent *ContainerRunnerEvent `json:"beta9_event"`
}

type ContainerRunnerEvent struct {
	Type        string            `json:"type"`
	ID          string            `json:"id"`
	Timestamp   string            `json:"timestamp"`
	StartTime   string            `json:"start_time"`
	EndTime     string            `json:"end_time"`
	DurationMs  int64             `json:"duration_ms"`
	Success     *bool             `json:"success"`
	ContainerID string            `json:"container_id"`
	StubID      string            `json:"stub_id"`
	StubType    string            `json:"stub_type"`
	TaskID      string            `json:"task_id"`
	Message     string            `json:"message"`
	Attrs       map[string]string `json:"attrs"`
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
	defer r.recordLogEvent(request, instance, types.ContainerEventLogsFlushCompleted, nil, "container log capture flushed")

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
					r.recordLogEvent(request, instance, types.ContainerEventLogsDropped, nil, "container log buffer dropped a rate limit message")
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

			if msg.RunnerEvent != nil {
				r.recordRunnerEvent(request, msg.RunnerEvent)
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
				if !instance.LogBuffer.Write([]byte(msg.Message)) {
					r.recordLogEvent(request, instance, types.ContainerEventLogsDropped, map[string]string{"task_id": stringPtrValue(msg.TaskID)}, "container log buffer dropped a message")
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
				r.recordLogEvent(request, instance, types.ContainerEventLogsDropped, nil, "container log buffer dropped a raw message")
			}
			r.recordLogLine(request, "", o.Message)
			r.recordFirstLogByte(request, instance, &firstByteRecorded)
		}

		if done, ok := o.Attrs["done"].(bool); ok && done {
			break
		}
	}

	if firstByteRecorded {
		r.recordLogEvent(request, instance, types.ContainerEventLogsLastByte, nil, "container log capture received final byte")
	}
	return nil
}

func (r *ContainerLogger) recordFirstLogByte(request *types.ContainerRequest, instance *ContainerInstance, recorded *bool) {
	if *recorded {
		return
	}
	*recorded = true
	r.recordLogEvent(request, instance, types.ContainerEventLogsFirstByte, nil, "container log capture received first byte")
}

func (r *ContainerLogger) recordLogLine(request *types.ContainerRequest, taskId string, message string) {
	for _, line := range strings.Split(message, "\n") {
		if line == "" {
			continue
		}
		recordContainerLogLine(r.eventRepo, types.EventContainerLogSchema{
			Timestamp:   time.Now().UTC(),
			ContainerID: request.ContainerId,
			StubID:      request.StubId,
			StubType:    string(request.Stub.Type.Kind()),
			TaskID:      taskId,
			WorkspaceID: request.WorkspaceId,
			WorkerID:    r.workerID,
			Stream:      "stdout",
			Line:        line,
		})
	}
}

func (r *ContainerLogger) recordLogEvent(request *types.ContainerRequest, instance *ContainerInstance, eventID types.ContainerEventID, attrs map[string]string, message string) {
	if r.eventRepo == nil {
		return
	}
	if attrs == nil {
		attrs = map[string]string{}
	}
	if attrs["task_id"] == "" {
		attrs["task_id"] = taskIDFromEnv(request.Env)
	}
	r.eventRepo.PushContainerEvent(types.EventContainerEventSchema{
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
	})
}

func (r *ContainerLogger) recordRunnerEvent(request *types.ContainerRequest, event *ContainerRunnerEvent) {
	if r.eventRepo == nil || event == nil || event.ID == "" {
		return
	}
	if event.Attrs == nil {
		event.Attrs = map[string]string{}
	}

	switch event.Type {
	case "phase":
		startTime, ok := parseRunnerEventTime(event.StartTime)
		if !ok {
			return
		}
		endTime, ok := parseRunnerEventTime(event.EndTime)
		if !ok || endTime.Before(startTime) {
			return
		}
		durationMs := event.DurationMs
		if durationMs == 0 {
			durationMs = endTime.Sub(startTime).Milliseconds()
		}
		success := true
		if event.Success != nil {
			success = *event.Success
		}
		def := types.ContainerPhaseDefinitionFor(types.ContainerPhaseID(event.ID))
		r.eventRepo.PushContainerPhaseEvent(types.EventContainerPhaseSchema{
			ID:          types.ContainerPhaseID(event.ID),
			Domain:      def.Domain,
			ParentID:    def.ParentID,
			StartTime:   startTime,
			EndTime:     endTime,
			DurationMs:  durationMs,
			ContainerID: firstNonEmpty(event.ContainerID, request.ContainerId),
			StubID:      firstNonEmpty(event.StubID, request.StubId),
			StubType:    firstNonEmpty(event.StubType, string(request.Stub.Type.Kind())),
			TaskID:      firstNonEmpty(event.TaskID, taskIDFromEnv(request.Env)),
			WorkspaceID: request.WorkspaceId,
			WorkerID:    r.workerID,
			Success:     &success,
			Source:      "runner.stdout",
			Attrs:       event.Attrs,
		})
	case "event":
		timestamp, ok := parseRunnerEventTime(event.Timestamp)
		if !ok {
			return
		}
		domain := types.ContainerEventDomain(types.ContainerEventID(event.ID))
		if domain == "" {
			domain = types.EventDomainRunner
		}
		r.eventRepo.PushContainerEvent(types.EventContainerEventSchema{
			ID:          types.ContainerEventID(event.ID),
			Domain:      domain,
			Timestamp:   timestamp,
			ContainerID: firstNonEmpty(event.ContainerID, request.ContainerId),
			StubID:      firstNonEmpty(event.StubID, request.StubId),
			StubType:    firstNonEmpty(event.StubType, string(request.Stub.Type.Kind())),
			TaskID:      firstNonEmpty(event.TaskID, taskIDFromEnv(request.Env)),
			WorkspaceID: request.WorkspaceId,
			WorkerID:    r.workerID,
			Source:      "runner.stdout",
			Message:     event.Message,
			Attrs:       event.Attrs,
		})
	}
}

func parseRunnerEventTime(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
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
