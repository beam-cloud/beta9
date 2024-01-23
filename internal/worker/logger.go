package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/sirupsen/logrus"
)

type ContainerLogMessage struct {
	Level   string  `json:"level"`
	Message string  `json:"message"`
	TaskID  *string `json:"task_id"`
}

type ContainerLogger struct {
	containerInstances *common.SafeMap[*ContainerInstance]
}

func (r *ContainerLogger) Read(containerId string, buffer []byte) (int64, error) {
	return 0, nil
}

func (r *ContainerLogger) CaptureLogs(containerId string, outputChan chan common.OutputMsg) error {
	logFilePath := path.Join(containerLogsPath, fmt.Sprintf("%s.log", containerId))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to create container log file: %w", err)
	}
	defer logFile.Close()

	// Create a new file logger
	f := logrus.New()
	f.SetOutput(logFile)
	f.SetFormatter(&logrus.JSONFormatter{})

	instance, exists := r.containerInstances.Get(containerId)
	if !exists {
		return errors.New("container not found")
	}

	for o := range outputChan {
		dec := json.NewDecoder(strings.NewReader(o.Msg))
		msgDecoded := false

		for {
			var msg ContainerLogMessage

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
				"container_id": containerId,
				"task_id":      msg.TaskID,
			}).Info(msg.Message)

			if msg.TaskID != nil {
				log.Printf("<%s>:<%s> - %s\n", containerId, *msg.TaskID, msg.Message)
			} else if msg.Message != "" {
				log.Printf("<%s> - %s\n", containerId, msg.Message)
			}
		}

		if o.Msg != "" {
			// Write logs to in-memory log buffer as well
			instance.LogBuffer.Write([]byte(o.Msg))
		}

		// Fallback in case the message was not JSON
		if !msgDecoded && o.Msg != "" {
			f.WithFields(logrus.Fields{
				"container_id": containerId,
			}).Info(o.Msg)

			log.Printf("<%s> - %s\n", containerId, o.Msg)
		}

		if o.Done {
			break
		}
	}

	return nil
}
