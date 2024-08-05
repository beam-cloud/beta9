package worker

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	types "github.com/beam-cloud/beta9/pkg/types"
)

type Logger struct {
	logFile     *os.File
	logger      *logrus.Logger
	fields      logrus.Fields
	containerId string
}

func NewLogger(request *types.ContainerRequest) (*Logger, error) {
	logFile, err := openLogFile(request.ContainerId)
	if err != nil {
		return nil, err
	}

	f := logrus.New()
	f.SetOutput(logFile)
	f.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339Nano})
	logFields := logrus.Fields{
		"container_id": request.ContainerId,
		"stub_id":      request.StubId,
	}

	return &Logger{
		logFile:     logFile,
		logger:      f,
		fields:      logFields,
		containerId: request.ContainerId,
	}, nil
}

func (l *Logger) Log(format string, args ...any) {
	log.Print(fmt.Sprintf("<%s> - ", l.containerId) + fmt.Sprintf(format, args...))
	l.logger.WithFields(l.fields).Infof(format, args...)
}
