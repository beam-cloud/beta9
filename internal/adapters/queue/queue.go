package queue

import (
	"context"

	"github.com/beam-cloud/beam/internal/repository"
	"github.com/gin-gonic/gin"
)

type TaskQueue interface {
	Push(identityId string, queueName string, taskId string, ctx *gin.Context) (string, error)
	QueueLength(identityId, queueName string) (int64, error)
	TaskRunning(identityId, queueName string) (bool, error)
	TasksRunning(identityId, queueName string) (int, error)
	GetTaskDuration(identityId, queueName string) (float64, error)
	SetAverageTaskDuration(identityId, queueName string, duration float64) error
	GetAverageTaskDuration(identityId, queueName string) (float64, error)
	MonitorTasks(ctx context.Context, beamRepo repository.BeamRepository)
}
