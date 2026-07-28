package task

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type monitorTaskRepository struct {
	repository.TaskRepository
	deleted bool
}

func (r *monitorTaskRepository) GetTasksInFlight(context.Context) ([]*types.TaskMessage, error) {
	return []*types.TaskMessage{{Executor: "not-registered-yet"}}, nil
}

func (r *monitorTaskRepository) DeleteTaskState(context.Context, string, string, string) error {
	r.deleted = true
	return nil
}

func TestMonitorKeepsTasksUntilExecutorRegisters(t *testing.T) {
	repo := &monitorTaskRepository{}
	if err := (&Dispatcher{
		taskRepo:  repo,
		executors: common.NewSafeMap[func(context.Context, types.TaskMessage) (types.TaskInterface, error)](),
	}).monitorTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repo.deleted {
		t.Fatal("monitor deleted a task before its executor registered")
	}
}
