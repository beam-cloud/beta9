package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestTaskStateAndClaimExpire(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	repo := NewTaskRedisRepository(rdb)
	task := &types.TaskMessage{
		TaskId:        "task",
		WorkspaceName: "workspace",
		StubId:        "stub",
		Policy:        types.TaskPolicy{Expires: time.Now().Add(time.Minute)},
	}
	msg, err := task.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.SetTaskState(context.Background(), task.WorkspaceName, task.StubId, task.TaskId, msg); err != nil {
		t.Fatal(err)
	}
	if err := repo.ClaimTask(context.Background(), task.WorkspaceName, task.StubId, task.TaskId, "container"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		common.RedisKeys.TaskEntry(task.WorkspaceName, task.StubId, task.TaskId),
		common.RedisKeys.TaskClaim(task.WorkspaceName, task.StubId, task.TaskId),
	} {
		if ttl := rdb.TTL(context.Background(), key).Val(); ttl <= 0 {
			t.Fatalf("%s has no TTL", key)
		}
	}
}
