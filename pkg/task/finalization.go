package task

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CompletionError distinguishes permanent rejection from retryable persistence errors.
func CompletionError(err error) error {
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, redis.Nil) {
		return status.Error(codes.NotFound, "task no longer exists")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "task completion attempt timed out")
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	return status.Error(codes.Unavailable, "task persistence is unavailable")
}

// CompletionContext lets an accepted completion outlive its client, within policy.
func CompletionContext(ctx context.Context, policy types.TaskCompletionConfig) (context.Context, context.CancelFunc, error) {
	if policy.Timeout <= 0 || policy.ResultUploadTimeout <= 0 || policy.ResultUploadTimeout >= policy.Timeout {
		return nil, nil, status.Error(codes.Internal, "invalid task completion configuration")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), policy.Timeout)
	return ctx, cancel, nil
}

var beginCompletionScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
local ttl = redis.call("PTTL", KEYS[2])
if ttl <= 0 then return -1 end
local deadline = redis.call("GET", KEYS[3])
if not deadline then
  deadline = ARGV[2]
  redis.call("PSETEX", KEYS[3], ttl, deadline)
end
redis.call("SADD", KEYS[4], ARGV[3])
return tonumber(deadline)
`)

// BeginCompletion fixes the execution's deadline across replays and gateway restarts.
func BeginCompletion(ctx context.Context, rdb *common.RedisClient, meta types.TaskMetadata, budget time.Duration) (context.Context, context.CancelFunc, error) {
	finishBy := time.Now().Add(budget)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(finishBy) {
		finishBy = parentDeadline
	}
	deadline, err := beginCompletionScript.Run(ctx, rdb, []string{
		common.RedisKeys.TaskClaim(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskEntry(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskCompletionDeadline(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskCompletionIndex(meta.WorkspaceName, meta.StubId),
	}, meta.ContainerId, finishBy.UnixMilli(), meta.TaskId).Int64()
	if err != nil {
		return nil, nil, CompletionError(err)
	}
	if deadline == 0 {
		return nil, nil, status.Error(codes.FailedPrecondition, "container does not own task")
	}
	if deadline < 0 {
		return nil, nil, status.Error(codes.NotFound, "task recovery state no longer exists")
	}
	if deadline <= time.Now().UnixMilli() {
		return nil, nil, status.Error(codes.FailedPrecondition, "task completion window expired")
	}
	ctx, cancel := context.WithDeadline(ctx, time.UnixMilli(deadline))
	return ctx, cancel, nil
}

var forgetDeletedTaskScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
redis.call("SREM", KEYS[4], KEYS[2])
redis.call("SREM", KEYS[5], ARGV[2])
redis.call("SREM", KEYS[6], ARGV[2])
redis.call("SREM", KEYS[7], ARGV[2])
redis.call("DEL", KEYS[1], KEYS[2], KEYS[3])
return 1
`)

// ForgetDeletedTask clears orphaned recovery state only for its current owner.
func ForgetDeletedTask(ctx context.Context, rdb *common.RedisClient, meta types.TaskMetadata) error {
	return forgetDeletedTaskScript.Run(ctx, rdb, []string{
		common.RedisKeys.TaskClaim(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskEntry(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskCompletionDeadline(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskIndex(), common.RedisKeys.TaskClaimIndex(meta.WorkspaceName, meta.StubId),
		common.RedisKeys.TaskIndexByStub(meta.WorkspaceName, meta.StubId),
		common.RedisKeys.TaskCompletionIndex(meta.WorkspaceName, meta.StubId),
	}, meta.ContainerId, meta.TaskId).Err()
}

func CompletionExpired(ctx context.Context, rdb *common.RedisClient, meta types.TaskMetadata) (bool, error) {
	deadline, err := rdb.Get(ctx, common.RedisKeys.TaskCompletionDeadline(meta.WorkspaceName, meta.StubId, meta.TaskId)).Int64()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return deadline <= time.Now().UnixMilli(), nil
}

var renewTaskLivenessScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call("EXISTS", KEYS[2]) == 0 then return -1 end
local heartbeat_ttl, running_ttl = tonumber(ARGV[3]), tonumber(ARGV[4])
local deadline = redis.call("GET", KEYS[3])
if deadline then
  local remaining = tonumber(deadline) - tonumber(ARGV[2])
  if remaining <= 0 then return -2 end
  heartbeat_ttl = math.min(heartbeat_ttl, remaining)
  running_ttl = math.min(running_ttl, remaining)
end
redis.call("PSETEX", KEYS[4], heartbeat_ttl, 1)
if KEYS[5] ~= "" then
  redis.call("PSETEX", KEYS[5], running_ttl, 1)
  redis.call("SADD", KEYS[6], ARGV[5])
end
return 1
`)

// RenewTaskLiveness atomically verifies ownership and caps leases at the completion deadline.
func RenewTaskLiveness(ctx context.Context, rdb *common.RedisClient, meta types.TaskMetadata, heartbeat, running, index string) error {
	result, err := renewTaskLivenessScript.Run(ctx, rdb, []string{
		common.RedisKeys.TaskClaim(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskEntry(meta.WorkspaceName, meta.StubId, meta.TaskId),
		common.RedisKeys.TaskCompletionDeadline(meta.WorkspaceName, meta.StubId, meta.TaskId),
		heartbeat, running, index,
	}, meta.ContainerId, time.Now().UnixMilli(), types.TaskHeartbeatTTL.Milliseconds(), types.TaskQueueRunningLeaseTTL.Milliseconds(), meta.TaskId).Int()
	if err != nil {
		return CompletionError(err)
	}
	if result == -1 {
		return status.Error(codes.NotFound, "task recovery state no longer exists")
	}
	if result != 1 {
		return status.Error(codes.FailedPrecondition, "task ownership or completion window is no longer valid")
	}
	return nil
}

// ContainerIsRunning distinguishes a stopped container from a repository outage.
// A lost gateway heartbeat alone must not cause another execution of user code.
func ContainerIsRunning(containers repository.ContainerRepository, containerID string) (bool, error) {
	container, err := containers.GetContainerState(containerID)
	if (&types.ErrContainerStateNotFound{}).From(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return container != nil && container.Status == types.ContainerStatusRunning, nil
}

// KeepFinalizationAlive renews leases until stopped. Its stop function joins the renewer.
func KeepFinalizationAlive(ctx context.Context, taskID string, refresh func(context.Context) error) (context.Context, func(), error) {
	if err := refresh(ctx); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(types.TaskHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := refresh(ctx); err != nil && ctx.Err() == nil {
					log.Error().Err(err).Str("task_id", taskID).Msg("failed to renew task finalization heartbeat")
					cancel()
					return
				}
			}
		}
	}()
	return ctx, func() { cancel(); <-done }, nil
}
