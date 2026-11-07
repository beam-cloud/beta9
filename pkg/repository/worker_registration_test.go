package repository

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

func TestWorkerRegistrationRepository(t *testing.T) {
	failure := errors.New("registration failed")
	for _, name := range []string{"success", "acquire error", "setup error", "persist error", "canceled setup", "plain AddWorker"} {
		t.Run(name, func(t *testing.T) {
			hook := &workerRegistrationHook{}
			if name == "acquire error" {
				hook.acquireErr = failure
			}
			if name == "persist error" {
				hook.persistErr = failure
			}
			client := redis.NewClient(&redis.Options{Addr: "memory.invalid:6379"})
			client.AddHook(hook)
			defer client.Close()
			repo := NewWorkerRedisRepository(&common.RedisClient{UniversalClient: client}, types.WorkerConfig{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			called := false
			setup := func(leaseCtx context.Context) error {
				called = true
				if !hook.held || leaseCtx == ctx || leaseCtx.Done() == nil {
					t.Error("setup did not run under the worker lease")
				}
				if name == "setup error" {
					return failure
				}
				if name == "canceled setup" {
					cancel()
				}
				return nil
			}
			worker := &types.Worker{Id: "registration", PoolName: "default"}
			var err error
			if name == "plain AddWorker" {
				err = repo.AddWorker(worker)
			} else {
				err = repo.AddWorkerWithSetup(ctx, worker, setup)
			}
			want := error(nil)
			switch name {
			case "acquire error", "setup error", "persist error":
				want = failure
			case "canceled setup":
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("registration error = %v, want %v", err, want)
			}
			if called != (name != "acquire error" && name != "plain AddWorker") {
				t.Errorf("setup called = %t", called)
			}
			if hook.held {
				t.Error("worker lease not released")
			}
			if hook.written != (want == nil) {
				t.Errorf("state written = %t, want %t", hook.written, want == nil)
			}
		})
	}
}

// This hook tests Go-level lease/setup/persist ordering only. It does not
// emulate Redis Lua atomicity, expiration, or lease renewal.
type workerRegistrationHook struct {
	mu                     sync.Mutex
	held, written          bool
	acquireErr, persistErr error
}

func (*workerRegistrationHook) DialHook(redis.DialHook) redis.DialHook {
	return func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("network forbidden in registration unit test")
	}
}
func (h *workerRegistrationHook) ProcessHook(redis.ProcessHook) redis.ProcessHook { return h.process }
func (h *workerRegistrationHook) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if err := h.process(ctx, cmd); err != nil {
				return err
			}
		}
		return nil
	}
}
func (h *workerRegistrationHook) process(ctx context.Context, cmd redis.Cmder) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	err := ctx.Err()
	if err == nil {
		switch cmd.Name() {
		case "evalsha":
			args := cmd.Args()
			if len(args) < 4 || args[3] != common.RedisKeys.SchedulerWorkerLock("registration") {
				err = fmt.Errorf("unexpected lock key: %v", args)
			} else {
				switch len(args) {
				case 7:
					if h.held {
						err = errors.New("nested worker lease")
					} else {
						err = h.acquireErr
					}
					if err == nil {
						h.held = true
					}
				case 5:
					h.held = false
				default:
					err = fmt.Errorf("unexpected lock operation: %v", args)
				}
			}
		case "hset":
			if !h.held {
				err = errors.New("persist outside worker lease")
			} else if h.persistErr != nil {
				err = h.persistErr
			} else {
				h.written = true
			}
		case "multi", "exec", "hgetall", "lrange", "hvals", "smembers", "expire", "sadd":
		default:
			err = fmt.Errorf("unexpected Redis command: %s", cmd.Name())
		}
	}
	switch value := cmd.(type) {
	case *redis.Cmd:
		value.SetVal("OK")
	case *redis.StatusCmd:
		value.SetVal("OK")
	case *redis.IntCmd:
		value.SetVal(1)
	case *redis.BoolCmd:
		value.SetVal(true)
	case *redis.SliceCmd:
		value.SetVal(nil)
	case *redis.StringSliceCmd:
		value.SetVal(nil)
	case *redis.MapStringStringCmd:
		value.SetVal(map[string]string{})
	default:
		if err == nil {
			err = fmt.Errorf("unexpected Redis result type %T", cmd)
		}
	}
	cmd.SetErr(err)
	return err
}
