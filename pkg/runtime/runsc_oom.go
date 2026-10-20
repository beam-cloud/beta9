package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	runc "github.com/beam-cloud/go-runc"
	"github.com/rs/zerolog/log"
)

// Events reports guest OOM counter increments, independently of host cgroup OOMs.
func (r *Runsc) Events(ctx context.Context, containerID string) (<-chan Event, error) {
	args := append(r.baseArgs(false), "events", "--interval=1", containerID)
	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, fmt.Errorf("start gVisor OOM events: %w", err)
	}
	events := make(chan Event, 1)
	go func() {
		defer close(events)
		defer func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}()
		decoder := json.NewDecoder(stdout)
		var previous uint64
		for {
			var stats runc.Event
			if err := decoder.Decode(&stats); err != nil {
				if err != io.EOF && ctx.Err() == nil {
					select {
					case events <- Event{Type: "error", Err: fmt.Errorf("decode gVisor OOM events: %w", err)}:
					case <-ctx.Done():
					}
				}
				return
			}
			if stats.Type != "stats" || stats.ID != containerID || stats.Stats == nil {
				continue
			}
			memory := stats.Stats.Memory
			count := memory.Raw["guest_oom_kill"]
			if count <= previous {
				continue
			}
			previous = count
			select {
			case events <- Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{
				Kills: count, MemoryLimit: int64(memory.Raw["guest_memory_limit"]),
				MemoryUsage: memory.Usage.Usage,
			}}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return events, nil
}

// RuntimeOOMWatcher combines native and host OOMs. A sandbox callback reports
// guest kills without terminating the container; host OOMs are always terminal.
type RuntimeOOMWatcher struct {
	ctx              context.Context
	cancel           context.CancelFunc
	runtime          Runtime
	containerID      string
	fallback         OOMWatcher
	onApplicationOOM func(*microvm.ApplicationOOM)
}

func NewRuntimeOOMWatcher(ctx context.Context, rt Runtime, containerID string, fallback OOMWatcher, onApplicationOOM func(*microvm.ApplicationOOM)) *RuntimeOOMWatcher {
	ctx, cancel := context.WithCancel(ctx)
	return &RuntimeOOMWatcher{ctx: ctx, cancel: cancel, runtime: rt, containerID: containerID, fallback: fallback, onApplicationOOM: onApplicationOOM}
}

func (w *RuntimeOOMWatcher) Watch(onOOM func()) error {
	notify := sync.OnceFunc(func() {
		if w.ctx.Err() == nil && onOOM != nil {
			onOOM()
		}
	})
	if err := w.fallback.Watch(notify); err != nil {
		return err
	}
	events, err := w.runtime.Events(w.ctx, w.containerID)
	if err != nil {
		log.Warn().Err(err).Str("container_id", w.containerID).Msg("runtime OOM watcher failed to start; host cgroup watcher remains active")
		return nil
	}
	go func() {
		for {
			select {
			case <-w.ctx.Done():
				return
			case event, ok := <-events:
				if !ok || w.ctx.Err() != nil {
					return
				}
				switch {
				case event.ApplicationOOM != nil && w.onApplicationOOM != nil:
					w.onApplicationOOM(event.ApplicationOOM)
				case event.ApplicationOOM != nil || event.Type == "oom":
					notify()
					return
				case event.Err != nil:
					log.Warn().Err(event.Err).Str("container_id", w.containerID).Msg("runtime OOM event stream failed; host cgroup watcher remains active")
				}
			}
		}
	}()
	return nil
}

func (w *RuntimeOOMWatcher) Stop() {
	w.cancel()
	w.fallback.Stop()
}
