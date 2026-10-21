package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/beam-cloud/beta9/pkg/types"
	runc "github.com/beam-cloud/go-runc"
	"github.com/rs/zerolog/log"
)

const runscEventSourceTimeout = 30 * time.Second

// Events reports guest OOM counter increments, independently of host cgroup OOMs.
func (r *Runsc) Events(ctx context.Context, containerID string) (<-chan Event, error) {
	args := append(r.baseArgs(false), "events", "--interval=1", containerID)
	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	events := make(chan Event, 1)
	go func() {
		defer close(events)
		defer stdout.Close()
		report := func(err error) {
			if ctx.Err() == nil {
				select {
				case events <- Event{Type: "error", Err: err}:
				case <-ctx.Done():
				}
			}
		}
		// Run publishes its CLI PID before the sandbox state/control socket exists.
		// Starting `runsc events` at that point exits immediately with no stats.
		if err := r.waitForEventSource(ctx, containerID); err != nil {
			report(err)
			return
		}
		if err := cmd.Start(); err != nil {
			report(fmt.Errorf("start gVisor OOM events: %w", err))
			return
		}
		defer func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}()
		decoder := json.NewDecoder(stdout)
		var previous uint64
		for {
			var stats runc.Event
			if err := decoder.Decode(&stats); err != nil {
				if err != io.EOF {
					report(fmt.Errorf("decode gVisor OOM events: %w", err))
				} else if err := cmd.Wait(); err != nil {
					report(fmt.Errorf("gVisor OOM event stream exited: %w: %s", err, stderr.String()))
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

func (r *Runsc) waitForEventSource(ctx context.Context, containerID string) error {
	ctx, cancel := context.WithTimeout(ctx, runscEventSourceTimeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := r.loadState(ctx, containerID)
		if err == nil && state.Status == types.RuncContainerStatusRunning {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for gVisor OOM event source: %w", ctx.Err())
		case <-ticker.C:
		}
	}
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
