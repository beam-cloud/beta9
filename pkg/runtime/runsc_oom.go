package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/rs/zerolog/log"
)

// runscStats uses the runtime's guest OOM counter to identify kills. Host RSS
// and allocation failures alone are not evidence of an application OOM kill.
type runscStats struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Data struct {
		Memory struct {
			Raw   map[string]uint64 `json:"raw"`
			Usage struct {
				Usage uint64 `json:"usage"`
			} `json:"usage"`
		} `json:"memory"`
	} `json:"data"`
}

func (r *Runsc) guestOOMEvents(ctx context.Context, containerID string) (<-chan Event, error) {
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
			var stats runscStats
			if err := decoder.Decode(&stats); err != nil {
				if err != io.EOF && ctx.Err() == nil {
					select {
					case events <- Event{Type: "error", Err: fmt.Errorf("decode gVisor OOM events: %w", err)}:
					case <-ctx.Done():
					}
				}
				return
			}
			count := stats.Data.Memory.Raw["guest_oom_kill"]
			if stats.Type != "stats" || stats.ID != containerID || count <= previous {
				continue
			}
			previous = count
			select {
			case events <- Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{
				Kills: count, MemoryLimit: int64(stats.Data.Memory.Raw["guest_memory_limit"]),
				MemoryUsage: stats.Data.Memory.Usage.Usage,
			}}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return events, nil
}

// RuntimeOOMWatcher combines guest process OOMs and host runtime OOMs. Without
// an application callback, guest OOMs use the terminal path too, preventing
// managed workloads from respawning OOM victims forever. Interactive sandboxes
// supply a callback to report guest OOMs while keeping the runtime alive.
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
	var once sync.Once
	notify := func() {
		if w.ctx.Err() == nil && onOOM != nil {
			once.Do(onOOM)
		}
	}
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
				if !ok {
					return
				}
				if w.ctx.Err() != nil {
					return
				}
				if event.ApplicationOOM != nil {
					if w.onApplicationOOM != nil {
						w.onApplicationOOM(event.ApplicationOOM)
						continue
					}
					notify()
					return
				}
				if event.Type == "oom" {
					notify()
					return
				}
				if event.Err != nil {
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
