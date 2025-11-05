package runtime

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	cgroupV2Root         = "/sys/fs/cgroup"
	memoryEventsFile     = "memory.events"
	oomWatcherPollInterval = 500 * time.Millisecond
)

// OOMWatcher watches for OOM kills via cgroup v2 memory.events
type OOMWatcher struct {
	cgroupPath string
	lastOOMCount int64
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewOOMWatcher creates a new OOM watcher for the given cgroup path
// cgroupPath should be relative to /sys/fs/cgroup (e.g., "beta9/container-123")
func NewOOMWatcher(ctx context.Context, cgroupPath string) *OOMWatcher {
	watcherCtx, cancel := context.WithCancel(ctx)
	return &OOMWatcher{
		cgroupPath: cgroupPath,
		ctx:        watcherCtx,
		cancel:     cancel,
	}
}

// Watch starts watching for OOM events and calls onOOM when detected
func (w *OOMWatcher) Watch(onOOM func()) error {
	// Read initial OOM count
	initialCount, err := w.readOOMKillCount()
	if err != nil {
		// If we can't read the file, container may not be started yet
		log.Debug().Str("cgroup", w.cgroupPath).Err(err).Msg("failed to read initial OOM count")
		initialCount = 0
	}
	w.lastOOMCount = initialCount

	go func() {
		ticker := time.NewTicker(oomWatcherPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-w.ctx.Done():
				return
			case <-ticker.C:
				currentCount, err := w.readOOMKillCount()
				if err != nil {
					// Container may have been deleted
					continue
				}

				if currentCount > w.lastOOMCount {
					log.Info().Str("cgroup", w.cgroupPath).
						Int64("previous", w.lastOOMCount).
						Int64("current", currentCount).
						Msg("OOM kill detected")
					
					w.lastOOMCount = currentCount
					
					// Call the callback
					if onOOM != nil {
						onOOM()
					}
				}
			}
		}
	}()

	return nil
}

// Stop stops the OOM watcher
func (w *OOMWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

// readOOMKillCount reads the oom_kill counter from memory.events
func (w *OOMWatcher) readOOMKillCount() (int64, error) {
	memEventsPath := filepath.Join(cgroupV2Root, w.cgroupPath, memoryEventsFile)
	
	file, err := os.Open(memEventsPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open memory.events: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Look for "oom_kill N" line
		if strings.HasPrefix(line, "oom_kill ") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				count, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return 0, fmt.Errorf("failed to parse oom_kill count: %w", err)
				}
				return count, nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading memory.events: %w", err)
	}

	// oom_kill line not found, return 0
	return 0, nil
}

// GetCgroupPath returns the cgroup path for a container
// This assumes containers are placed in a deterministic cgroup hierarchy
func GetCgroupPath(containerID string) string {
	// By default, runc and runsc place containers under /sys/fs/cgroup/<containerID>
	// Adjust this based on your actual cgroup hierarchy
	return containerID
}

// GetCgroupPathWithPrefix returns the cgroup path for a container with a prefix
func GetCgroupPathWithPrefix(prefix, containerID string) string {
	return filepath.Join(prefix, containerID)
}
