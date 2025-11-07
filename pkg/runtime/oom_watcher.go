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
	cgroupV2Root           = "/sys/fs/cgroup"
	memoryEventsFile       = "memory.events"
	oomWatcherPollInterval = 500 * time.Millisecond
)

// OOMWatcher watches for OOM kills via cgroup v2 memory.events
type OOMWatcher struct {
	cgroupPath   string
	lastOOMCount int64
	ctx          context.Context
	cancel       context.CancelFunc
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
		// If we can't read the file, log more details for debugging
		log.Warn().Str("cgroup", w.cgroupPath).Err(err).Msg("failed to read initial OOM count - OOM detection may not work")
		
		// Check if the file exists
		memEventsPath := filepath.Join(cgroupV2Root, w.cgroupPath, memoryEventsFile)
		if _, statErr := os.Stat(memEventsPath); os.IsNotExist(statErr) {
			log.Warn().
				Str("cgroup", w.cgroupPath).
				Str("path", memEventsPath).
				Msg("memory.events file does not exist - this is expected for gVisor, OOM detection will not work")
		}
		
		initialCount = 0
	} else {
		log.Debug().Str("cgroup", w.cgroupPath).Int64("initial_count", initialCount).Msg("OOM watcher initialized successfully")
	}
	w.lastOOMCount = initialCount

	go func() {
		ticker := time.NewTicker(oomWatcherPollInterval)
		defer ticker.Stop()

		failedReads := 0
		maxFailedReads := 10 // Stop logging after 10 failed reads

		for {
			select {
			case <-w.ctx.Done():
				return
			case <-ticker.C:
				currentCount, err := w.readOOMKillCount()
				if err != nil {
					failedReads++
					// Only log first few failures to avoid spam
					if failedReads <= maxFailedReads {
						log.Debug().Str("cgroup", w.cgroupPath).Err(err).Msg("failed to read OOM count")
					} else if failedReads == maxFailedReads+1 {
						log.Warn().Str("cgroup", w.cgroupPath).Msg("OOM watcher continuing to fail, suppressing further logs")
					}
					// Container may have been deleted
					continue
				}

				// Reset fail counter on success
				if failedReads > maxFailedReads {
					log.Info().Str("cgroup", w.cgroupPath).Msg("OOM watcher recovered")
				}
				failedReads = 0

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
	// Try cgroup v2 path first
	memEventsPath := filepath.Join(cgroupV2Root, w.cgroupPath, memoryEventsFile)

	file, err := os.Open(memEventsPath)
	if err != nil {
		// If cgroup v2 doesn't work, try cgroup v1
		// For cgroup v1, memory.oom_control is in /sys/fs/cgroup/<cgroupPath>/
		memOOMControlPath := filepath.Join(cgroupV2Root, w.cgroupPath, "memory.oom_control")
		file, err = os.Open(memOOMControlPath)
		if err != nil {
			return 0, fmt.Errorf("failed to open memory.events or memory.oom_control: %w", err)
		}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// For cgroup v2: Look for "oom_kill N" line in memory.events
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

		// For cgroup v1: Look for "oom_kill N" or "under_oom N" in memory.oom_control
		if strings.HasPrefix(line, "oom_kill ") || strings.HasPrefix(line, "under_oom ") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				count, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return 0, fmt.Errorf("failed to parse oom count: %w", err)
				}
				return count, nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading memory file: %w", err)
	}

	// oom_kill line not found, return 0
	return 0, nil
}

// GetCgroupPathFromPID reads the actual cgroup path from a process
// This works for both cgroup v1 and v2, and for any runtime
func GetCgroupPathFromPID(pid int) (string, error) {
	cgroupFile := fmt.Sprintf("/proc/%d/cgroup", pid)
	file, err := os.Open(cgroupFile)
	if err != nil {
		return "", fmt.Errorf("failed to open %s: %w", cgroupFile, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Format: hierarchy-ID:controller-list:cgroup-path
		// For cgroup v2: 0::/path/to/cgroup
		// For cgroup v1: N:subsystem:/path/to/cgroup

		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}

		hierarchyID := parts[0]
		cgroupPath := parts[2]

		// For cgroup v2 (unified hierarchy)
		if hierarchyID == "0" && cgroupPath != "" {
			// Remove leading slash as we'll join with cgroupV2Root
			return strings.TrimPrefix(cgroupPath, "/"), nil
		}

		// For cgroup v1, look for memory controller
		if strings.Contains(parts[1], "memory") && cgroupPath != "" {
			// For cgroup v1, the path is under /sys/fs/cgroup/memory/
			return filepath.Join("memory", strings.TrimPrefix(cgroupPath, "/")), nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error reading cgroup file: %w", err)
	}

	return "", fmt.Errorf("cgroup path not found for pid %d", pid)
}
