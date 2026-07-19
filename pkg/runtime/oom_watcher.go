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
	"github.com/shirou/gopsutil/v4/process"
)

const (
	cgroupV2Root           = "/sys/fs/cgroup"
	procRoot               = "/proc"
	memoryEventsFile       = "memory.events"
	oomWatcherPollInterval = 500 * time.Millisecond
	// For gVisor memory monitoring, trigger OOM when usage exceeds 95% of limit
	gvisorOOMThresholdPercent       = 95.0
	gvisorOOMProcessRefreshInterval = 5 * time.Second
)

// OOMWatcher interface for different runtime implementations
type OOMWatcher interface {
	Watch(onOOM func()) error
	Stop()
}

// CgroupOOMWatcher watches for OOM kills via cgroup v2 memory.events
// Works for runc and other traditional runtimes
type CgroupOOMWatcher struct {
	cgroupPath   string
	lastOOMCount int64
	ctx          context.Context
	cancel       context.CancelFunc
}

// GvisorOOMWatcher watches for OOM by monitoring memory usage vs limits
// Works for gVisor where cgroup files aren't accessible from host
type GvisorOOMWatcher struct {
	pid                int32
	memoryLimit        uint64 // in bytes
	ctx                context.Context
	cancel             context.CancelFunc
	processPIDs        []int32
	nextProcessRefresh time.Time
}

// NewCgroupOOMWatcher creates a new cgroup-based OOM watcher for runc
func NewCgroupOOMWatcher(ctx context.Context, cgroupPath string) *CgroupOOMWatcher {
	watcherCtx, cancel := context.WithCancel(ctx)
	return &CgroupOOMWatcher{
		cgroupPath: cgroupPath,
		ctx:        watcherCtx,
		cancel:     cancel,
	}
}

// NewGvisorOOMWatcher creates a new memory-monitoring OOM watcher for gVisor
func NewGvisorOOMWatcher(ctx context.Context, pid int, memoryLimitBytes uint64) *GvisorOOMWatcher {
	watcherCtx, cancel := context.WithCancel(ctx)
	return &GvisorOOMWatcher{
		pid:         int32(pid),
		memoryLimit: memoryLimitBytes,
		ctx:         watcherCtx,
		cancel:      cancel,
	}
}

// Watch starts watching for OOM events via cgroup memory.events
func (w *CgroupOOMWatcher) Watch(onOOM func()) error {
	// Read initial OOM count
	initialCount, err := w.readOOMKillCount()
	if err != nil {
		log.Warn().Str("cgroup", w.cgroupPath).Err(err).Msg("failed to read initial OOM count")
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

// Watch starts monitoring memory usage for gVisor containers
func (w *GvisorOOMWatcher) Watch(onOOM func()) error {
	if w.memoryLimit == 0 {
		return fmt.Errorf("memory limit is 0, cannot monitor OOM")
	}

	thresholdBytes := uint64(float64(w.memoryLimit) * gvisorOOMThresholdPercent / 100.0)
	log.Info().
		Int32("pid", w.pid).
		Uint64("memory_limit_mb", w.memoryLimit/1024/1024).
		Uint64("threshold_mb", thresholdBytes/1024/1024).
		Float64("threshold_percent", gvisorOOMThresholdPercent).
		Msg("starting OOM watcher")

	go func() {
		ticker := time.NewTicker(oomWatcherPollInterval)
		defer ticker.Stop()

		oomTriggered := false

		for {
			select {
			case <-w.ctx.Done():
				return
			case <-ticker.C:
				// Get memory usage for the process and all children
				memoryUsage, err := w.getMemoryUsage()
				if err != nil {
					// Process may have exited
					continue
				}

				// Check if we've exceeded the threshold
				if memoryUsage >= thresholdBytes && !oomTriggered {
					oomTriggered = true
					log.Warn().
						Int32("pid", w.pid).
						Uint64("memory_usage_mb", memoryUsage/1024/1024).
						Uint64("memory_limit_mb", w.memoryLimit/1024/1024).
						Float64("usage_percent", float64(memoryUsage)*100.0/float64(w.memoryLimit)).
						Msg("memory usage exceeded threshold - OOM likely")

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
func (w *CgroupOOMWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

// Stop stops the OOM watcher
func (w *GvisorOOMWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *GvisorOOMWatcher) getMemoryUsage() (uint64, error) {
	now := time.Now()
	if len(w.processPIDs) == 0 || !now.Before(w.nextProcessRefresh) {
		// gopsutil.Children scans every host PID; procfs exposes the tree directly.
		pids, err := readProcTree(procRoot, w.pid)
		if err == nil {
			w.processPIDs = pids
		} else if len(w.processPIDs) == 0 {
			return 0, err
		}
		w.nextProcessRefresh = now.Add(gvisorOOMProcessRefreshInterval)
	}

	var totalMemory uint64
	for _, pid := range w.processPIDs {
		proc, err := process.NewProcess(pid)
		if err != nil {
			if pid == w.pid {
				return 0, err
			}
			continue
		}
		memory, err := proc.MemoryInfo()
		if err == nil {
			totalMemory += memory.RSS
		}
	}

	return totalMemory, nil
}

func readProcTree(root string, rootPID int32) ([]int32, error) {
	pids := []int32{rootPID}
	seen := map[int32]struct{}{rootPID: {}}
	for i := 0; i < len(pids); i++ {
		pid := pids[i]
		taskRoot := filepath.Join(root, strconv.Itoa(int(pid)), "task")
		tasks, err := os.ReadDir(taskRoot)
		if err != nil {
			if pid == rootPID {
				return nil, fmt.Errorf("read process %d tasks: %w", pid, err)
			}
			continue
		}
		for _, task := range tasks {
			data, err := os.ReadFile(filepath.Join(taskRoot, task.Name(), "children"))
			if err != nil {
				continue
			}
			for _, field := range strings.Fields(string(data)) {
				value, err := strconv.ParseInt(field, 10, 32)
				child := int32(value)
				if err != nil || child <= 0 {
					continue
				}
				if _, ok := seen[child]; ok {
					continue
				}
				seen[child] = struct{}{}
				pids = append(pids, child)
			}
		}
	}
	return pids, nil
}

// readOOMKillCount reads the oom_kill counter from memory.events
func (w *CgroupOOMWatcher) readOOMKillCount() (int64, error) {
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
