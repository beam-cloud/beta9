//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
)

func setupWorkloadMemory(limit int64) error {
	if limit <= 0 {
		return fmt.Errorf("invalid workload memory limit %d", limit)
	}
	const root = "/sys/fs/cgroup"
	if err := os.WriteFile(filepath.Join(root, "cgroup.subtree_control"), []byte("+memory"), 0o644); err != nil {
		return err
	}
	for _, group := range []string{microvm.ControlCgroup, microvm.WorkloadCgroup} {
		if err := os.MkdirAll(group, 0o755); err != nil {
			return err
		}
	}
	for _, setting := range []struct{ path, value string }{
		{microvm.ControlCgroup + "/cgroup.procs", "1"},
		{microvm.ControlCgroup + "/memory.min", strconv.FormatInt(microvm.GuestMemoryHeadroom, 10)},
		{microvm.WorkloadCgroup + "/memory.max", strconv.FormatInt(limit, 10)},
		{microvm.WorkloadCgroup + "/memory.swap.max", "0"},
		{microvm.WorkloadCgroup + "/memory.oom.group", "0"},
		{microvm.WorkloadCgroup + "/cgroup.subtree_control", "+memory"},
	} {
		if err := os.WriteFile(setting.path, []byte(setting.value), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", setting.path, err)
		}
	}
	if err := os.MkdirAll(microvm.WorkloadExecCgroup, 0o755); err != nil {
		return err
	}
	// Expose the same limit at the exec leaf for runtimes that read only their
	// own cgroup. The parent still covers the aggregate, including Docker.
	return os.WriteFile(microvm.WorkloadExecCgroup+"/memory.max", []byte(strconv.FormatInt(limit, 10)), 0o644)
}

func workloadMemoryValue(name string) uint64 {
	data, err := os.ReadFile(filepath.Join(microvm.WorkloadCgroup, name))
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	return n
}

func workloadOOMKills() (uint64, error) {
	data, err := os.ReadFile(filepath.Join(microvm.WorkloadCgroup, "memory.events"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "oom_kill" {
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("memory.events has no oom_kill counter")
}

func watchWorkloadOOM(ctrl *control) {
	var previous uint64
	for {
		kills, err := workloadOOMKills()
		if err != nil {
			logf("read workload OOM counter: %v", err)
		} else if kills > previous {
			event := &microvm.ApplicationOOM{
				Kills:       kills - previous,
				MemoryLimit: int64(workloadMemoryValue("memory.max")),
				MemoryUsage: workloadMemoryValue("memory.current"),
				MemoryPeak:  workloadMemoryValue("memory.peak"),
			}
			logf("application OOM: killed=%d usage=%d peak=%d limit=%d; sandbox remains running", event.Kills, event.MemoryUsage, event.MemoryPeak, event.MemoryLimit)
			if err := ctrl.send(microvm.Message{Type: microvm.MsgApplicationOOM, OOM: event}); err == nil {
				previous = kills
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}
