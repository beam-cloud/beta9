//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
)

type workloadMemory struct {
	limitBytes int64
	cgroup     *os.File
	done       chan struct{}
}

func newWorkloadMemory(limit int64) (*workloadMemory, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid workload memory limit %d", limit)
	}
	if err := os.WriteFile("/sys/fs/cgroup/cgroup.subtree_control", []byte("+memory"), 0o644); err != nil {
		return nil, err
	}
	for _, group := range []string{microvm.ControlCgroup, microvm.WorkloadExecCgroup} {
		if err := os.MkdirAll(group, 0o755); err != nil {
			return nil, err
		}
	}
	for _, setting := range []struct{ path, value string }{
		{microvm.ControlCgroup + "/cgroup.procs", "1"},
		{microvm.ControlCgroup + "/memory.min", strconv.FormatInt(microvm.GuestMemoryHeadroom, 10)},
		{microvm.WorkloadCgroup + "/memory.max", strconv.FormatInt(limit, 10)},
		{microvm.WorkloadCgroup + "/memory.swap.max", "0"},
		{microvm.WorkloadCgroup + "/memory.oom.group", "0"},
		{microvm.WorkloadCgroup + "/cgroup.subtree_control", "+memory"},
		// The parent caps the aggregate, including Docker; the exec leaf
		// exposes that limit to applications reading their own cgroup.
		{microvm.WorkloadExecCgroup + "/memory.max", strconv.FormatInt(limit, 10)},
	} {
		if err := os.WriteFile(setting.path, []byte(setting.value), 0o644); err != nil {
			return nil, fmt.Errorf("write %s: %w", setting.path, err)
		}
	}
	cgroup, err := os.Open(microvm.WorkloadExecCgroup)
	if err != nil {
		return nil, err
	}
	return &workloadMemory{limitBytes: limit, cgroup: cgroup, done: make(chan struct{})}, nil
}

func (w *workloadMemory) close() {
	close(w.done)
	_ = w.cgroup.Close()
}

func (w *workloadMemory) configureCommand(cmd *exec.Cmd) {
	cmd.Env = slices.DeleteFunc(cmd.Env, func(env string) bool {
		return strings.HasPrefix(env, microvm.WorkloadCgroupEnv+"=")
	})
	if filepath.Base(cmd.Path) == "goproc" {
		// Keep the manager with PID 1; goproc places its exec children.
		cmd.Env = append(cmd.Env, microvm.WorkloadCgroupEnv+"="+microvm.WorkloadExecCgroup)
		return
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(w.cgroup.Fd())
}

func (w *workloadMemory) readMemoryValue(name string) uint64 {
	data, err := os.ReadFile(filepath.Join(microvm.WorkloadCgroup, name))
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	return n
}

func (w *workloadMemory) readOOMKills() (uint64, error) {
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

func (w *workloadMemory) watchOOM(ctrl *control) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var previous uint64
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
		}
		kills, err := w.readOOMKills()
		if err != nil {
			logf("read workload OOM counter: %v", err)
			continue
		}
		if kills <= previous {
			continue
		}
		event := &microvm.ApplicationOOM{
			Kills:       kills - previous,
			MemoryLimit: w.limitBytes,
			MemoryUsage: w.readMemoryValue("memory.current"),
			MemoryPeak:  w.readMemoryValue("memory.peak"),
		}
		logf("application OOM: killed=%d usage=%d peak=%d limit=%d; sandbox remains running",
			event.Kills, event.MemoryUsage, event.MemoryPeak, event.MemoryLimit)
		if err := ctrl.send(microvm.Message{Type: microvm.MsgApplicationOOM, OOM: event}); err == nil {
			previous = kills
		}
	}
}
