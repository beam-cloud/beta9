//go:build linux

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
)

func TestWorkloadCommandPlacement(t *testing.T) {
	group, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	workload := &workloadMemory{cgroup: group}
	manager := exec.Command("/.beam/goproc")
	manager.SysProcAttr = &syscall.SysProcAttr{}
	manager.Env = []string{"HOME=/root", microvm.WorkloadCgroupEnv + "=/wrong"}
	workload.configureCommand(manager)
	if manager.SysProcAttr.UseCgroupFD || strings.Join(manager.Env, ",") != "HOME=/root,"+microvm.WorkloadCgroupEnv+"="+microvm.WorkloadExecCgroup {
		t.Fatal("process manager must remain outside the budget and receive the authoritative child cgroup")
	}
	child := exec.Command("/bin/sh")
	child.SysProcAttr = &syscall.SysProcAttr{}
	workload.configureCommand(child)
	if !child.SysProcAttr.UseCgroupFD || child.SysProcAttr.CgroupFD != int(group.Fd()) {
		t.Fatal("ordinary workloads must enter the cgroup atomically at clone")
	}
}

func TestSystemdMemoryConfiguration(t *testing.T) {
	analyze, err := exec.LookPath("systemd-analyze")
	if err != nil {
		t.Skip("systemd-analyze is unavailable")
	}
	root := t.TempDir()
	workload := &workloadMemory{limitBytes: 512 << 20}
	if err := workload.configureSystemd(root); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "run/systemd/system")
	defaults, err := os.ReadFile(filepath.Join(dir, "service.d/00-workload.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(defaults), "OOMPolicy=continue\n") {
		t.Fatal("a service's CI parent must survive when a child is OOM-killed")
	}
	for _, name := range []string{"probe.service", "beam-guest.service"} {
		unit := "[Unit]\nDefaultDependencies=no\n[Service]\nExecStart=/bin/true\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(unit), 0644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(analyze, "verify", "--man=no", "probe.service", "beam-guest.service", microvm.WorkloadSlice, microvm.ControlSlice)
	cmd.Env = append(os.Environ(), "SYSTEMD_UNIT_PATH="+dir+":/usr/lib/systemd/system")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("invalid systemd memory configuration: %v\n%s", err, out)
	}
}
