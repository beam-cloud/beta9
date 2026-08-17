package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Node names inside each daemon's block graph. One daemon serves exactly one
// volume, so the names are constant. The export points at a raw wrapper node:
// blockdev-snapshot-sync re-parents the qcow2 node underneath it, which keeps
// the NBD export stable across pivots.
const (
	qsdFileNodePrefix = "file-"
	qsdFmtNodePrefix  = "fmt-"
	qsdRootNode       = "root"
	qsdExportName     = "vol"

	qsdStartTimeout = 15 * time.Second
)

type qsdProcess struct {
	pid        int
	qmpSocket  string
	nbdSocket  string
	runtimeDir string
}

// startQSD launches one qemu-storage-daemon serving headPath over an NBD unix
// socket and waits until its QMP socket answers. fmtNode is the node name of
// the active qcow2 layer, which changes on every pivot.
func (m *Manager) startQSD(ctx context.Context, runtimeDir, headPath, fmtNode string, readOnly bool) (*qsdProcess, error) {
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		return nil, err
	}
	qmpSocket := filepath.Join(runtimeDir, "qmp.sock")
	nbdSocket := filepath.Join(runtimeDir, "nbd.sock")
	pidFile := filepath.Join(runtimeDir, "qsd.pid")
	for _, stale := range []string{qmpSocket, nbdSocket, pidFile} {
		if err := os.Remove(stale); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	fileBlockdev, _ := json.Marshal(map[string]any{
		"driver": "file", "filename": headPath, "node-name": qsdFileNodePrefix + fmtNode, "read-only": readOnly,
	})
	fmtBlockdev, _ := json.Marshal(map[string]any{
		"driver": "qcow2", "file": qsdFileNodePrefix + fmtNode, "node-name": fmtNode, "read-only": readOnly,
	})
	rootBlockdev, _ := json.Marshal(map[string]any{
		"driver": "raw", "file": fmtNode, "node-name": qsdRootNode, "read-only": readOnly,
	})

	args := []string{
		"--daemonize",
		"--pidfile", pidFile,
		"--chardev", fmt.Sprintf("socket,path=%s,server=on,wait=off,id=qmp0", qmpSocket),
		"--monitor", "chardev=qmp0",
		"--blockdev", string(fileBlockdev),
		"--blockdev", string(fmtBlockdev),
		"--blockdev", string(rootBlockdev),
		"--nbd-server", fmt.Sprintf("addr.type=unix,addr.path=%s", nbdSocket),
		"--export", fmt.Sprintf("type=nbd,id=%s,node-name=%s,name=%s,writable=%s", qsdExportName, qsdRootNode, qsdExportName, boolOnOff(!readOnly)),
	}

	if output, err := exec.CommandContext(ctx, m.binaries.QSD, args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("start qemu-storage-daemon: %w: %s", err, string(output))
	}

	pid, err := waitForPidFile(ctx, pidFile, qsdStartTimeout)
	if err != nil {
		return nil, err
	}
	if err := waitForSocket(ctx, nbdSocket, qsdStartTimeout); err != nil {
		killProcess(pid, m.binaries.qsdComm())
		return nil, err
	}
	return &qsdProcess{pid: pid, qmpSocket: qmpSocket, nbdSocket: nbdSocket, runtimeDir: runtimeDir}, nil
}

func boolOnOff(v bool) string {
	if v {
		return "on"
	}
	return "off"
}

func waitForPidFile(ctx context.Context, pidFile string, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		if data, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
				return pid, nil
			}
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("qemu-storage-daemon did not write %s within %s", pidFile, timeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("socket %s did not appear within %s", path, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// stopQSD asks the daemon to exit via QMP and escalates to SIGKILL. The kill
// path verifies process identity so a recycled pid is never signaled.
func (m *Manager) stopQSD(ctx context.Context, proc *qsdProcess) error {
	if proc == nil || proc.pid <= 0 {
		return nil
	}
	if client, err := dialQMP(ctx, proc.qmpSocket); err == nil {
		_ = client.quit(ctx)
		client.Close()
	}
	deadline := time.Now().Add(5 * time.Second)
	for processAlive(proc.pid, m.binaries.qsdComm()) {
		if time.Now().After(deadline) {
			killProcess(proc.pid, m.binaries.qsdComm())
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// processAlive reports whether pid is running and is the expected binary.
func processAlive(pid int, comm string) bool {
	if pid <= 0 {
		return false
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) == comm
}

func killProcess(pid int, comm string) {
	if processAlive(pid, comm) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}
