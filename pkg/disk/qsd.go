package disk

import (
	"context"
	"encoding/json"
	"errors"
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
// volume. The NBD export attaches directly to the active qcow2 node: pivots
// re-parent it onto each new overlay, and the head stays a root node, which
// block-commit (live compaction) requires.
const (
	qsdFileNodePrefix = "file-"
	qsdFmtNodePrefix  = "fmt-"
	qsdExportName     = "vol"

	qsdStartTimeout = 15 * time.Second

	// Wait loops in this package (socket and pidfile appearance, NBD settle,
	// daemon shutdown) start polling at minPollInterval and back off to
	// maxPollInterval. The conditions usually hold within a millisecond or
	// two of the preceding exec returning; a fixed 50ms tick was costing
	// most of the attach time.
	minPollInterval = time.Millisecond
	maxPollInterval = 50 * time.Millisecond
)

// waitFor polls ready until it returns true, the timeout elapses, or ctx is
// done. The interval doubles from minPollInterval up to maxPollInterval.
func waitFor(ctx context.Context, timeout time.Duration, ready func() bool) error {
	deadline := time.Now().Add(timeout)
	interval := minPollInterval
	for {
		if ready() {
			return nil
		}
		if time.Now().After(deadline) {
			return errTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
		interval = min(interval*2, maxPollInterval)
	}
}

var errTimeout = errors.New("timed out")

// fmtNodeName is the qcow2 node name of the head created by pivot number n.
func fmtNodeName(pivot int) string {
	return fmt.Sprintf("%s%d", qsdFmtNodePrefix, pivot)
}

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

	args := []string{
		"--daemonize",
		"--pidfile", pidFile,
		"--chardev", fmt.Sprintf("socket,path=%s,server=on,wait=off,id=qmp0", qmpSocket),
		"--monitor", "chardev=qmp0",
		"--blockdev", string(fileBlockdev),
		"--blockdev", string(fmtBlockdev),
		"--nbd-server", fmt.Sprintf("addr.type=unix,addr.path=%s", nbdSocket),
		"--export", fmt.Sprintf("type=nbd,id=%s,node-name=%s,name=%s,writable=%s", qsdExportName, fmtNode, qsdExportName, boolOnOff(!readOnly)),
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
	pid := 0
	err := waitFor(ctx, timeout, func() bool {
		data, err := os.ReadFile(pidFile)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil && pid > 0
	})
	if errors.Is(err, errTimeout) {
		return 0, fmt.Errorf("qemu-storage-daemon did not write %s within %s", pidFile, timeout)
	}
	return pid, err
}

func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	err := waitFor(ctx, timeout, func() bool { return fileExists(path) })
	if errors.Is(err, errTimeout) {
		return fmt.Errorf("socket %s did not appear within %s", path, timeout)
	}
	return err
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
	if err := waitFor(context.Background(), 5*time.Second, func() bool {
		return !processAlive(proc.pid, m.binaries.qsdComm())
	}); err != nil {
		killProcess(proc.pid, m.binaries.qsdComm())
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
