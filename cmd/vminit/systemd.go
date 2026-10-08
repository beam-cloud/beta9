//go:build linux

package main

// Systemd owns PID 1 in persistent VMs. The same static executable runs as
// an ordinary systemd service to retain the worker's vsock and filesystem
// protocol. Transient boot configuration lives on /run, never on the disk.
import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

const systemdProcessFile = "/run/beam-vm/process.json"
const guestAgentUnit = `[Unit]
Description=Beam VM control and workload agent
DefaultDependencies=no
After=local-fs.target systemd-journald.service
Before=sysinit.target shutdown.target
Conflicts=shutdown.target

[Service]
Type=exec
ExecStart=/.beam/init --systemd-agent
Restart=no
Delegate=yes
KillMode=mixed
TimeoutStopSec=30
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=sysinit.target
`

func processEnv(proc *specs.Process, key string) string {
	if proc == nil {
		return ""
	}
	// Runtime overrides follow image defaults in the OCI environment.
	for i := len(proc.Env) - 1; i >= 0; i-- {
		entry := proc.Env[i]
		if value, ok := strings.CutPrefix(entry, key+"="); ok {
			return value
		}
	}
	return ""
}

func systemdBinary() (string, error) {
	for _, name := range []string{"/usr/lib/systemd/systemd", "/lib/systemd/systemd"} {
		if info, err := os.Stat(name); err == nil && info.Mode()&0111 != 0 {
			return name, nil
		}
	}
	return "", fmt.Errorf("persistent VMs require systemd in the image; install systemd and dbus or use the Beam VM image")
}

func bootSystemd(spec *specs.Spec) error {
	binary, err := systemdBinary()
	if err != nil {
		return err
	}
	if spec.Process == nil {
		return fmt.Errorf("missing VM workload")
	}
	// Unlike the durable root, /tmp is boot state. In particular, X display
	// sockets and lock files must not be restored from a disk snapshot.
	if err := os.MkdirAll("/tmp", 01777); err != nil {
		return err
	}
	if err := unix.Mount("tmpfs", "/tmp", "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "mode=1777"); err != nil {
		return err
	}
	// Give the guest its own hosts file. Container mounts can be read-only,
	// and KasmVNC/xauth require the guest hostname to resolve locally.
	if err := guestHosts(spec.Hostname); err != nil {
		return err
	}
	if err := writeSystemdBootFiles("/", spec.Process); err != nil {
		return err
	}
	env := []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C.UTF-8"}
	return unix.Exec(binary, []string{binary, "--unit=multi-user.target"}, env)
}

func writeSystemdBootFiles(root string, process *specs.Process) error {
	// Machine identity is stable on start, and distinct on fork/template.
	if id := strings.ReplaceAll(processEnv(process, "BEAM_VM_ID"), "-", ""); id != "" {
		if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" {
			return fmt.Errorf("invalid VM machine ID")
		}
		if err := os.WriteFile(filepath.Join(root, "etc/machine-id"), []byte(id+"\n"), 0644); err != nil {
			return err
		}
	}
	processFile := filepath.Join(root, systemdProcessFile)
	if err := os.MkdirAll(filepath.Dir(processFile), 0700); err != nil {
		return err
	}
	data, err := json.Marshal(process)
	if err != nil {
		return err
	}
	if err := os.WriteFile(processFile, data, 0600); err != nil {
		return err
	}
	// Apply VM environment before any enabled user units start, including
	// units ordered before multi-user.target. Keep secrets on transient /run.
	managerDir := filepath.Join(root, "run/systemd/system.conf.d")
	if err := os.MkdirAll(managerDir, 0755); err != nil {
		return err
	}
	var manager strings.Builder
	manager.WriteString("[Manager]\nDefaultEnvironment=")
	for _, entry := range process.Env {
		manager.WriteString(" " + systemdQuote(entry))
	}
	manager.WriteString("\n")
	if err := os.WriteFile(filepath.Join(managerDir, "90-beam-vm.conf"), []byte(manager.String()), 0600); err != nil {
		return err
	}
	unitDir := filepath.Join(root, "run/systemd/system")
	if err := os.MkdirAll(filepath.Join(unitDir, "sysinit.target.wants"), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(unitDir, "beam-guest.service"), []byte(guestAgentUnit), 0644); err != nil {
		return err
	}
	if err := os.Symlink("../beam-guest.service", filepath.Join(unitDir, "sysinit.target.wants/beam-guest.service")); err != nil {
		return err
	}
	// The NIC is configured before exec. Network managers must not replace
	// the scheduler's addresses; user services still have a real systemd.
	for _, unit := range []string{"systemd-networkd.service", "systemd-networkd.socket", "systemd-networkd-wait-online.service", "systemd-resolved.service", "NetworkManager.service"} {
		if err := os.Symlink("/dev/null", filepath.Join(unitDir, unit)); err != nil {
			return err
		}
	}
	return nil
}

func systemdQuote(value string) string {
	replace := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n", "\r", "\\r", "\t", "\\t", "%", "%%")
	return "\"" + replace.Replace(value) + "\""
}

func guestHosts(hostname string) error {
	seed, _ := os.ReadFile("/etc/hosts")
	if err := unix.Unmount("/etc/hosts", unix.MNT_DETACH); err != nil && err != unix.EINVAL {
		return fmt.Errorf("unmount guest hosts: %w", err)
	}
	if data, err := os.ReadFile("/etc/hosts"); err == nil && len(data) > 0 {
		seed = data
	}
	lines := []string{}
	for _, line := range strings.Split(string(seed), "\n") {
		if line != "" && !strings.HasSuffix(line, "# Beam VM hostname") {
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 {
		lines = append(lines, "127.0.0.1 localhost", "::1 localhost")
	}
	if hostname != "" {
		lines = append(lines, "127.0.1.1 "+hostname+" # Beam VM hostname")
	}
	if err := os.WriteFile("/etc/hosts", []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		return err
	}
	if err := unix.Unmount("/etc/hostname", unix.MNT_DETACH); err != nil && err != unix.EINVAL && err != unix.ENOENT {
		return fmt.Errorf("unmount guest hostname: %w", err)
	}
	return os.WriteFile("/etc/hostname", []byte(hostname+"\n"), 0644)
}

func runSystemdAgent() (int, error) {
	data, err := os.ReadFile(systemdProcessFile)
	if err != nil {
		return 1, err
	}
	var process specs.Process
	if err := json.Unmarshal(data, &process); err != nil {
		return 1, err
	}
	if err := serveFS(microvm.FSPort); err != nil {
		return 1, err
	}
	ctrl, err := dialControl(microvm.ControlPort)
	if err != nil {
		return 1, err
	}
	defer ctrl.close()
	ctrl.systemd = true
	return runProcess(&process, ctrl)
}
