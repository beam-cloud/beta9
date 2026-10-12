//go:build linux

package main

// Systemd owns PID 1 in persistent VMs. The same static executable runs as
// an ordinary systemd service to retain the worker's vsock and filesystem
// protocol. Transient boot configuration lives on /run, never on the disk.
import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

const systemdProcessFile = "/run/beam-vm/process.json"
const systemdAgentPIDFile = "/run/beam-vm/agent.pid"
const sshKeysUnit = `[Unit]
Description=Beam VM SSH host keys
Before=ssh.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'test -f /etc/ssh/ssh_host_ed25519_key || exec /usr/bin/ssh-keygen -q -t ed25519 -N "" -f /etc/ssh/ssh_host_ed25519_key'
`
const guestAgentUnit = `[Unit]
Description=Beam VM control and workload agent
DefaultDependencies=no
After=local-fs.target systemd-journald.service
Before=sysinit.target shutdown.target
Conflicts=shutdown.target
OnFailure=poweroff.target

[Service]
Type=forking
PIDFile=/run/beam-vm/agent.pid
ExecStart=/.beam/init --systemd-adopt-agent
Restart=no
Delegate=yes
KillMode=mixed
TimeoutStopSec=infinity
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

func bootSystemd(spec *specs.Spec, workload *workloadMemory) error {
	binary, err := systemdBinary()
	if err != nil {
		return err
	}

	if spec.Process == nil {
		return fmt.Errorf("missing VM workload")
	}

	if err := prepareSystemdTmp("/", processEnv(spec.Process, "BEAM_VM_DESKTOP") == "true"); err != nil {
		return err
	}

	// Give the guest its own hosts file. Container mounts can be read-only,
	// and KasmVNC/xauth require the guest hostname to resolve locally.
	if err := guestHosts(spec.Hostname); err != nil {
		return err
	}

	if err := workload.configureSystemd("/"); err != nil {
		return err
	}

	if err := writeSystemdBootFiles("/", spec.Process); err != nil {
		return err
	}

	agent := exec.Command("/.beam/init", "--systemd-agent")
	agent.Stdin, agent.Stdout, agent.Stderr = os.Stdin, os.Stdout, os.Stderr
	agent.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := agent.Start(); err != nil {
		return fmt.Errorf("start guest agent: %w", err)
	}

	if err := os.WriteFile(systemdAgentPIDFile, []byte(strconv.Itoa(agent.Process.Pid)+"\n"), 0600); err != nil {
		agent.Process.Kill()
		return err
	}

	env := []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C.UTF-8"}
	logf("boot phase: executing systemd")
	return unix.Exec(binary, []string{binary, "--unit=multi-user.target"}, env)
}

// Keep /tmp on the durable root. Only the managed desktop's display sockets
// and lock are boot state; restoring them can prevent Xvnc from starting.
func prepareSystemdTmp(root string, desktop bool) error {
	tmp := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmp, 0777); err != nil {
		return err
	}
	if err := os.Chmod(tmp, os.ModeSticky|0777); err != nil {
		return err
	}
	if desktop {
		for _, name := range []string{".X1-lock", ".X11-unix/X1"} {
			if err := os.Remove(filepath.Join(tmp, name)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func writeSystemdBootFiles(root string, process *specs.Process) error {
	if err := prepareVMIdentity(root, process); err != nil {
		return err
	}

	// The worker starts goproc directly. Set the environment and working
	// directory here, without a shell or SSH key generation on the exec path.
	process.Env = append(process.Env, "HOME=/root", "LANG=C.UTF-8")
	if processEnv(process, "BEAM_VM_DESKTOP") == "true" {
		process.Env = append(process.Env, "DISPLAY=:1", "XAUTHORITY=/run/beam-desktop/.Xauthority")
	}

	process.Cwd = "/workspace"
	if err := os.MkdirAll(filepath.Join(root, process.Cwd), 0755); err != nil {
		return err
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

	// Ubuntu's default D rule clears /tmp during sysinit, deleting durable
	// files and racing stdin uploads from the early exec agent. Retain normal
	// directory creation and age-based cleanup instead.
	tmpfilesDir := filepath.Join(root, "run/tmpfiles.d")
	if err := os.MkdirAll(tmpfilesDir, 0755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmpfilesDir, "tmp.conf"), []byte("d /tmp 1777 root root 10d\n"), 0644); err != nil {
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

	// Enable managed services before PID 1 reads its unit graph. An early
	// systemctl call can race creation of systemd's bus socket.
	wants := filepath.Join(unitDir, "multi-user.target.wants")
	if err := os.MkdirAll(wants, 0755); err != nil {
		return err
	}

	services := map[string]string{"beam-terminal.service": "/etc/systemd/system/beam-terminal.service"}
	if processEnv(process, "BEAM_VM_SSH") == "true" {
		// SSH validates its configuration in ExecStartPre, so keys must be
		// ready before the entire SSH service, including user pre-start hooks.
		if err := os.MkdirAll(filepath.Join(root, "run/sshd"), 0755); err != nil {
			return err
		}

		if err := os.WriteFile(filepath.Join(root, "run/beam-vm/authorized_keys"), []byte(processEnv(process, "BEAM_VM_SSH_PUBLIC_KEY")+"\n"), 0600); err != nil {
			return err
		}

		if err := os.WriteFile(filepath.Join(unitDir, "beam-ssh-keys.service"), []byte(sshKeysUnit), 0644); err != nil {
			return err
		}

		dropin := filepath.Join(unitDir, "ssh.service.d")
		if err := os.MkdirAll(dropin, 0755); err != nil {
			return err
		}

		if err := os.WriteFile(filepath.Join(dropin, "90-beam-keys.conf"), []byte("[Unit]\nRequires=beam-ssh-keys.service\nAfter=beam-ssh-keys.service\n"), 0644); err != nil {
			return err
		}

		services["ssh.service"] = "/lib/systemd/system/ssh.service"
	}

	if processEnv(process, "BEAM_VM_DESKTOP") == "true" {
		services["beam-desktop.service"] = "/etc/systemd/system/beam-desktop.service"
	}

	for name, target := range services {
		if err := os.Symlink(target, filepath.Join(wants, name)); err != nil {
			return err
		}
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

func vmMachineID(id string) (string, error) {
	compact := strings.ReplaceAll(strings.TrimPrefix(id, "vm-"), "-", "")
	if (len(compact) != 16 && len(compact) != 32) || strings.Trim(compact, "0123456789abcdef") != "" {
		return "", fmt.Errorf("invalid VM machine ID")
	}

	if len(compact) == 32 {
		return compact, nil // Preserve the identity of existing UUID-based VMs.
	}

	// Systemd requires 128 bits, independent of the public resource ID's size.
	sum := sha256.Sum256([]byte("vm-" + compact))
	return fmt.Sprintf("%x", sum[:16]), nil
}

func prepareVMIdentity(root string, process *specs.Process) error {
	id := processEnv(process, "BEAM_VM_ID")
	machineID, err := vmMachineID(id)
	if err != nil {
		return err
	}

	identityPath := filepath.Join(root, "etc/beam-vm-identity")
	previous, err := os.ReadFile(identityPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	sameID := strings.TrimSpace(string(previous)) == id
	if sameID {
		// Retain the durable identity across upgrades of the ID derivation.
		stored, err := os.ReadFile(filepath.Join(root, "etc/machine-id"))
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		if value := strings.TrimSpace(string(stored)); len(value) == 32 && strings.Trim(value, "0123456789abcdef") == "" {
			return nil
		}
	}

	// A cold start keeps host keys; a fork/template must not reuse them.
	if !sameID {
		keys, err := filepath.Glob(filepath.Join(root, "etc/ssh/ssh_host_*"))
		if err != nil {
			return err
		}

		for _, key := range keys {
			if err := os.Remove(key); err != nil {
				return err
			}
		}
	}

	// Write the new machine identity before marking the disk as this VM's root.
	if err := os.WriteFile(filepath.Join(root, "etc/machine-id"), []byte(machineID+"\n"), 0644); err != nil {
		return err
	}

	return os.WriteFile(identityPath, []byte(id+"\n"), 0644)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go logExecReadiness(ctx)
	workload, err := openWorkloadMemory()
	if err != nil {
		return 1, err
	}
	defer workload.close()
	return runAgent(&process, true, workload)
}

// The agent starts beside PID 1's exec of systemd. Once its ordinary service
// starts, move the existing process tree into that service's delegated cgroup.
// Its root-owned PIDFile lets systemd supervise the same agent without a
// second process manager, while retaining the normal shutdown ordering.
func adoptSystemdAgent() error {
	data, err := os.ReadFile(systemdAgentPIDFile)
	if err != nil {
		return err
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		return fmt.Errorf("invalid guest agent PID")
	}

	if err := unix.Kill(pid, 0); err != nil {
		return fmt.Errorf("guest agent exited before adoption: %w", err)
	}

	data, err = os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(data), "\n") {
		if group, ok := strings.CutPrefix(line, "0::"); ok {
			return moveAgentProcessTree(pid, filepath.Join("/sys/fs/cgroup", group, "cgroup.procs"), map[int]bool{})
		}
	}

	return fmt.Errorf("guest agent requires a unified service cgroup")
}

func moveAgentProcessTree(pid int, group string, visited map[int]bool) error {
	if visited[pid] || processInWorkloadCgroup(pid) {
		return nil
	}

	visited[pid] = true
	// Move the parent first: any subsequent forks already inherit the group.
	if err := os.WriteFile(group, []byte(strconv.Itoa(pid)), 0644); err != nil {
		if os.IsNotExist(err) || errors.Is(err, unix.ESRCH) {
			return nil // A short exec may exit during adoption.
		}

		return err
	}

	tasks, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	for _, task := range tasks {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", pid, task.Name()))
		if os.IsNotExist(err) {
			continue
		}

		if err != nil {
			return err
		}

		for _, child := range strings.Fields(string(data)) {
			childPID, err := strconv.Atoi(child)
			if err != nil {
				return err
			}

			if err := moveAgentProcessTree(childPID, group, visited); err != nil {
				return err
			}
		}
	}

	return nil
}

// Record when the workload opens exec without adding a boot dependency.
func logExecReadiness(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		conn, err := (&net.Dialer{Timeout: 20 * time.Millisecond}).DialContext(ctx, "tcp", "127.0.0.1:7111")
		if err == nil {
			conn.Close()
			logf("boot phase: exec TCP listening")
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}
