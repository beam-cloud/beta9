package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	pb "github.com/beam-cloud/beta9/proto"
	"golang.org/x/crypto/ssh"
	"google.golang.org/protobuf/proto"
)

const (
	managedSSHListenPort     = 22
	managedSSHStatusRefresh  = 5 * time.Minute
	managedSSHCommandTimeout = 2 * time.Minute
	managedSSHPublicIPLookup = "https://api.ipify.org?format=text"
	managedSSHAuthorizedKeys = "/home/beam/.ssh/authorized_keys"
	managedSSHSudoers        = "/etc/sudoers.d/90-beam-managed"
	managedSSHDMainConfig    = "/etc/ssh/sshd_config"
	managedSSHDConfig        = "/etc/ssh/sshd_config.d/90-beam-managed.conf"
	managedSSHHostPublicKey  = "/etc/ssh/ssh_host_ed25519_key.pub"
)

type hostSSHManager struct {
	client      pb.GatewayServiceClient
	agentToken  string
	stderr      io.Writer
	httpClient  *http.Client
	publicIPURL string
	isListening func() bool

	mu             sync.Mutex
	desired        *pb.AgentSSHConfig
	applying       bool
	applied        uint64
	lastStatusSent time.Time
	sshdProcess    *exec.Cmd
}

func newHostSSHManager(client pb.GatewayServiceClient, agentToken string, stderr io.Writer) *hostSSHManager {
	if stderr == nil {
		stderr = io.Discard
	}
	return &hostSSHManager{
		client:      client,
		agentToken:  agentToken,
		stderr:      stderr,
		httpClient:  &http.Client{Timeout: 3 * time.Second},
		publicIPURL: managedSSHPublicIPLookup,
		isListening: managedSSHListening,
	}
}

func (m *hostSSHManager) reconcile(ctx context.Context, desired *pb.AgentSSHConfig) {
	if m == nil || desired == nil || !desired.Enabled || desired.Generation == 0 || strings.TrimSpace(desired.PublicKey) == "" {
		return
	}
	m.mu.Lock()
	m.desired = proto.Clone(desired).(*pb.AgentSSHConfig)
	if m.applied == desired.Generation && m.isListening != nil && !m.isListening() {
		// A directly supervised daemon can exit independently of the desired
		// generation. Force the idempotent installation path to restart it.
		m.applied = 0
	}
	needsApply := m.applied != desired.Generation
	needsRefresh := time.Since(m.lastStatusSent) >= managedSSHStatusRefresh
	if m.applying || (!needsApply && !needsRefresh) {
		m.mu.Unlock()
		return
	}
	m.applying = true
	m.mu.Unlock()
	go m.run(ctx)
}

func (m *hostSSHManager) run(ctx context.Context) {
	defer func() {
		m.mu.Lock()
		m.applying = false
		m.mu.Unlock()
	}()
	for {
		m.mu.Lock()
		desired := proto.Clone(m.desired).(*pb.AgentSSHConfig)
		applied := m.applied
		m.mu.Unlock()

		if applied != desired.Generation {
			m.report(ctx, desired.Generation, compute.MachineSSHStatusInstalling, "", "", "")
			hostFingerprint, err := m.apply(ctx, desired)
			if err != nil {
				fmt.Fprintf(m.stderr, "managed SSH reconcile failed: %v\n", err)
				m.report(ctx, desired.Generation, compute.MachineSSHStatusError, discoverPublicIP(ctx, m.httpClient, m.publicIPURL), "", err.Error())
				return
			}
			m.mu.Lock()
			m.applied = desired.Generation
			m.mu.Unlock()
			m.report(ctx, desired.Generation, compute.MachineSSHStatusReady, discoverPublicIP(ctx, m.httpClient, m.publicIPURL), hostFingerprint, "")
		} else {
			hostFingerprint, _ := managedSSHHostFingerprint()
			m.report(ctx, desired.Generation, compute.MachineSSHStatusReady, discoverPublicIP(ctx, m.httpClient, m.publicIPURL), hostFingerprint, "")
		}

		m.mu.Lock()
		latest := m.desired.Generation
		m.mu.Unlock()
		if latest == desired.Generation {
			return
		}
	}
}

func (m *hostSSHManager) apply(ctx context.Context, desired *pb.AgentSSHConfig) (string, error) {
	if runtime.GOOS != "linux" {
		return "", errors.New("managed SSH is supported only on Linux")
	}
	if os.Geteuid() != 0 {
		return "", errors.New("managed SSH installation requires a root agent")
	}
	if desired.Username != "" && desired.Username != "beam" {
		return "", fmt.Errorf("unsupported managed SSH username %q", desired.Username)
	}
	if _, _, _, _, err := ssh.ParseAuthorizedKey([]byte(strings.TrimSpace(desired.PublicKey))); err != nil {
		return "", fmt.Errorf("invalid desired SSH public key: %w", err)
	}

	sshdPath, err := ensureOpenSSHServer(ctx)
	if err != nil {
		return "", err
	}
	if err := ensureManagedSSHUser(ctx); err != nil {
		return "", err
	}
	if err := ensureManagedSSHFiles(desired.PublicKey); err != nil {
		return "", err
	}
	if err := os.MkdirAll("/run/sshd", 0o755); err != nil {
		return "", fmt.Errorf("create sshd runtime directory: %w", err)
	}
	if err := runManagedSSHCommand(ctx, "ssh-keygen", "-A"); err != nil {
		return "", fmt.Errorf("generate SSH host keys: %w", err)
	}
	if err := runManagedSSHCommand(ctx, sshdPath, "-t"); err != nil {
		return "", fmt.Errorf("validate sshd configuration: %w", err)
	}
	if err := validateManagedSSHDConfig(ctx, sshdPath); err != nil {
		return "", err
	}
	if err := m.reloadOrSuperviseSSHD(ctx, sshdPath); err != nil {
		return "", err
	}
	openManagedSSHFirewall(ctx)
	if err := waitForManagedSSHListener(ctx); err != nil {
		return "", err
	}
	return managedSSHHostFingerprint()
}

func ensureOpenSSHServer(ctx context.Context) (string, error) {
	if path, err := exec.LookPath("sshd"); err == nil {
		return path, nil
	}
	switch {
	case commandExists("apt-get"):
		if err := runManagedSSHCommand(ctx, "apt-get", "update"); err != nil {
			return "", err
		}
		if err := runManagedSSHCommand(ctx, "apt-get", "install", "-y", "openssh-server", "sudo"); err != nil {
			return "", err
		}
	case commandExists("dnf"):
		if err := runManagedSSHCommand(ctx, "dnf", "install", "-y", "openssh-server", "sudo"); err != nil {
			return "", err
		}
	case commandExists("yum"):
		if err := runManagedSSHCommand(ctx, "yum", "install", "-y", "openssh-server", "sudo"); err != nil {
			return "", err
		}
	case commandExists("apk"):
		if err := runManagedSSHCommand(ctx, "apk", "add", "--no-cache", "openssh-server", "sudo"); err != nil {
			return "", err
		}
	default:
		return "", errors.New("no supported package manager found (apt, dnf/yum, or apk)")
	}
	path, err := exec.LookPath("sshd")
	if err != nil {
		return "", errors.New("openssh-server installed but sshd was not found")
	}
	return path, nil
}

func ensureManagedSSHUser(ctx context.Context) error {
	if err := runManagedSSHCommand(ctx, "id", "-u", "beam"); err != nil {
		switch {
		case commandExists("useradd"):
			if err := runManagedSSHCommand(ctx, "useradd", "--create-home", "--shell", "/bin/bash", "beam"); err != nil {
				return fmt.Errorf("create beam user: %w", err)
			}
		case commandExists("adduser"):
			if err := runManagedSSHCommand(ctx, "adduser", "-D", "-s", "/bin/sh", "beam"); err != nil {
				return fmt.Errorf("create beam user: %w", err)
			}
		default:
			return errors.New("neither useradd nor adduser is available")
		}
	}
	if commandExists("passwd") {
		if err := runManagedSSHCommand(ctx, "passwd", "-l", "beam"); err != nil {
			if !commandExists("usermod") {
				return fmt.Errorf("lock beam password: %w", err)
			}
			if fallbackErr := runManagedSSHCommand(ctx, "usermod", "-L", "beam"); fallbackErr != nil {
				return fmt.Errorf("lock beam password: %w", errors.Join(err, fallbackErr))
			}
		}
	} else if commandExists("usermod") {
		if err := runManagedSSHCommand(ctx, "usermod", "-L", "beam"); err != nil {
			return fmt.Errorf("lock beam password: %w", err)
		}
	} else {
		return errors.New("neither passwd nor usermod is available to lock the beam account")
	}
	return nil
}

func ensureManagedSSHFiles(publicKey string) error {
	account, err := user.Lookup("beam")
	if err != nil {
		return fmt.Errorf("lookup beam user: %w", err)
	}
	uid64, err := strconv.ParseUint(account.Uid, 10, 32)
	if err != nil {
		return err
	}
	gid64, err := strconv.ParseUint(account.Gid, 10, 32)
	if err != nil {
		return err
	}
	uid, gid := int(uid64), int(gid64)
	sshDir := filepath.Dir(managedSSHAuthorizedKeys)
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(sshDir, 0o700); err != nil {
		return err
	}
	if err := os.Chown(sshDir, uid, gid); err != nil {
		return err
	}
	if err := writeManagedFileAtomic(managedSSHAuthorizedKeys, []byte(strings.TrimSpace(publicKey)+"\n"), 0o600, uid, gid); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(managedSSHSudoers), 0o750); err != nil {
		return err
	}
	if err := writeManagedFileAtomic(managedSSHSudoers, []byte("beam ALL=(ALL) NOPASSWD:ALL\n"), 0o440, 0, 0); err != nil {
		return err
	}
	if commandExists("visudo") {
		if err := runManagedSSHCommand(context.Background(), "visudo", "-cf", managedSSHSudoers); err != nil {
			return fmt.Errorf("validate sudo policy: %w", err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(managedSSHDConfig), 0o755); err != nil {
		return err
	}
	config := strings.Join([]string{
		"PubkeyAuthentication yes",
		"PasswordAuthentication no",
		"KbdInteractiveAuthentication no",
		"PermitRootLogin no",
		"AuthorizedKeysFile .ssh/authorized_keys",
		"",
	}, "\n")
	if err := writeManagedFileAtomic(managedSSHDConfig, []byte(config), 0o644, 0, 0); err != nil {
		return err
	}
	return ensureManagedSSHDConfigIncluded(managedSSHDMainConfig, managedSSHDConfig)
}

func ensureManagedSSHDConfigIncluded(mainConfigPath, dropInPath string) error {
	data, err := os.ReadFile(mainConfigPath)
	if err != nil {
		return fmt.Errorf("read sshd main configuration: %w", err)
	}
	include := "Include " + dropInPath
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.EqualFold(trimmed, include) {
			return nil
		}
		break
	}
	info, err := os.Stat(mainConfigPath)
	if err != nil {
		return fmt.Errorf("stat sshd main configuration: %w", err)
	}
	uid, gid := 0, 0
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		uid, gid = int(stat.Uid), int(stat.Gid)
	}
	updated := []byte(include + "\n" + string(data))
	if err := writeManagedFileAtomic(mainConfigPath, updated, info.Mode().Perm(), uid, gid); err != nil {
		return fmt.Errorf("enable managed sshd configuration: %w", err)
	}
	return nil
}

func writeManagedFileAtomic(path string, data []byte, mode os.FileMode, uid, gid int) error {
	file, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return err
	}
	if err := file.Chown(uid, gid); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func (m *hostSSHManager) reloadOrSuperviseSSHD(ctx context.Context, sshdPath string) error {
	if commandExists("systemctl") {
		for _, service := range []string{"ssh", "sshd"} {
			if err := runManagedSSHCommand(ctx, "systemctl", "enable", "--now", service); err == nil {
				if err := runManagedSSHCommand(ctx, "systemctl", "reload", service); err == nil {
					return nil
				}
				if err := runManagedSSHCommand(ctx, "systemctl", "restart", service); err == nil {
					return nil
				}
			}
		}
	}
	if commandExists("service") {
		for _, service := range []string{"ssh", "sshd"} {
			if err := runManagedSSHCommand(ctx, "service", service, "restart"); err == nil {
				return nil
			}
		}
	}
	if managedSSHListening() {
		if commandExists("pkill") {
			if err := runManagedSSHCommand(ctx, "pkill", "-HUP", "-x", "sshd"); err == nil {
				return nil
			}
		}
		return errors.New("sshd is listening but could not be reloaded")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sshdProcess != nil && m.sshdProcess.Process != nil {
		if err := m.sshdProcess.Process.Signal(syscall.Signal(0)); err == nil {
			_ = m.sshdProcess.Process.Kill()
		}
		m.sshdProcess = nil
	}
	// This daemon belongs to the agent process, not to an individual stream.
	// A gateway reconnect must not cancel host SSH.
	cmd := exec.Command(sshdPath, "-D", "-e")
	cmd.Stdout = m.stderr
	cmd.Stderr = m.stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start supervised sshd: %w", err)
	}
	m.sshdProcess = cmd
	go func() {
		_ = cmd.Wait()
		m.mu.Lock()
		if m.sshdProcess == cmd {
			m.sshdProcess = nil
		}
		m.mu.Unlock()
	}()
	return nil
}

func runManagedSSHCommand(ctx context.Context, name string, args ...string) error {
	_, err := managedSSHCommandOutput(ctx, name, args...)
	return err
}

func managedSSHCommandOutput(ctx context.Context, name string, args ...string) (string, error) {
	commandCtx, cancel := context.WithTimeout(ctx, managedSSHCommandTimeout)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, name, args...).CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message != "" {
			return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, message)
		}
		return "", fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return string(output), nil
}

func validateManagedSSHDConfig(ctx context.Context, sshdPath string) error {
	output, err := managedSSHCommandOutput(ctx, sshdPath, "-T")
	if err != nil {
		return fmt.Errorf("inspect effective sshd configuration: %w", err)
	}
	settings := map[string]string{}
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(strings.ToLower(line))
		if len(fields) >= 2 {
			settings[fields[0]] = fields[1]
		}
	}
	for name, want := range map[string]string{
		"pubkeyauthentication":         "yes",
		"passwordauthentication":       "no",
		"kbdinteractiveauthentication": "no",
		"permitrootlogin":              "no",
	} {
		if settings[name] != want {
			return fmt.Errorf("effective sshd setting %s=%q, want %q", name, settings[name], want)
		}
	}
	return nil
}

func openManagedSSHFirewall(ctx context.Context) {
	switch {
	case commandExists("ufw"):
		_ = runManagedSSHCommand(ctx, "ufw", "allow", "22/tcp")
	case commandExists("firewall-cmd"):
		_ = runManagedSSHCommand(ctx, "firewall-cmd", "--permanent", "--add-service=ssh")
		_ = runManagedSSHCommand(ctx, "firewall-cmd", "--reload")
	}
}

func waitForManagedSSHListener(ctx context.Context) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if managedSSHListening() {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("sshd did not begin listening on port 22")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func managedSSHListening() bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(managedSSHListenPort)), 250*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func managedSSHHostFingerprint() (string, error) {
	data, err := os.ReadFile(managedSSHHostPublicKey)
	if err != nil {
		return "", err
	}
	key, _, _, _, err := ssh.ParseAuthorizedKey(data)
	if err != nil {
		return "", err
	}
	return ssh.FingerprintSHA256(key), nil
}

func discoverPublicIP(ctx context.Context, client *http.Client, url string) string {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}
	response, err := client.Do(request)
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ""
	}
	var buffer [64]byte
	n, _ := response.Body.Read(buffer[:])
	value := strings.TrimSpace(string(buffer[:n]))
	if net.ParseIP(value) == nil {
		return ""
	}
	return value
}

func (m *hostSSHManager) report(ctx context.Context, generation uint64, status, publicIP, hostFingerprint, errMessage string) {
	reportCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := m.client.UpdateAgentSSHStatus(reportCtx, &pb.UpdateAgentSSHStatusRequest{
		AgentToken:         m.agentToken,
		Generation:         generation,
		Status:             status,
		PublicIp:           publicIP,
		HostKeyFingerprint: hostFingerprint,
		Error:              errMessage,
		ListenPort:         managedSSHListenPort,
	})
	if err != nil {
		fmt.Fprintf(m.stderr, "managed SSH status update failed: %v\n", err)
		return
	}
	if !response.Ok {
		fmt.Fprintf(m.stderr, "managed SSH status rejected: %s\n", response.ErrMsg)
		return
	}
	m.mu.Lock()
	m.lastStatusSent = time.Now()
	m.mu.Unlock()
}
