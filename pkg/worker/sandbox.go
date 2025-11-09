package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// Timing strategy for Docker daemon startup:
	// 1. Wait up to 10s for goproc to be ready (usually takes 100-500ms)
	// 2. Setup cgroups (fast, ~100ms)
	// 3. Start dockerd in background
	// 4. Wait up to 30s for dockerd to be ready (usually takes 2-5s)
	goprocReadyTimeout             = 10 * time.Second
	goprocInitialBackoff           = 100 * time.Millisecond
	goprocMaxBackoff               = 2 * time.Second
	goprocBackoffMultiplier        = 1.5
	goprocCommandCompletionWait    = 100 * time.Millisecond
	cgroupSetupCompletionWait      = 500 * time.Millisecond
	dockerDaemonStartupTimeout     = 30 * time.Second
	dockerDaemonReadyPollInterval  = 1 * time.Second
	dockerInfoCommandTimeout       = 2 * time.Second
	dockerInfoCommandCheckInterval = 100 * time.Millisecond
)

// startDockerDaemon starts the Docker daemon inside a sandbox container
func (s *Worker) startDockerDaemon(ctx context.Context, containerId string, instance *ContainerInstance) {
	if instance.SandboxProcessManager == nil {
		log.Error().Str("container_id", containerId).Msg("sandbox process manager not available")
		return
	}

	log.Info().Str("container_id", containerId).Msg("starting docker daemon in sandbox")

	// Setup cgroups for Docker-in-Docker
	if err := s.setupDockerCgroups(ctx, containerId, instance); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to setup cgroups")
		return
	}

	// Setup networking for Docker-in-gVisor
	// Per https://gvisor.dev/docs/tutorials/docker-in-gvisor/
	if err := s.setupDockerNetworking(ctx, containerId, instance); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to setup docker networking")
		return
	}

	// Start dockerd with gVisor-compatible flags
	// Must use --iptables=false --ip6tables=false per gVisor guidance
	cmd := []string{
		"dockerd",
		"--iptables=false",
		"--ip6tables=false",
	}
	
	pid, err := instance.SandboxProcessManager.Exec(cmd, "/", []string{}, true)

	if err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to start docker daemon")
		return
	}

	log.Info().Str("container_id", containerId).Int("pid", pid).Msg("docker daemon started")

	// Wait for daemon to be ready
	s.waitForDockerDaemon(ctx, containerId, instance, pid)
}

// setupDockerCgroups configures cgroups required for Docker-in-Docker in gVisor
func (s *Worker) setupDockerCgroups(ctx context.Context, containerId string, instance *ContainerInstance) error {
	script := `
set -e
mount -t tmpfs cgroups /sys/fs/cgroup
mkdir -p /sys/fs/cgroup/devices
mount -t cgroup -o devices devices /sys/fs/cgroup/devices
`

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return err
	}

	// Wait for cgroup setup to complete
	time.Sleep(cgroupSetupCompletionWait)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		return fmt.Errorf("cgroup setup failed with exit code %d", exitCode)
	}

	return nil
}

// setupDockerNetworking configures networking for Docker-in-gVisor
// Follows the official gVisor guidance: https://gvisor.dev/docs/tutorials/docker-in-gvisor/
// This is based on: https://github.com/google/gvisor/blob/master/images/basic/docker/start-dockerd.sh
//
// This function automatically installs required networking tools (iproute2, iptables) if not present
// Supports: Ubuntu, Debian, Alpine, RHEL, CentOS, Fedora
func (s *Worker) setupDockerNetworking(ctx context.Context, containerId string, instance *ContainerInstance) error {
	// The gVisor approach:
	// 1. Enable IP forwarding
	// 2. Setup SNAT rules using iptables-legacy (critical for bridge networking to work)
	// 3. Start dockerd with --iptables=false --ip6tables=false
	
	// Ensure required tools are installed
	// Try to auto-install if not present (works for most common base images)
	installScript := `
# Check and install required networking tools
install_tools() {
    # Check if tools already exist
    if command -v ip >/dev/null 2>&1 && command -v iptables-legacy >/dev/null 2>&1; then
        echo "Networking tools already installed"
        return 0
    fi
    
    echo "Installing required networking tools..."
    
    # Try different package managers
    if command -v apt-get >/dev/null 2>&1; then
        # Debian/Ubuntu
        apt-get update -qq && apt-get install -y -qq iproute2 iptables >/dev/null 2>&1
    elif command -v apk >/dev/null 2>&1; then
        # Alpine
        apk add --no-cache iproute2 iptables >/dev/null 2>&1
    elif command -v yum >/dev/null 2>&1; then
        # RHEL/CentOS
        yum install -y -q iproute iptables >/dev/null 2>&1
    elif command -v dnf >/dev/null 2>&1; then
        # Fedora
        dnf install -y -q iproute iptables >/dev/null 2>&1
    else
        echo "ERROR: No supported package manager found (apt, apk, yum, dnf)"
        echo "Please install 'iproute2' and 'iptables' packages in your Docker image"
        return 1
    fi
    
    # Verify installation
    if ! command -v ip >/dev/null 2>&1; then
        echo "ERROR: Failed to install 'ip' command"
        return 1
    fi
    if ! command -v iptables-legacy >/dev/null 2>&1; then
        echo "ERROR: Failed to install 'iptables-legacy' command"
        return 1
    fi
    
    echo "Networking tools installed successfully"
    return 0
}

install_tools
`
	
	installPid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", installScript}, "/", []string{}, false)
	if err == nil {
		time.Sleep(cgroupSetupCompletionWait * 3) // Give more time for package installation
		installExitCode, _ := instance.SandboxProcessManager.Status(installPid)
		if installExitCode != 0 {
			stderr, _ := instance.SandboxProcessManager.Stderr(installPid)
			stdout, _ := instance.SandboxProcessManager.Stdout(installPid)
			return fmt.Errorf("failed to install networking tools:\nstdout: %s\nstderr: %s", stdout, stderr)
		}
		stdout, _ := instance.SandboxProcessManager.Stdout(installPid)
		log.Info().Str("container_id", containerId).Str("output", stdout).Msg("networking tools ready")
	}
	
	script := `
set -e

# Get default network interface and its IP address
dev=$(ip route show default | sed 's/.*\sdev\s\(\S*\)\s.*$/\1/')
addr=$(ip addr show dev "$dev" | grep -w inet | sed 's/^\s*inet\s\(\S*\)\/.*$/\1/')

# Enable IPv4 forwarding
echo 1 > /proc/sys/net/ipv4/ip_forward

# Setup SNAT for Docker bridge networking
# This allows containers on Docker's bridge network to access the external network
iptables-legacy -t nat -A POSTROUTING -o "$dev" -j SNAT --to-source "$addr" -p tcp
iptables-legacy -t nat -A POSTROUTING -o "$dev" -j SNAT --to-source "$addr" -p udp

echo "Docker networking configured for $dev ($addr)"
`

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return fmt.Errorf("failed to execute networking setup: %w", err)
	}

	time.Sleep(cgroupSetupCompletionWait)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		stderr, _ := instance.SandboxProcessManager.Stderr(pid)
		stdout, _ := instance.SandboxProcessManager.Stdout(pid)
		return fmt.Errorf("networking setup failed (exit %d)\nstdout: %s\nstderr: %s", exitCode, stdout, stderr)
	}

	stdout, _ := instance.SandboxProcessManager.Stdout(pid)
	log.Info().Str("container_id", containerId).Str("output", stdout).Msg("docker networking configured")
	return nil
}

// waitForProcessManager waits for the goproc process manager to be ready to accept commands
// Uses exponential backoff to efficiently wait for goproc startup
// This should be called ONCE during container initialization, not on every exec
func (s *Worker) waitForProcessManager(ctx context.Context, containerId string, instance *ContainerInstance) bool {
	start := time.Now()
	backoff := goprocInitialBackoff

	for time.Since(start) < goprocReadyTimeout {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		// Try a simple echo command to check if goproc is ready
		pid, err := instance.SandboxProcessManager.Exec(
			[]string{"echo", "ready"},
			"/",
			[]string{},
			false,
		)

		if err == nil {
			// Successfully executed - goproc is ready
			time.Sleep(goprocCommandCompletionWait)
			instance.SandboxProcessManager.Status(pid)
			log.Info().
				Str("container_id", containerId).
				Dur("wait_time", time.Since(start)).
				Msg("process manager is ready")
			return true
		}

		// Not ready yet - wait with exponential backoff
		time.Sleep(backoff)

		backoff = time.Duration(float64(backoff) * goprocBackoffMultiplier)
		if backoff > goprocMaxBackoff {
			backoff = goprocMaxBackoff
		}
	}

	log.Error().
		Str("container_id", containerId).
		Msg("process manager did not become ready within timeout")

	return false
}

// waitForDockerDaemon waits for the Docker daemon to be ready to accept commands
func (s *Worker) waitForDockerDaemon(ctx context.Context, containerId string, instance *ContainerInstance, daemonPid int) {
	timeout := time.After(dockerDaemonStartupTimeout)
	ticker := time.NewTicker(dockerDaemonReadyPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Container is being stopped, exit quietly
			return

		case <-timeout:
			log.Warn().
				Str("container_id", containerId).
				Msg("docker daemon startup timeout - daemon may not be fully ready")
			return

		case <-ticker.C:
			// Check if dockerd crashed
			if s.dockerDaemonCrashed(containerId, instance, daemonPid) {
				return
			}

			// Check if daemon is ready
			if s.isDockerDaemonReady(containerId, instance) {
				log.Info().Str("container_id", containerId).Msg("docker daemon is ready")
				return
			}
		}
	}
}

// dockerDaemonCrashed checks if dockerd process has exited
func (s *Worker) dockerDaemonCrashed(containerId string, instance *ContainerInstance, daemonPid int) bool {
	exitCode, err := instance.SandboxProcessManager.Status(daemonPid)
	if err == nil && exitCode >= 0 {
		stdout, _ := instance.SandboxProcessManager.Stdout(daemonPid)
		stderr, _ := instance.SandboxProcessManager.Stderr(daemonPid)
		log.Error().
			Str("container_id", containerId).
			Int("exit_code", exitCode).
			Str("stdout", stdout).
			Str("stderr", stderr).
			Msg("docker daemon crashed")
		return true
	}
	return false
}

// isDockerDaemonReady checks if docker daemon responds to 'docker info'
func (s *Worker) isDockerDaemonReady(containerId string, instance *ContainerInstance) bool {
	checkPid, err := instance.SandboxProcessManager.Exec(
		[]string{"docker", "info"},
		"/",
		[]string{},
		false,
	)
	if err != nil {
		return false
	}

	// Wait for docker info to complete with timeout
	deadline := time.Now().Add(dockerInfoCommandTimeout)
	for time.Now().Before(deadline) {
		exitCode, err := instance.SandboxProcessManager.Status(checkPid)
		if err == nil && exitCode == 0 {
			return true
		} else if err == nil && exitCode > 0 {
			return false
		}

		// Still running, wait a bit
		time.Sleep(dockerInfoCommandCheckInterval)
	}

	// Timed out
	return false
}
