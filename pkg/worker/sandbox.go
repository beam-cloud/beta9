package worker

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	goproc "github.com/beam-cloud/goproc/pkg"
	"github.com/rs/zerolog/log"
)

const (
	// Timing strategy for Docker daemon startup:
	// 1. Wait up to 30s for goproc to be ready (usually takes 100-500ms)
	// 2. Setup cgroups (fast, ~100ms)
	// 3. Start dockerd in background
	// 4. Wait up to 30s for dockerd to be ready (usually takes 2-5s)
	goprocReadyTimeout             = 30 * time.Second
	goprocReadyProbeTimeout        = 50 * time.Millisecond
	goprocInitialBackoff           = 10 * time.Millisecond
	goprocMaxBackoff               = 50 * time.Millisecond
	goprocBackoffMultiplier        = 1.5
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

	// Enable IPv4 forwarding (required for Docker networking)
	if err := s.enableIPv4Forwarding(ctx, containerId, instance); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to enable IPv4 forwarding")
		return
	}

	// Start dockerd with gVisor-compatible flags
	// Per https://gvisor.dev/docs/tutorials/docker-in-gvisor/:
	// --iptables=false --ip6tables=false are REQUIRED for gVisor
	// --bridge=none disables default bridge network (gVisor doesn't support veth interfaces)
	// This means inner containers MUST use --network=host
	cmd := []string{
		"dockerd",
		"--iptables=false",
		"--ip6tables=false",
		"--bridge=none",
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

// enableIPv4Forwarding enables IPv4 forwarding which is required for Docker networking in gVisor sandboxes
func (s *Worker) enableIPv4Forwarding(ctx context.Context, containerId string, instance *ContainerInstance) error {
	script := `echo 1 > /proc/sys/net/ipv4/ip_forward`

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return err
	}

	time.Sleep(cgroupSetupCompletionWait)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		stderr, _ := instance.SandboxProcessManager.Stderr(pid)
		return fmt.Errorf("IPv4 forwarding failed with exit code %d: %s", exitCode, stderr)
	}

	return nil
}

// waitForProcessManager waits for the goproc process manager to be ready to accept commands
// Uses exponential backoff to efficiently wait for goproc startup
// This should be called ONCE during container initialization, not on every exec
func (s *Worker) waitForProcessManager(ctx context.Context, containerId string, instance *ContainerInstance) (*goproc.GoProcClient, bool) {
	start := time.Now()
	backoff := goprocInitialBackoff
	var lastErr error

	for time.Since(start) < goprocReadyTimeout {
		select {
		case <-ctx.Done():
			return nil, false
		default:
		}

		if probeProcessManager(ctx, instance) {
			client, err := newProcessManagerClient(ctx, instance)
			if err != nil {
				lastErr = err
			} else {
				log.Info().
					Str("container_id", containerId).
					Dur("wait_time", time.Since(start)).
					Msg("process manager is ready")
				return client, true
			}
		}

		// Not ready yet - wait with exponential backoff
		time.Sleep(backoff)

		backoff = time.Duration(float64(backoff) * goprocBackoffMultiplier)
		if backoff > goprocMaxBackoff {
			backoff = goprocMaxBackoff
		}
	}

	logEvent := log.Error().Str("container_id", containerId)
	if lastErr != nil {
		logEvent = logEvent.Err(lastErr)
	}
	logEvent.Msg("process manager did not become ready within timeout")

	return nil, false
}

func probeProcessManager(ctx context.Context, instance *ContainerInstance) bool {
	probeCtx, cancel := context.WithTimeout(ctx, goprocReadyProbeTimeout)
	defer cancel()

	if instance == nil || instance.ContainerIp == "" {
		return false
	}

	address := fmt.Sprintf("%s:%d", instance.ContainerIp, types.WorkerSandboxProcessManagerPort)
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(probeCtx, "tcp", address)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func newProcessManagerClient(ctx context.Context, instance *ContainerInstance) (*goproc.GoProcClient, error) {
	if instance == nil || instance.ContainerIp == "" {
		return nil, fmt.Errorf("sandbox process manager address unavailable")
	}
	return goproc.NewGoProcClient(ctx, instance.ContainerIp, uint(types.WorkerSandboxProcessManagerPort))
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
