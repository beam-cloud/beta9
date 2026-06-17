package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"syscall"
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
	sandboxSetupCommandTimeout     = 10 * time.Second
	dockerDaemonStartupTimeout     = 30 * time.Second
	dockerDaemonReadyPollInterval  = 1 * time.Second
	dockerInfoCommandTimeout       = 2 * time.Second
	dockerInfoCommandCheckInterval = 100 * time.Millisecond
)

type tcpProbeResult struct {
	Connected  bool
	RouteReady bool
	Class      string
	Err        error
	Duration   time.Duration
}

type processManagerWaitStats struct {
	TCPAttempts         int
	TCPFailures         int
	TCPFailureClasses   map[string]int
	FirstTCPReadyAfter  time.Duration
	LastTCPFailureClass string
	LastError           string
}

func (s processManagerWaitStats) attrs() map[string]string {
	attrs := map[string]string{
		types.EventAttrAttempts:     strconv.Itoa(s.TCPAttempts),
		types.EventAttrFailureCount: strconv.Itoa(s.TCPFailures),
	}
	if s.FirstTCPReadyAfter > 0 {
		attrs[types.EventAttrFirstTCPReadyMs] = strconv.FormatInt(s.FirstTCPReadyAfter.Milliseconds(), 10)
	}
	if s.LastTCPFailureClass != "" {
		attrs[types.EventAttrFailureClass] = s.LastTCPFailureClass
	}
	if s.LastError != "" {
		attrs[types.EventAttrLastError] = s.LastError
	}
	if len(s.TCPFailureClasses) > 0 {
		parts := make([]string, 0, len(s.TCPFailureClasses))
		for class, count := range s.TCPFailureClasses {
			parts = append(parts, fmt.Sprintf("%s=%d", class, count))
		}
		sort.Strings(parts)
		attrs[types.EventAttrFailureClasses] = strings.Join(parts, ",")
	}
	return attrs
}

// startDockerDaemon starts the Docker daemon inside a sandbox container
func (s *Worker) startDockerDaemon(ctx context.Context, containerId string, instance *ContainerInstance) {
	if instance.SandboxProcessManager == nil {
		log.Error().Str("container_id", containerId).Msg("sandbox process manager not available")
		return
	}

	log.Info().Str("container_id", containerId).Msg("starting docker daemon in sandbox")

	// Setup cgroups for Docker-in-Docker
	if err := s.setupDockerCgroups(ctx, containerId, instance); err != nil {
		if logDockerStartupCanceled(ctx, containerId, "setup cgroups", err) {
			return
		}
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to setup cgroups")
		return
	}

	// Enable IPv4 forwarding (required for Docker networking)
	if err := s.enableIPv4Forwarding(ctx, containerId, instance); err != nil {
		if logDockerStartupCanceled(ctx, containerId, "enable IPv4 forwarding", err) {
			return
		}
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to enable IPv4 forwarding")
		return
	}

	// Start dockerd with gVisor-compatible flags
	// Per https://gvisor.dev/docs/tutorials/docker-in-gvisor/:
	// --iptables=false --ip6tables=false are REQUIRED for gVisor
	// --bridge=none disables default bridge network (gVisor doesn't support veth interfaces)
	// --storage-driver=vfs avoids nested overlay mounts, which are not supported by gVisor
	// This means inner containers MUST use --network=host
	cmd := []string{
		"dockerd",
		"--iptables=false",
		"--ip6tables=false",
		"--bridge=none",
		"--storage-driver=vfs",
	}

	pid, err := instance.SandboxProcessManager.Exec(cmd, "/", []string{}, true)

	if err != nil {
		if logDockerStartupCanceled(ctx, containerId, "start dockerd", err) {
			return
		}
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to start docker daemon")
		return
	}

	log.Info().Str("container_id", containerId).Int("pid", pid).Msg("docker daemon started")

	// Wait for daemon to be ready
	s.waitForDockerDaemon(ctx, containerId, instance, pid)
}

func logDockerStartupCanceled(ctx context.Context, containerId, phase string, err error) bool {
	if err == nil {
		return false
	}

	if ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		msg := strings.ToLower(err.Error())
		shutdownTransport := strings.Contains(msg, "graceful_stop") || strings.Contains(msg, "received prior goaway")
		if !shutdownTransport && !strings.Contains(msg, "context canceled") && !strings.Contains(msg, "code = canceled") {
			return false
		}
	}

	log.Debug().
		Str("container_id", containerId).
		Str("phase", phase).
		Err(err).
		Msg("docker daemon startup canceled during sandbox shutdown")
	return true
}

// setupDockerCgroups configures cgroups required for Docker-in-Docker in gVisor
func (s *Worker) setupDockerCgroups(ctx context.Context, containerId string, instance *ContainerInstance) error {
	script := `
set -e
mkdir -p /sys/fs/cgroup
if ! grep -q ' /sys/fs/cgroup ' /proc/self/mountinfo; then
  mount -t tmpfs cgroups /sys/fs/cgroup
fi
if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
  exit 0
fi
mkdir -p /sys/fs/cgroup/devices
if ! grep -q ' /sys/fs/cgroup/devices ' /proc/self/mountinfo; then
  mount -t cgroup -o devices devices /sys/fs/cgroup/devices
fi
`

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return err
	}

	return s.waitForSandboxSetupCommand(ctx, instance, pid, "cgroup setup")
}

// enableIPv4Forwarding enables IPv4 forwarding which is required for Docker networking in gVisor sandboxes
func (s *Worker) enableIPv4Forwarding(ctx context.Context, containerId string, instance *ContainerInstance) error {
	script := `echo 1 > /proc/sys/net/ipv4/ip_forward`

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return err
	}

	return s.waitForSandboxSetupCommand(ctx, instance, pid, "IPv4 forwarding")
}

func (s *Worker) waitForSandboxSetupCommand(ctx context.Context, instance *ContainerInstance, pid int, name string) error {
	ticker := time.NewTicker(cgroupSetupCompletionWait)
	defer ticker.Stop()
	timeout := time.NewTimer(sandboxSetupCommandTimeout)
	defer timeout.Stop()

	for {
		exitCode, err := instance.SandboxProcessManager.Status(pid)
		if err == nil && exitCode == 0 {
			return nil
		}
		if err == nil && exitCode > 0 {
			stdout, _ := instance.SandboxProcessManager.Stdout(pid)
			stderr, _ := instance.SandboxProcessManager.Stderr(pid)
			return fmt.Errorf("%s failed with exit code %d: stdout=%q stderr=%q", name, exitCode, stdout, stderr)
		}
		if err != nil {
			return fmt.Errorf("%s status failed: %w", name, err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%s canceled: %w", name, ctx.Err())
		case <-timeout.C:
			stdout, _ := instance.SandboxProcessManager.Stdout(pid)
			stderr, _ := instance.SandboxProcessManager.Stderr(pid)
			return fmt.Errorf("%s did not finish within %s: stdout=%q stderr=%q", name, sandboxSetupCommandTimeout, stdout, stderr)
		case <-ticker.C:
		}
	}
}

// waitForProcessManager waits for the goproc process manager to be ready to accept commands
// Uses exponential backoff to efficiently wait for goproc startup
// This should be called ONCE during container initialization, not on every exec
func (s *Worker) waitForProcessManager(ctx context.Context, containerId string, instance *ContainerInstance) (*goproc.GoProcClient, bool, processManagerWaitStats) {
	start := time.Now()
	backoff := goprocInitialBackoff
	var lastErr error
	stats := processManagerWaitStats{TCPFailureClasses: map[string]int{}}
	tcpReadyRecorded := false

	for time.Since(start) < goprocReadyTimeout {
		select {
		case <-ctx.Done():
			stats.LastError = ctx.Err().Error()
			return nil, false, stats
		default:
		}

		stats.TCPAttempts++
		probe := probeProcessManager(ctx, instance)
		if probe.Connected {
			if !tcpReadyRecorded {
				stats.FirstTCPReadyAfter = time.Since(start)
				s.recordContainerLifecycle(ctx, instance.Request, containerLifecycleFromDuration(
					types.ContainerLifecycleSandboxProcessManagerTCP,
					instance.Request,
					start,
					stats.FirstTCPReadyAfter,
					true,
					stats.attrs(),
				))
				tcpReadyRecorded = true
			}

			client, err := newProcessManagerClient(ctx, instance)
			if err != nil {
				lastErr = err
				stats.LastError = err.Error()
			} else {
				log.Info().
					Str("container_id", containerId).
					Dur("wait_time", time.Since(start)).
					Int("tcp_attempts", stats.TCPAttempts).
					Msg("process manager is ready")
				return client, true, stats
			}
		} else {
			stats.TCPFailures++
			stats.TCPFailureClasses[probe.Class]++
			stats.LastTCPFailureClass = probe.Class
			if probe.Err != nil {
				stats.LastError = probe.Err.Error()
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

	if !tcpReadyRecorded {
		s.recordContainerLifecycle(ctx, instance.Request, containerLifecycleFromDuration(
			types.ContainerLifecycleSandboxProcessManagerTCP,
			instance.Request,
			start,
			time.Since(start),
			false,
			stats.attrs(),
		))
	}

	return nil, false, stats
}

func probeProcessManager(ctx context.Context, instance *ContainerInstance) tcpProbeResult {
	if instance == nil || instance.ContainerIp == "" {
		return tcpProbeResult{Class: "address_unavailable"}
	}

	return probeTCP(ctx, instance.ContainerIp, int(types.WorkerSandboxProcessManagerPort), goprocReadyProbeTimeout)
}

func newProcessManagerClient(ctx context.Context, instance *ContainerInstance) (*goproc.GoProcClient, error) {
	if instance == nil || instance.ContainerIp == "" {
		return nil, fmt.Errorf("sandbox process manager address unavailable")
	}
	return goproc.NewGoProcClient(ctx, instance.ContainerIp, uint(types.WorkerSandboxProcessManagerPort))
}

func probeTCP(ctx context.Context, ip string, port int, timeout time.Duration) tcpProbeResult {
	start := time.Now()
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	address := fmt.Sprintf("%s:%d", ip, port)
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(probeCtx, "tcp", address)
	result := tcpProbeResult{
		Class:    classifyTCPProbeError(err),
		Err:      err,
		Duration: time.Since(start),
	}
	if err == nil {
		_ = conn.Close()
		result.Connected = true
		result.RouteReady = true
		return result
	}
	if result.Class == "connection_refused" {
		result.RouteReady = true
	}
	return result
}

func classifyTCPProbeError(err error) string {
	if err == nil {
		return "connected"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection_refused"
	}
	if errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return "no_route"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "no route") || strings.Contains(msg, "network is unreachable") || strings.Contains(msg, "host is unreachable"):
		return "no_route"
	case strings.Contains(msg, "i/o timeout") || strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	default:
		return "other"
	}
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
