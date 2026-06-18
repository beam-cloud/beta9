package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	goproc "github.com/beam-cloud/goproc/pkg"
	goprocpb "github.com/beam-cloud/goproc/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Timing strategy for Docker daemon startup:
	// 1. Wait up to 30s for goproc to be ready (usually takes 100-500ms)
	// 2. Setup cgroups (fast, ~100ms)
	// 3. Start dockerd in background
	// 4. Wait up to 30s for dockerd to be ready (usually takes 2-5s)
	goprocReadyTimeout            = 30 * time.Second
	goprocReadyProbeTimeout       = 50 * time.Millisecond
	goprocInitialBackoff          = 10 * time.Millisecond
	goprocMaxBackoff              = 50 * time.Millisecond
	goprocBackoffMultiplier       = 1.5
	cgroupSetupCompletionWait     = 500 * time.Millisecond
	sandboxSetupCommandTimeout    = 10 * time.Second
	dockerDaemonStartupTimeout    = 30 * time.Second
	dockerDaemonReadyPollInterval = 1 * time.Second
	dockerInfoCommandTimeout      = 2 * time.Second
	sandboxMissingProcessExitCode = 137
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
	ReadyAttempts       int
	ReadyFailures       int
	ReadyFailureClasses map[string]int
	FirstTCPReadyAfter  time.Duration
	LastTCPFailureClass string
	LastReadyClass      string
	LastError           string
}

func (s processManagerWaitStats) attrs() map[string]string {
	attrs := map[string]string{
		types.EventAttrAttempts:     strconv.Itoa(s.ReadyAttempts),
		types.EventAttrFailureCount: strconv.Itoa(s.ReadyFailures),
	}
	if s.FirstTCPReadyAfter > 0 {
		attrs[types.EventAttrFirstTCPReadyMs] = strconv.FormatInt(s.FirstTCPReadyAfter.Milliseconds(), 10)
	}
	if s.LastReadyClass != "" {
		attrs[types.EventAttrFailureClass] = s.LastReadyClass
	}
	if s.LastError != "" {
		attrs[types.EventAttrLastError] = s.LastError
	}
	if len(s.ReadyFailureClasses) > 0 {
		attrs[types.EventAttrFailureClasses] = failureClassSummary(s.ReadyFailureClasses)
	}
	return attrs
}

func (s processManagerWaitStats) tcpAttrs() map[string]string {
	attrs := map[string]string{
		types.EventAttrAttempts:     strconv.Itoa(s.TCPAttempts),
		types.EventAttrFailureCount: strconv.Itoa(s.TCPFailures),
	}
	if s.LastTCPFailureClass != "" {
		attrs[types.EventAttrFailureClass] = s.LastTCPFailureClass
	}
	if s.LastError != "" {
		attrs[types.EventAttrLastError] = s.LastError
	}
	if len(s.TCPFailureClasses) > 0 {
		attrs[types.EventAttrFailureClasses] = failureClassSummary(s.TCPFailureClasses)
	}
	return attrs
}

func failureClassSummary(classes map[string]int) string {
	if len(classes) == 0 {
		return ""
	}

	parts := make([]string, 0, len(classes))
	for class, count := range classes {
		parts = append(parts, fmt.Sprintf("%s=%d", class, count))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
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
		if s.logDockerStartupCanceled(ctx, containerId, "setup cgroups", err) {
			return
		}
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to setup cgroups")
		return
	}

	// Enable IPv4 forwarding (required for Docker networking)
	if err := s.enableIPv4Forwarding(ctx, containerId, instance); err != nil {
		if s.logDockerStartupCanceled(ctx, containerId, "enable IPv4 forwarding", err) {
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

	// dockerd runs in the foreground; waiting here would block readiness checks.
	pid, err := instance.SandboxProcessManager.Exec(cmd, "/", []string{}, false)

	if err != nil {
		if s.logDockerStartupCanceled(ctx, containerId, "start dockerd", err) {
			return
		}
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to start docker daemon")
		return
	}

	log.Info().Str("container_id", containerId).Int("pid", pid).Msg("docker daemon started")

	// Wait for daemon to be ready
	s.waitForDockerDaemon(ctx, containerId, instance, pid)
}

func (s *Worker) stopDockerSandbox(containerId string, instance *ContainerInstance, force bool) {
	if force || instance == nil || instance.Request == nil || !instance.Request.DockerEnabled {
		return
	}
	if instance.SandboxProcessManager == nil || !instance.SandboxProcessManagerReady {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), sandboxSetupCommandTimeout)
	defer cancel()

	if err := runSandboxProcessManagerCommand(ctx, instance.SandboxProcessManager, []string{"sh", "-c", dockerSandboxShutdownScript()}, "/", []string{}, "docker sandbox pre-stop"); err != nil {
		if s.logDockerStartupCanceled(ctx, containerId, "stop docker", err) {
			return
		}
		log.Debug().Str("container_id", containerId).Err(err).Msg("docker sandbox pre-stop did not complete cleanly")
		return
	}

	log.Info().Str("container_id", containerId).Msg("docker sandbox pre-stop complete")
}

func runSandboxProcessManagerCommand(ctx context.Context, manager *goproc.GoProcClient, args []string, cwd string, env []string, name string) error {
	stream, err := manager.StreamExec(ctx, args, cwd, env, true)
	if err != nil {
		return fmt.Errorf("%s start failed: %w", name, err)
	}

	var stdout, stderr strings.Builder

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%s stream closed before process exit", name)
		}
		if err != nil {
			return fmt.Errorf("%s stream failed: %w", name, err)
		}

		if resp.GetStarted() != nil {
			continue
		}

		if chunk := resp.GetChunk(); chunk != nil {
			if chunk.Stream == "stderr" {
				stderr.Write(chunk.Data)
			} else {
				stdout.Write(chunk.Data)
			}
			if err := stream.Send(&goprocpb.StreamExecRequest{
				Message: &goprocpb.StreamExecRequest_Ack{
					Ack: &goprocpb.ProcessLogAck{Seq: chunk.Seq, Ok: true},
				},
			}); err != nil {
				return fmt.Errorf("%s log ack failed: %w", name, err)
			}
			continue
		}

		if exited := resp.GetExited(); exited != nil {
			if exited.ExitCode == 0 {
				return nil
			}
			if exited.ErrorMsg != "" {
				return fmt.Errorf("%s failed with exit code %d: %s stdout=%q stderr=%q", name, exited.ExitCode, exited.ErrorMsg, stdout.String(), stderr.String())
			}
			return fmt.Errorf("%s failed with exit code %d: stdout=%q stderr=%q", name, exited.ExitCode, stdout.String(), stderr.String())
		}
	}
}

func sandboxProcessMissing(manager *goproc.GoProcClient, pid int32) (bool, error) {
	checkPID, err := manager.Exec([]string{
		"sh",
		"-c",
		fmt.Sprintf("if [ -d /proc/%d ]; then echo alive; else echo missing; fi", pid),
	}, "/", []string{}, true)
	if err != nil {
		return false, err
	}

	output, err := manager.Stdout(checkPID)
	if err != nil {
		return false, err
	}

	return strings.TrimSpace(output) == "missing", nil
}

func waitForSandboxProcessMissing(ctx context.Context, manager *goproc.GoProcClient, pid int32, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		missing, err := sandboxProcessMissing(manager, pid)
		if err != nil {
			return false, err
		}
		if missing {
			return true, nil
		}

		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
		}
	}
}

func dockerSandboxShutdownScript() string {
	return `
set +e
run() {
  if command -v timeout >/dev/null 2>&1; then
    timeout 3s "$@"
  else
    "$@"
  fi
}
if command -v docker >/dev/null 2>&1; then
  ids="$(run docker ps -q 2>/dev/null || true)"
  if [ -n "$ids" ]; then
    run docker kill $ids >/dev/null 2>&1 || true
  fi
  ids="$(run docker ps -aq 2>/dev/null || true)"
  if [ -n "$ids" ]; then
    run docker rm -f $ids >/dev/null 2>&1 || true
  fi
fi
if command -v pkill >/dev/null 2>&1; then
  pkill -TERM dockerd >/dev/null 2>&1 || true
  pkill -TERM containerd-shim >/dev/null 2>&1 || true
  pkill -TERM containerd >/dev/null 2>&1 || true
  for _ in 1 2 3 4 5; do
    pgrep dockerd >/dev/null 2>&1 || pgrep containerd >/dev/null 2>&1 || pgrep containerd-shim >/dev/null 2>&1 || exit 0
    sleep 0.2
  done
  pkill -KILL dockerd >/dev/null 2>&1 || true
  pkill -KILL containerd-shim >/dev/null 2>&1 || true
  pkill -KILL containerd >/dev/null 2>&1 || true
fi
exit 0
`
}

func (s *Worker) logDockerStartupCanceled(ctx context.Context, containerId, phase string, err error) bool {
	if !s.dockerStartupCanceled(ctx, containerId, err) {
		return false
	}

	log.Debug().
		Str("container_id", containerId).
		Str("phase", phase).
		Err(err).
		Msg("docker daemon startup canceled during sandbox shutdown")
	return true
}

func (s *Worker) dockerStartupCanceled(ctx context.Context, containerId string, err error) bool {
	if dockerStartupCanceled(ctx, err) {
		return true
	}
	if err == nil || s == nil || s.containerInstances == nil {
		return false
	}

	instance, exists := s.containerInstances.Get(containerId)
	if !exists {
		return true
	}
	return instance != nil && instance.StopReason != ""
}

func dockerStartupCanceled(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}

	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}

	msg := strings.ToLower(err.Error())
	shutdownTransport := strings.Contains(msg, "graceful_stop") ||
		strings.Contains(msg, "received prior goaway") ||
		strings.Contains(msg, "transport is closing")

	return shutdownTransport ||
		strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "code = canceled") ||
		strings.Contains(msg, "code = deadlineexceeded") ||
		strings.Contains(msg, "code = deadline exceeded")
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
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s canceled: %w", name, ctx.Err())
		default:
		}

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
	stats := processManagerWaitStats{
		TCPFailureClasses:   map[string]int{},
		ReadyFailureClasses: map[string]int{},
	}
	tcpReadyRecorded := false
	restartNudged := false

	for time.Since(start) < goprocReadyTimeout {
		select {
		case <-ctx.Done():
			stats.LastError = ctx.Err().Error()
			return nil, false, stats
		default:
		}

		stats.TCPAttempts++
		tcp := probeProcessManager(ctx, instance)
		if tcp.Connected {
			if !tcpReadyRecorded {
				stats.FirstTCPReadyAfter = time.Since(start)
				s.recordContainerLifecycle(ctx, instance.Request, containerLifecycleFromDuration(
					types.ContainerLifecycleSandboxProcessManagerTCP,
					instance.Request,
					start,
					stats.FirstTCPReadyAfter,
					true,
					stats.tcpAttrs(),
				))
				tcpReadyRecorded = true
			}
		} else {
			lastErr = tcp.Err
			stats.TCPFailures++
			stats.LastTCPFailureClass = tcp.Class
			stats.TCPFailureClasses[tcp.Class]++
			if tcp.Err != nil {
				stats.LastError = tcp.Err.Error()
			}

			if shouldNudgeRestoredProcessManager(instance, restartNudged) {
				s.nudgeSandboxProcessManager(ctx, containerId, instance.Runtime)
				restartNudged = true
			}

			if err := waitProcessManagerBackoff(ctx, backoff); err != nil {
				stats.LastError = ctx.Err().Error()
				return nil, false, stats
			}

			backoff = nextProcessManagerBackoff(backoff)
			continue
		}

		stats.ReadyAttempts++
		client, err := readyProcessManagerClient(ctx, instance)
		if err == nil {
			log.Info().
				Str("container_id", containerId).
				Dur("wait_time", time.Since(start)).
				Int("tcp_attempts", stats.TCPAttempts).
				Int("ready_attempts", stats.ReadyAttempts).
				Msg("process manager is ready")
			return client, true, stats
		}

		lastErr = err
		stats.ReadyFailures++
		stats.LastError = err.Error()
		stats.LastReadyClass = classifyProcessManagerReadyError(err)
		stats.ReadyFailureClasses[stats.LastReadyClass]++

		if shouldNudgeRestoredProcessManager(instance, restartNudged) {
			s.nudgeSandboxProcessManager(ctx, containerId, instance.Runtime)
			restartNudged = true
		}

		if err := waitProcessManagerBackoff(ctx, backoff); err != nil {
			stats.LastError = ctx.Err().Error()
			return nil, false, stats
		}

		backoff = nextProcessManagerBackoff(backoff)
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
			stats.tcpAttrs(),
		))
	}

	return nil, false, stats
}

func shouldNudgeRestoredProcessManager(instance *ContainerInstance, nudged bool) bool {
	return !nudged &&
		instance != nil &&
		instance.Runtime != nil &&
		instance.Request != nil &&
		instance.Request.Checkpoint != nil &&
		instance.Request.Stub.Type.Kind() == types.StubTypeSandbox
}

func nextProcessManagerBackoff(delay time.Duration) time.Duration {
	delay = time.Duration(float64(delay) * goprocBackoffMultiplier)
	if delay > goprocMaxBackoff {
		return goprocMaxBackoff
	}
	return delay
}

func probeProcessManager(ctx context.Context, instance *ContainerInstance) tcpProbeResult {
	endpoints := sandboxProcessManagerEndpoints(instance)
	if len(endpoints) == 0 {
		return tcpProbeResult{Class: "address_unavailable"}
	}

	var last tcpProbeResult
	for _, endpoint := range endpoints {
		result := probeTCP(ctx, endpoint.host, endpoint.port, goprocReadyProbeTimeout)
		if result.Connected {
			return result
		}
		last = result
	}
	return last
}

func newProcessManagerClient(ctx context.Context, instance *ContainerInstance) (*goproc.GoProcClient, error) {
	endpoints := sandboxProcessManagerEndpoints(instance)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("sandbox process manager address unavailable")
	}

	var lastErr error
	for _, endpoint := range endpoints {
		client, err := goproc.NewGoProcClient(ctx, endpoint.host, uint(endpoint.port))
		if err != nil {
			lastErr = err
			continue
		}

		probeCtx, cancel := context.WithTimeout(ctx, goprocReadyProbeTimeout)
		err = client.ReadyContext(probeCtx)
		cancel()
		if err == nil {
			return client, nil
		}

		_ = client.Cleanup()
		lastErr = err
	}
	return nil, lastErr
}

type processManagerEndpoint struct {
	host string
	port int
}

func sandboxProcessManagerEndpoint(instance *ContainerInstance) (processManagerEndpoint, bool) {
	endpoints := sandboxProcessManagerEndpoints(instance)
	if len(endpoints) == 0 {
		return processManagerEndpoint{}, false
	}
	return endpoints[0], true
}

func sandboxProcessManagerEndpoints(instance *ContainerInstance) []processManagerEndpoint {
	if instance == nil {
		return nil
	}

	endpoints := make([]processManagerEndpoint, 0, 2)
	if instance.ProcessManagerHost != "" && instance.ProcessManagerPort > 0 {
		endpoints = append(endpoints, processManagerEndpoint{host: instance.ProcessManagerHost, port: instance.ProcessManagerPort})
	}

	if instance.ContainerIp != "" {
		endpoints = append(endpoints, processManagerEndpoint{host: instance.ContainerIp, port: int(types.WorkerSandboxProcessManagerPort)})
	}

	return endpoints
}

func readyProcessManagerClient(ctx context.Context, instance *ContainerInstance) (*goproc.GoProcClient, error) {
	return newProcessManagerClient(ctx, instance)
}

func classifyProcessManagerReadyError(err error) string {
	switch code := status.Code(err); code {
	case codes.OK:
		return "ok"
	case codes.Unavailable:
		return "unavailable"
	case codes.DeadlineExceeded:
		return "timeout"
	case codes.Unimplemented:
		return "unimplemented"
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "no route") || strings.Contains(msg, "network is unreachable") || strings.Contains(msg, "host is unreachable"):
		return "no_route"
	case strings.Contains(msg, "deadline exceeded") || strings.Contains(msg, "i/o timeout"):
		return "timeout"
	default:
		return "other"
	}
}

func waitProcessManagerBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
			if err := s.probeDockerDaemon(ctx, instance); err == nil {
				log.Info().Str("container_id", containerId).Msg("docker daemon is ready")
				return
			} else {
				log.Debug().Str("container_id", containerId).Err(err).Msg("docker daemon readiness check failed")
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

// probeDockerDaemon checks if Docker responds to commands inside the sandbox.
func (s *Worker) probeDockerDaemon(ctx context.Context, instance *ContainerInstance) error {
	infoCtx, cancel := context.WithTimeout(ctx, dockerInfoCommandTimeout)
	defer cancel()

	return runSandboxProcessManagerCommand(infoCtx, instance.SandboxProcessManager, []string{"docker", "info"}, "/", []string{}, "docker info")
}
