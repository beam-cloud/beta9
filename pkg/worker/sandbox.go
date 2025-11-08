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

	// Create a docker wrapper that automatically adds --network=host to build commands
	// This ensures all builds (including docker-compose) use host networking for gVisor
	dockerWrapperScript := `#!/bin/sh
# Wrapper for docker command to add --network=host to builds in gVisor
if [ "$1" = "build" ] || [ "$1" = "buildx" ]; then
    # Check if --network is already specified
    has_network=false
    for arg in "$@"; do
        if echo "$arg" | grep -q "^--network"; then
            has_network=true
            break
        fi
    done
    
    # Add --network=host if not already specified
    if [ "$has_network" = "false" ]; then
        /usr/bin/docker.real "$1" --network=host "${@:2}"
    else
        /usr/bin/docker.real "$@"
    fi
else
    /usr/bin/docker.real "$@"
fi
`
	
	// Install the wrapper
	wrapperCmd := []string{"sh", "-c", `
mv /usr/bin/docker /usr/bin/docker.real 2>/dev/null || true
cat > /usr/bin/docker << 'WRAPPER_EOF'
` + dockerWrapperScript + `
WRAPPER_EOF
chmod +x /usr/bin/docker
`}
	if pid, err := instance.SandboxProcessManager.Exec(wrapperCmd, "/", []string{}, false); err == nil {
		time.Sleep(100 * time.Millisecond)
		instance.SandboxProcessManager.Status(pid)
	}

	// Enable BuildKit
	daemonConfig := `{"features": {"buildkit": true}}`
	daemonConfigCmd := []string{"sh", "-c", "mkdir -p /etc/docker && echo '" + daemonConfig + "' > /etc/docker/daemon.json"}
	if pid, err := instance.SandboxProcessManager.Exec(daemonConfigCmd, "/", []string{}, false); err == nil {
		time.Sleep(50 * time.Millisecond)
		instance.SandboxProcessManager.Status(pid)
	}

	// Start dockerd
	cmd := []string{
		"dockerd",
		"--bridge=none",
		"--iptables=false",
		"--ip6tables=false",
		"--ip-forward=false",
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
