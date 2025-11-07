package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	dockerDaemonStartupTimeout = 30 * time.Second
	dockerDaemonReadyPollInterval = 1 * time.Second
)

// startDockerDaemon starts the Docker daemon inside a gVisor sandbox container
// This function handles:
// 1. Setting up cgroups (required for gVisor Docker-in-Docker)
// 2. Starting dockerd in background mode
// 3. Waiting for daemon to be ready
func (s *Worker) startDockerDaemon(ctx context.Context, containerId string, instance *ContainerInstance) {
	if instance.SandboxProcessManager == nil {
		log.Error().Str("container_id", containerId).Msg("sandbox process manager not available")
		return
	}

	log.Info().Str("container_id", containerId).Msg("starting docker daemon in sandbox")

	// Setup cgroups for Docker-in-Docker (gVisor requirement)
	// Reference: https://gvisor.dev/docs/user_guide/tutorials/docker/
	if err := s.setupDockerCgroups(ctx, containerId, instance); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to setup cgroups")
		return
	}

	// Start dockerd as a background daemon
	cmd := []string{
		"dockerd",
		"--bridge=none",
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
	time.Sleep(500 * time.Millisecond)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		return fmt.Errorf("cgroup setup failed with exit code %d", exitCode)
	}

	return nil
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
			if exitCode, err := instance.SandboxProcessManager.Status(daemonPid); err == nil && exitCode >= 0 {
				stdout, _ := instance.SandboxProcessManager.Stdout(daemonPid)
				stderr, _ := instance.SandboxProcessManager.Stderr(daemonPid)
				log.Error().
					Str("container_id", containerId).
					Int("exit_code", exitCode).
					Str("stdout", stdout).
					Str("stderr", stderr).
					Msg("docker daemon crashed")
				return
			}

			// Check if daemon is ready with docker info
			checkPid, err := instance.SandboxProcessManager.Exec(
				[]string{"docker", "info"},
				"/",
				[]string{},
				false,
			)
			if err != nil {
				continue
			}

			// Wait up to 2 seconds for docker info to complete
			checkTimeout := time.After(2 * time.Second)
			checkTicker := time.NewTicker(100 * time.Millisecond)
			defer checkTicker.Stop()

			for {
				select {
				case <-checkTimeout:
					// docker info timed out, daemon not ready
					goto nextAttempt

				case <-checkTicker.C:
					exitCode, err := instance.SandboxProcessManager.Status(checkPid)
					if err == nil && exitCode == 0 {
						log.Info().Str("container_id", containerId).Msg("docker daemon is ready")
						return
					} else if err == nil && exitCode > 0 {
						// docker info failed, daemon not ready
						goto nextAttempt
					}
					// exitCode < 0 means still running, keep waiting
				}
			}

		nextAttempt:
			// Continue to next ticker iteration
		}
	}
}
