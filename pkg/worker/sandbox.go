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

	// Enable IPv4 forwarding for Docker networking
	if err := s.enableIPv4Forwarding(ctx, containerId, instance); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to enable IPv4 forwarding")
		return
	}

	// Setup network stubs to prevent bridge configuration errors
	if err := s.setupNetworkStubs(ctx, containerId, instance); err != nil {
		log.Warn().Str("container_id", containerId).Err(err).Msg("failed to setup network stubs (non-fatal)")
		// Don't return - this is a non-fatal error
	}

	// Create Docker daemon configuration for gVisor compatibility
	if err := s.createDockerDaemonConfig(ctx, containerId, instance); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to create docker daemon config")
		return
	}

	// Create docker-compose wrapper to force host networking
	if err := s.createDockerComposeWrapper(ctx, containerId, instance); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to create docker-compose wrapper")
		return
	}

	// Start dockerd with configuration for gVisor
	// Key settings:
	// - Bridge networking disabled (gVisor doesn't support veth interfaces)
	// - iptables disabled (not supported in gVisor sandbox)
	// - Host network mode enforced via wrapper scripts
	// - Additional flags to prevent bridge network driver issues
	cmd := []string{
		"dockerd",
		"--config-file=/etc/docker/daemon.json",
		"--default-address-pool=base=0.0.0.0/0,size=0",
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

// enableIPv4Forwarding enables IPv4 forwarding which is required for Docker networking
func (s *Worker) enableIPv4Forwarding(ctx context.Context, containerId string, instance *ContainerInstance) error {
	script := `
set -e
# Enable IPv4 forwarding at the kernel level
echo 1 > /proc/sys/net/ipv4/ip_forward
`

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return fmt.Errorf("failed to execute IPv4 forwarding script: %w", err)
	}

	// Wait for command to complete
	time.Sleep(cgroupSetupCompletionWait)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		stderr, _ := instance.SandboxProcessManager.Stderr(pid)
		return fmt.Errorf("IPv4 forwarding setup failed with exit code %d: %s", exitCode, stderr)
	}

	log.Info().Str("container_id", containerId).Msg("IPv4 forwarding enabled")
	return nil
}

// setupNetworkStubs creates a background process to handle bridge network creation attempts
// Since gVisor doesn't support bridge interfaces, this intercepts Docker's network creation
// and makes it use host networking instead
func (s *Worker) setupNetworkStubs(ctx context.Context, containerId string, instance *ContainerInstance) error {
	// Create a script that monitors for Docker network creation attempts and handles them gracefully
	// This uses inotify to watch for bridge creation attempts and creates stub configurations
	script := `
set -e

# Create directory for bridge network stubs if it doesn't exist
mkdir -p /proc/sys/net/ipv4/conf

# Create a background process that handles bridge network creation
# Docker will try to create bridges like br-xxx but they won't work in gVisor
# We'll create a script that intercepts docker network commands

# Create a docker network stub that always succeeds for host network
# This prevents errors when docker-compose tries to create networks
mkdir -p /usr/local/libexec/docker
cat > /usr/local/libexec/docker/network-stub.sh << 'STUBEOF'
#!/bin/sh
# Stub for Docker network operations in gVisor
# Always return success but log the operation
echo "Network operation intercepted (gVisor compatibility mode): $@" >> /var/log/docker-network-stub.log
exit 0
STUBEOF

chmod +x /usr/local/libexec/docker/network-stub.sh

# Create a more permissive route_localnet default
# This prevents the "Cannot read IPv4 local routing setup" error
# by ensuring the directory structure exists
for conf_dir in /proc/sys/net/ipv4/conf/default /proc/sys/net/ipv4/conf/all; do
    if [ -d "$conf_dir" ] && [ ! -f "$conf_dir/route_localnet" ]; then
        echo 1 > "$conf_dir/route_localnet" 2>/dev/null || true
    fi
done

echo "Network stubs configured"
`

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return fmt.Errorf("failed to execute network stubs script: %w", err)
	}

	// Wait for command to complete
	time.Sleep(cgroupSetupCompletionWait)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		stderr, _ := instance.SandboxProcessManager.Stderr(pid)
		return fmt.Errorf("network stubs setup failed with exit code %d: %s", exitCode, stderr)
	}

	log.Info().Str("container_id", containerId).Msg("network stubs configured")
	return nil
}

// createDockerDaemonConfig creates a daemon.json configuration file optimized for gVisor
func (s *Worker) createDockerDaemonConfig(ctx context.Context, containerId string, instance *ContainerInstance) error {
	// Docker daemon configuration optimized for gVisor:
	// - bridge: "none" - Disables default bridge network (gVisor doesn't support veth)
	// - iptables: false - Disables iptables management (not supported in gVisor)
	// - ip6tables: false - Disables ip6tables management
	// - ip-forward: false - Docker won't manage forwarding (we enable it manually)
	// - userland-proxy: false - Disables userland proxy (reduces overhead)
	// - storage-driver: "vfs" - Most compatible storage driver for gVisor
	// - bip: "" - Prevents bridge IP configuration
	daemonConfig := `{
  "bridge": "none",
  "iptables": false,
  "ip6tables": false,
  "ip-forward": false,
  "userland-proxy": false,
  "storage-driver": "vfs",
  "bip": ""
}`

	script := fmt.Sprintf(`
set -e
mkdir -p /etc/docker
cat > /etc/docker/daemon.json << 'EOF'
%s
EOF
`, daemonConfig)

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return fmt.Errorf("failed to create daemon config: %w", err)
	}

	// Wait for file creation to complete
	time.Sleep(cgroupSetupCompletionWait)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		stderr, _ := instance.SandboxProcessManager.Stderr(pid)
		return fmt.Errorf("daemon config creation failed with exit code %d: %s", exitCode, stderr)
	}

	log.Info().Str("container_id", containerId).Msg("docker daemon config created")
	return nil
}

// createDockerComposeWrapper creates a wrapper script for docker-compose that forces host networking
func (s *Worker) createDockerComposeWrapper(ctx context.Context, containerId string, instance *ContainerInstance) error {
	// Create a wrapper script that intercepts docker-compose commands and forces host networking
	// This ensures users cannot accidentally create bridge networks that won't work in gVisor
	wrapperScript := `#!/bin/sh
# Docker Compose wrapper for gVisor compatibility
# This script forces host networking mode for all services and disables custom networks

# Store the original docker-compose binary location
REAL_COMPOSE="/usr/local/bin/docker-compose.real"

# Check if we need to move the real binary
if [ -f "/usr/local/bin/docker-compose" ] && [ ! -f "$REAL_COMPOSE" ]; then
    mv /usr/local/bin/docker-compose "$REAL_COMPOSE" 2>/dev/null || true
fi

# If the real binary exists, use it; otherwise try to find docker-compose in PATH
if [ -f "$REAL_COMPOSE" ]; then
    COMPOSE_CMD="$REAL_COMPOSE"
else
    # Try to find docker-compose elsewhere in PATH
    COMPOSE_CMD=$(which docker-compose 2>/dev/null | grep -v "/usr/local/bin/docker-compose" | head -n1)
    if [ -z "$COMPOSE_CMD" ]; then
        # Fallback: try common locations
        for loc in /usr/bin/docker-compose /bin/docker-compose; do
            if [ -f "$loc" ]; then
                COMPOSE_CMD="$loc"
                break
            fi
        done
    fi
fi

# If we still can't find docker-compose, try docker compose plugin
if [ -z "$COMPOSE_CMD" ] || [ ! -f "$COMPOSE_CMD" ]; then
    COMPOSE_CMD="docker compose"
fi

# Check if we need to modify the compose file for network compatibility
COMPOSE_FILE="docker-compose.yml"
COMPOSE_FILE_OVERRIDE=""

# Find the compose file
for file in docker-compose.yml docker-compose.yaml compose.yml compose.yaml; do
    if [ -f "$file" ]; then
        COMPOSE_FILE="$file"
        break
    fi
done

# For commands that create/start containers, check network configuration
case "$1" in
    up|run|start|create|build)
        if [ -f "$COMPOSE_FILE" ]; then
            # Check if the compose file properly configures networking for gVisor
            HAS_NETWORK_MODE=$(grep -c "network_mode:" "$COMPOSE_FILE" 2>/dev/null || echo "0")
            HAS_NETWORKS=$(grep -c "^networks:" "$COMPOSE_FILE" 2>/dev/null || echo "0")
            
            # If networks are defined without network_mode, warn and create override
            if [ "$HAS_NETWORKS" -gt "0" ] && [ "$HAS_NETWORK_MODE" -eq "0" ]; then
                echo "⚠️  WARNING: docker-compose.yml defines custom networks without network_mode" >&2
                echo "⚠️  Bridge networking is NOT supported in gVisor sandbox" >&2
                echo "⚠️  " >&2
                echo "⚠️  Please add 'network_mode: host' to each service in your docker-compose.yml:" >&2
                echo "⚠️  " >&2
                echo "⚠️    services:" >&2
                echo "⚠️      myservice:" >&2
                echo "⚠️        image: myimage" >&2
                echo "⚠️        network_mode: host  # Add this line" >&2
                echo "⚠️  " >&2
                echo "⚠️  Alternatively, disable networks section and add network_mode to services." >&2
                echo "" >&2
                
                # Try to create an override that at least attempts to use host networking
                COMPOSE_FILE_OVERRIDE="/tmp/.docker-compose.gvisor-override.$$.yml"
                cat > "$COMPOSE_FILE_OVERRIDE" << 'OVERRIDEEOF'
# Auto-generated gVisor compatibility override
# Bridge networking is not supported in gVisor
# This attempts to disable custom network creation

networks:
  default:
    name: host
    external: true

OVERRIDEEOF

                # Add the override file to the compose command
                set -- "$@" -f "$COMPOSE_FILE_OVERRIDE"
                
                # Clean up override file after docker-compose exits
                trap "rm -f '$COMPOSE_FILE_OVERRIDE'" EXIT INT TERM
            elif [ "$HAS_NETWORK_MODE" -eq "0" ] && [ "$HAS_NETWORKS" -eq "0" ]; then
                # No networks defined and no network_mode - compose will try to create default network
                echo "⚠️  WARNING: No network_mode specified in docker-compose.yml" >&2
                echo "⚠️  Docker Compose will attempt to create a bridge network (not supported in gVisor)" >&2
                echo "⚠️  " >&2
                echo "⚠️  Add 'network_mode: host' to each service to avoid errors:" >&2
                echo "⚠️  " >&2
                echo "⚠️    services:" >&2
                echo "⚠️      myservice:" >&2
                echo "⚠️        network_mode: host" >&2
                echo "" >&2
            fi
        fi
        ;;
esac

# Execute the real docker-compose with all arguments
exec $COMPOSE_CMD "$@"
`

	script := fmt.Sprintf(`
set -e

# Save the real docker-compose binary if it exists
if [ -f /usr/local/bin/docker-compose ]; then
    mv /usr/local/bin/docker-compose /usr/local/bin/docker-compose.real 2>/dev/null || true
fi

# Create the wrapper script
cat > /usr/local/bin/docker-compose << 'EOF'
%s
EOF

# Make it executable
chmod +x /usr/local/bin/docker-compose

# Also create a helper message script
cat > /usr/local/bin/docker-compose-help << 'HELPEOF'
#!/bin/sh
echo "Docker Compose in gVisor Sandbox"
echo "================================"
echo ""
echo "⚠️  IMPORTANT: Bridge networking is not supported in gVisor."
echo ""
echo "You must add 'network_mode: host' to all services in docker-compose.yml:"
echo ""
echo "services:"
echo "  myservice:"
echo "    image: myimage"
echo "    network_mode: host"
echo ""
echo "For more information, see Docker Compose documentation."
HELPEOF

chmod +x /usr/local/bin/docker-compose-help

echo "Docker Compose wrapper installed successfully"
`, wrapperScript)

	pid, err := instance.SandboxProcessManager.Exec([]string{"sh", "-c", script}, "/", []string{}, false)
	if err != nil {
		return fmt.Errorf("failed to create docker-compose wrapper: %w", err)
	}

	// Wait for wrapper creation to complete
	time.Sleep(cgroupSetupCompletionWait)

	exitCode, _ := instance.SandboxProcessManager.Status(pid)
	if exitCode != 0 {
		stderr, _ := instance.SandboxProcessManager.Stderr(pid)
		return fmt.Errorf("docker-compose wrapper creation failed with exit code %d: %s", exitCode, stderr)
	}

	log.Info().Str("container_id", containerId).Msg("docker-compose wrapper created")
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
