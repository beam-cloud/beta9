package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

// Runsc implements Runtime using the gVisor runsc runtime
//
// CUDA Checkpoint/Restore:
// For GPU workloads, cuda-checkpoint is bind-mounted from the host and executed
// inside the container via runsc exec to freeze/unfreeze GPU state before/after
// checkpoint/restore operations.
type Runsc struct {
	cfg            Config
	nvproxyEnabled bool
}

// NewRunsc creates a new runsc (gVisor) runtime
func NewRunsc(cfg Config) (*Runsc, error) {
	if cfg.RunscPath == "" {
		cfg.RunscPath = "runsc"
	}
	if cfg.RunscRoot == "" {
		cfg.RunscRoot = "/run/gvisor"
	}

	// Check if runsc is available
	if _, err := exec.LookPath(cfg.RunscPath); err != nil {
		return nil, ErrRuntimeNotAvailable{
			Runtime: types.ContainerRuntimeGvisor.String(),
			Reason:  "runsc binary not found in PATH",
		}
	}

	return &Runsc{
		cfg: cfg,
	}, nil
}

func (r *Runsc) Name() string {
	return types.ContainerRuntimeGvisor.String()
}

func (r *Runsc) Capabilities() Capabilities {
	return Capabilities{
		CheckpointRestore: true,
		GPU:               true,
		OOMEvents:         false,
		JoinExistingNetNS: true,
		CDI:               false, // gVisor's nvproxy doesn't use CDI - it's a proxy driver
	}
}

// Prepare mutates the OCI spec to be compatible with gVisor
func (r *Runsc) Prepare(ctx context.Context, spec *specs.Spec) error {
	if spec == nil || spec.Linux == nil {
		return fmt.Errorf("spec is nil")
	}

	// gVisor requires seccomp to be disabled
	spec.Linux.Seccomp = nil

	// Detect if GPU is requested by checking devices or CDI annotations
	r.nvproxyEnabled = r.hasGPUDevices(spec)

	if r.nvproxyEnabled {
		// Mount cuda-checkpoint tool for CUDA checkpoint/restore support
		r.mountCudaCheckpoint(spec)

		// Log devices before filtering
		var devicesBefore []string
		for _, dev := range spec.Linux.Devices {
			devicesBefore = append(devicesBefore, dev.Path)
		}

		// CRITICAL: Filter to only gVisor-supported GPU devices
		// Per gVisor docs: "gVisor only exposes /dev/nvidiactl, /dev/nvidia-uvm and /dev/nvidia#"
		// CDI injects unsupported devices like /dev/nvidia-modeset, /dev/dri/* which cause startup failures
		r.filterToSupportedGPUDevices(spec)

		// Log devices after filtering
		var devicesAfter []string
		for _, dev := range spec.Linux.Devices {
			devicesAfter = append(devicesAfter, dev.Path)
		}

		log.Info().
			Strs("devices_before_filter", devicesBefore).
			Strs("devices_after_filter", devicesAfter).
			Int("removed_count", len(devicesBefore)-len(devicesAfter)).
			Msg("gVisor device filtering complete")

		// CRITICAL: Filter mounts to remove paths that don't exist
		// CDI may inject mounts for libraries that aren't present (e.g., ngx libraries)
		// Attempting to mount non-existent files causes StartRoot to fail with EOF
		log.Info().Int("total_mounts_before_filter", len(spec.Mounts)).Msg("About to filter mounts")
		r.filterNonExistentMounts(spec)
		log.Info().Int("total_mounts_after_filter", len(spec.Mounts)).Msg("Mount filtering complete")

		// CRITICAL: Filter env vars to remove unsupported driver capabilities
		// CDI injects NVIDIA_DRIVER_CAPABILITIES with all capabilities including ngx
		// gVisor's nvproxy doesn't support ngx, so we need to remove it
		r.filterUnsupportedDriverCapabilities(spec)
	} else {
		// For non-GPU workloads, clear all devices as gVisor handles them internally
		spec.Linux.Devices = nil
	}

	return nil
}

// filterToSupportedGPUDevices keeps only GPU devices that gVisor's nvproxy supports
// Per gVisor documentation, only these devices are supported:
// - /dev/nvidiactl
// - /dev/nvidia-uvm
// - /dev/nvidia# (where # is a GPU number like 0, 1, 2, etc.)
// All other devices (like /dev/nvidia-modeset, /dev/dri/*, etc.) cause startup failures
func (r *Runsc) filterToSupportedGPUDevices(spec *specs.Spec) {
	if spec.Linux == nil {
		return
	}

	var supportedDevices []specs.LinuxDevice
	for _, device := range spec.Linux.Devices {
		// Only keep devices explicitly supported by gVisor nvproxy
		if device.Path == "/dev/nvidiactl" ||
			device.Path == "/dev/nvidia-uvm" ||
			strings.HasPrefix(device.Path, "/dev/nvidia") && len(device.Path) > len("/dev/nvidia") && device.Path[len("/dev/nvidia")] >= '0' && device.Path[len("/dev/nvidia")] <= '9' {
			supportedDevices = append(supportedDevices, device)
		}
	}

	spec.Linux.Devices = supportedDevices
}

// mountCudaCheckpoint bind-mounts cuda-checkpoint binary into the container
func (r *Runsc) mountCudaCheckpoint(spec *specs.Spec) {
	cudaCheckpointPath, err := exec.LookPath("cuda-checkpoint")
	if err != nil {
		return // Not found, skip
	}

	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: "/usr/local/bin/cuda-checkpoint",
		Type:        "bind",
		Source:      cudaCheckpointPath,
		Options:     []string{"bind", "ro"},
	})
}

// filterNonExistentMounts removes mounts where the source path doesn't exist on the host
// This is critical for gVisor as mounting non-existent files causes StartRoot to fail with EOF
func (r *Runsc) filterNonExistentMounts(spec *specs.Spec) {
	if spec.Mounts == nil {
		log.Warn().Msg("spec.Mounts is nil!")
		return
	}

	var validMounts []specs.Mount
	var removedMounts []string
	var skippedTypes []string
	bindMountCount := 0
	checkCount := 0

	// Log first 3 mounts to see what we're dealing with
	for i, mount := range spec.Mounts {
		if i < 3 {
			log.Info().
				Str("source", mount.Source).
				Str("dest", mount.Destination).
				Str("type", mount.Type).
				Strs("options", mount.Options).
				Msgf("Sample mount %d", i)
		}
	}

	for _, mount := range spec.Mounts {
		// Skip non-bind mounts (they don't require source path to exist)
		// Note: CDI often uses empty Type which defaults to "bind"
		if mount.Type != "" && mount.Type != "bind" && mount.Type != "rbind" {
			validMounts = append(validMounts, mount)
			skippedTypes = append(skippedTypes, mount.Type)
			continue
		}

		bindMountCount++
		checkCount++
		
		// Check if source exists
		if stat, err := os.Stat(mount.Source); err == nil {
			validMounts = append(validMounts, mount)
			if checkCount <= 3 {
				log.Info().
					Str("source", mount.Source).
					Bool("is_dir", stat.IsDir()).
					Msg("Mount source EXISTS")
			}
		} else {
			log.Warn().
				Str("mount_source", mount.Source).
				Str("mount_dest", mount.Destination).
				Str("mount_type", mount.Type).
				Str("error", err.Error()).
				Msg("REMOVING non-existent mount")
			removedMounts = append(removedMounts, mount.Source)
		}
	}

	log.Info().
		Int("total_mounts", len(spec.Mounts)).
		Int("bind_mount_count", bindMountCount).
		Int("checked_count", checkCount).
		Strs("skipped_types", skippedTypes).
		Int("removed_count", len(removedMounts)).
		Msg("Mount filtering stats")

	if len(removedMounts) > 0 {
		log.Warn().
			Strs("removed_mounts", removedMounts).
			Int("removed_count", len(removedMounts)).
			Int("total_bind_mounts", bindMountCount).
			Msg("Filtered out non-existent mount paths for gVisor")
	} else {
		log.Warn().Msg("NO MOUNTS WERE REMOVED - all mount sources exist or were skipped!")
	}

	spec.Mounts = validMounts
}

// filterUnsupportedDriverCapabilities removes unsupported capabilities from NVIDIA_DRIVER_CAPABILITIES
// gVisor's nvproxy only supports: compute, utility, graphics, video (not ngx)
func (r *Runsc) filterUnsupportedDriverCapabilities(spec *specs.Spec) {
	if spec.Process == nil || spec.Process.Env == nil {
		return
	}

	supportedCaps := []string{"compute", "utility", "graphics", "video"}
	
	for i, env := range spec.Process.Env {
		if strings.HasPrefix(env, "NVIDIA_DRIVER_CAPABILITIES=") {
			value := strings.TrimPrefix(env, "NVIDIA_DRIVER_CAPABILITIES=")
			caps := strings.Split(value, ",")
			
			// Filter to only supported capabilities
			var filtered []string
			var removed []string
			for _, cap := range caps {
				cap = strings.TrimSpace(cap)
				if cap == "" {
					continue
				}
				
				supported := false
				for _, supportedCap := range supportedCaps {
					if cap == supportedCap {
						supported = true
						break
					}
				}
				
				if supported {
					filtered = append(filtered, cap)
				} else {
					removed = append(removed, cap)
				}
			}
			
			if len(removed) > 0 {
				newValue := strings.Join(filtered, ",")
				spec.Process.Env[i] = "NVIDIA_DRIVER_CAPABILITIES=" + newValue
				log.Warn().
					Strs("removed_capabilities", removed).
					Str("old_value", value).
					Str("new_value", newValue).
					Msg("Filtered unsupported NVIDIA driver capabilities for gVisor")
			}
			
			break // Only one NVIDIA_DRIVER_CAPABILITIES env var
		}
	}
}

// hasGPUDevices checks if the spec contains GPU device configurations
// For gVisor, we don't inject device nodes, so check env vars instead
func (r *Runsc) hasGPUDevices(spec *specs.Spec) bool {
	// Check environment variables for NVIDIA_VISIBLE_DEVICES
	// This is how we signal GPU requirements for gVisor (no device nodes needed)
	if spec.Process != nil && spec.Process.Env != nil {
		for _, env := range spec.Process.Env {
			if strings.HasPrefix(env, "NVIDIA_VISIBLE_DEVICES=") {
				value := strings.TrimPrefix(env, "NVIDIA_VISIBLE_DEVICES=")
				// Check if it's not empty, "void", or "none"
				if value != "" && value != "void" && value != "none" {
					return true
				}
			}
		}
	}

	// Fallback: check for device nodes (for CDI/runc compatibility)
	if spec.Linux != nil {
		for _, device := range spec.Linux.Devices {
			if strings.HasPrefix(device.Path, "/dev/nvidia") {
				return true
			}
		}
	}

	// Check CDI annotations
	for key := range spec.Annotations {
		if strings.HasPrefix(key, "cdi.k8s.io") {
			return true
		}
	}

	return false
}

func (r *Runsc) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
	dockerEnabled := opts != nil && opts.DockerEnabled

	defer func() {
		deleteArgs := r.baseArgs(dockerEnabled)
		deleteArgs = append(deleteArgs, "delete", "--force", containerID)
		_ = exec.Command(r.cfg.RunscPath, deleteArgs...).Run()
	}()

	args := r.baseArgs(dockerEnabled)
	if r.nvproxyEnabled {
		args = append(args, "--nvproxy=true")
		// Allow all driver capabilities that the container might need  
		// Per gVisor docs, supported capabilities are: compute, utility, graphics, video
		// Note: ngx is NOT supported by gVisor and will cause a fatal error if included
		args = append(args, "--nvproxy-allowed-driver-capabilities=compute,utility,graphics,video")
	}
	args = append(args, "run", "--bundle", bundlePath, containerID)

	// Log the full runsc command for debugging
	log.Info().
		Str("container_id", containerID).
		Str("command", r.cfg.RunscPath).
		Strs("args", args).
		Bool("nvproxy_enabled", r.nvproxyEnabled).
		Msg("Executing runsc command")

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Kill entire process tree

	// Capture stderr to log runsc errors
	var stderrBuf bytes.Buffer
	if opts != nil && opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = io.MultiWriter(opts.OutputWriter, &stderrBuf)
	} else {
		cmd.Stderr = &stderrBuf
	}

	if err := cmd.Start(); err != nil {
		return -1, err
	}

	if opts != nil && opts.Started != nil {
		opts.Started <- cmd.Process.Pid
	}

	// Handle cancellation by killing process group
	go func() {
		<-ctx.Done()
		if pgid, _ := syscall.Getpgid(cmd.Process.Pid); pgid > 0 {
			syscall.Kill(-pgid, syscall.SIGKILL)
		}
	}()

	// Wait for exit
	err := cmd.Wait()
	if err != nil {
		// Log stderr output from runsc for debugging
		stderrStr := stderrBuf.String()
		if stderrStr != "" {
			log.Error().
				Str("container_id", containerID).
				Str("runsc_stderr", stderrStr).
				Msg("runsc command failed with stderr output")
		}

		// Try to read debug log if available
		if r.cfg.Debug {
			debugLogPath := filepath.Join(r.cfg.RunscRoot, "debug.log")
			if debugContent, readErr := os.ReadFile(debugLogPath); readErr == nil {
				// Log last 2000 chars to avoid overwhelming logs
				maxLen := 2000
				content := string(debugContent)
				if len(content) > maxLen {
					content = "..." + content[len(content)-maxLen:]
				}
				log.Error().
					Str("container_id", containerID).
					Str("debug_log_tail", content).
					Msg("runsc debug log (last 2000 chars)")
			}
		}

		if exitErr, ok := err.(*exec.ExitError); ok {
			if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				return ws.ExitStatus(), nil
			}
		}

		return -1, err
	}

	return 0, nil
}

func (r *Runsc) Exec(ctx context.Context, containerID string, proc specs.Process, opts *ExecOpts) error {
	// Create a temporary process spec file
	procFile, err := os.CreateTemp("", "runsc-process-*.json")
	if err != nil {
		return fmt.Errorf("failed to create process spec: %w", err)
	}
	defer os.Remove(procFile.Name())

	procJSON, err := json.Marshal(proc)
	if err != nil {
		return fmt.Errorf("failed to marshal process spec: %w", err)
	}

	if _, err := procFile.Write(procJSON); err != nil {
		return fmt.Errorf("failed to write process spec: %w", err)
	}
	procFile.Close()

	args := r.baseArgs(false)
	args = append(args, "exec")
	args = append(args, "--process", procFile.Name())
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)

	if opts != nil && opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = opts.OutputWriter
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to exec in container: %w", err)
	}

	// Notify that process has started
	if opts != nil && opts.Started != nil {
		select {
		case opts.Started <- cmd.Process.Pid:
		default:
		}
	}

	return cmd.Wait()
}

func (r *Runsc) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *KillOpts) error {
	args := r.baseArgs(false)
	args = append(args, "kill")

	if opts != nil && opts.All {
		args = append(args, "--all")
	}

	args = append(args, containerID, fmt.Sprintf("%d", sig))

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	return cmd.Run()
}

func (r *Runsc) Delete(ctx context.Context, containerID string, opts *DeleteOpts) error {
	args := r.baseArgs(false)
	args = append(args, "delete")

	if opts != nil && opts.Force {
		args = append(args, "--force")
	}

	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	return cmd.Run()
}

func (r *Runsc) State(ctx context.Context, containerID string) (State, error) {
	args := r.baseArgs(false)
	args = append(args, "state", containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)

	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return State{}, ErrContainerNotFound{ContainerID: containerID}
		}
		return State{}, fmt.Errorf("failed to get state: %w", err)
	}

	// Parse the JSON output from runsc state
	var stateJSON struct {
		ID     string `json:"id"`
		Pid    int    `json:"pid"`
		Status string `json:"status"`
	}

	if err := json.Unmarshal(stdout.Bytes(), &stateJSON); err != nil {
		return State{}, fmt.Errorf("failed to parse state output: %w", err)
	}

	return State{
		ID:     stateJSON.ID,
		Pid:    stateJSON.Pid,
		Status: stateJSON.Status,
	}, nil
}

func (r *Runsc) Events(ctx context.Context, containerID string) (<-chan Event, error) {
	// gVisor doesn't support events API natively
	// Return a closed channel to indicate no events will be sent
	// Caller should use cgroup poller instead
	ch := make(chan Event)
	close(ch)
	return ch, nil
}

func (r *Runsc) Checkpoint(ctx context.Context, containerID string, opts *CheckpointOpts) error {
	if opts == nil {
		return fmt.Errorf("checkpoint options cannot be nil")
	}

	// Freeze CUDA processes before checkpointing (non-fatal)
	if r.nvproxyEnabled {
		if err := r.cudaCheckpointProcesses(ctx, containerID, "checkpoint", opts.OutputWriter); err != nil {
			// Log but don't fail - CUDA checkpoint is optional
			if opts.OutputWriter != nil {
				fmt.Fprintf(opts.OutputWriter, "Warning: CUDA checkpoint failed: %v\n", err)
			}
		}
	}

	// Ensure directories exist
	if opts.ImagePath != "" {
		if err := os.MkdirAll(opts.ImagePath, 0755); err != nil {
			return fmt.Errorf("failed to create image path: %w", err)
		}
	}
	if opts.WorkDir != "" {
		if err := os.MkdirAll(opts.WorkDir, 0755); err != nil {
			return fmt.Errorf("failed to create work dir: %w", err)
		}
	}

	args := r.baseArgs(false)
	args = append(args, "checkpoint")
	if opts.ImagePath != "" {
		args = append(args, "--image-path", opts.ImagePath)
	}

	// NOTE: gVisor's --work-path flag is marked as "ignored" in runsc
	// but we include it for compatibility
	if opts.WorkDir != "" {
		args = append(args, "--work-path", opts.WorkDir)
	}
	if opts.LeaveRunning {
		args = append(args, "--leave-running")
	}

	// NOTE: gVisor doesn't support CRIU-specific flags like:
	// --allow-open-tcp, --skip-in-flight, --link-remap
	// These are runc/CRIU specific and not available in runsc
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)

	// Capture both stdout and stderr for better error reporting
	var stderr bytes.Buffer
	if opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = io.MultiWriter(opts.OutputWriter, &stderr)
	} else {
		cmd.Stderr = &stderr
	}

	if err := cmd.Run(); err != nil {
		stderrStr := stderr.String()
		if stderrStr != "" {
			return fmt.Errorf("checkpoint failed: %w (stderr: %s)", err, stderrStr)
		}
		return fmt.Errorf("checkpoint failed: %w", err)
	}

	return nil
}

func (r *Runsc) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
	if opts == nil {
		return -1, fmt.Errorf("restore options cannot be nil")
	}

	defer func() {
		deleteArgs := r.baseArgs(false)
		deleteArgs = append(deleteArgs, "delete", "--force", containerID)
		_ = exec.Command(r.cfg.RunscPath, deleteArgs...).Run()
	}()

	// Ensure directories exist
	if opts.WorkDir != "" {
		if err := os.MkdirAll(opts.WorkDir, 0755); err != nil {
			return -1, fmt.Errorf("failed to create work dir: %w", err)
		}
	}

	args := r.baseArgs(false)
	if r.nvproxyEnabled {
		args = append(args, "--nvproxy=true")
		// Allow all driver capabilities for restore as well
		// Supported by gVisor: compute, utility, graphics, video (ngx is NOT supported)
		args = append(args, "--nvproxy-allowed-driver-capabilities=compute,utility,graphics,video")
	}
	args = append(args, "restore")
	if opts.ImagePath != "" {
		args = append(args, "--image-path", opts.ImagePath)
	}
	// Note: gVisor uses --work-path (not --work-dir)
	if opts.WorkDir != "" {
		args = append(args, "--work-path", opts.WorkDir)
	}
	if opts.BundlePath != "" {
		args = append(args, "--bundle", opts.BundlePath)
	}
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Capture stderr for better error reporting
	var stderr bytes.Buffer
	if opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = io.MultiWriter(opts.OutputWriter, &stderr)
	} else {
		cmd.Stderr = &stderr
	}

	if err := cmd.Start(); err != nil {
		stderrStr := stderr.String()
		if stderrStr != "" {
			return -1, fmt.Errorf("restore failed to start: %w (stderr: %s)", err, stderrStr)
		}
		return -1, err
	}

	if opts.Started != nil {
		opts.Started <- cmd.Process.Pid
	}

	go func() {
		<-ctx.Done()
		if pgid, _ := syscall.Getpgid(cmd.Process.Pid); pgid > 0 {
			syscall.Kill(-pgid, syscall.SIGKILL)
		}
	}()

	err := cmd.Wait()
	if err != nil {
		stderrStr := stderr.String()
		
		// Log stderr for debugging
		if stderrStr != "" {
			log.Error().
				Str("container_id", containerID).
				Str("runsc_stderr", stderrStr).
				Msg("runsc restore failed with stderr output")
		}

		// Try to read debug log if available
		if r.cfg.Debug {
			debugLogPath := filepath.Join(r.cfg.RunscRoot, "debug.log")
			if debugContent, readErr := os.ReadFile(debugLogPath); readErr == nil {
				// Log last 2000 chars to avoid overwhelming logs
				maxLen := 2000
				content := string(debugContent)
				if len(content) > maxLen {
					content = "..." + content[len(content)-maxLen:]
				}
				log.Error().
					Str("container_id", containerID).
					Str("debug_log_tail", content).
					Msg("runsc debug log (last 2000 chars)")
			}
		}

		if exitErr, ok := err.(*exec.ExitError); ok {
			if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				if stderrStr != "" {
					return ws.ExitStatus(), fmt.Errorf("restore failed with exit code %d (stderr: %s)", ws.ExitStatus(), stderrStr)
				}
				return ws.ExitStatus(), nil
			}
		}
		if stderrStr != "" {
			return -1, fmt.Errorf("restore failed: %w (stderr: %s)", err, stderrStr)
		}
		return -1, err
	}

	// Unfreeze CUDA processes after restore (non-fatal)
	if r.nvproxyEnabled {
		if err := r.cudaCheckpointProcesses(ctx, containerID, "restore", opts.OutputWriter); err != nil {
			// Log but don't fail - CUDA restore is optional
			if opts.OutputWriter != nil {
				fmt.Fprintf(opts.OutputWriter, "Warning: CUDA restore failed: %v\n", err)
			}
		}
	}

	return 0, nil
}

// cudaCheckpointProcesses runs cuda-checkpoint on all CUDA processes inside the container
// action should be "checkpoint" (freeze) or "restore" (unfreeze)
func (r *Runsc) cudaCheckpointProcesses(ctx context.Context, containerID, action string, outputWriter OutputWriter) error {
	pids, err := r.findCUDAProcesses(ctx, containerID)
	if err != nil || len(pids) == 0 {
		return nil // No CUDA processes, skip
	}

	for _, pid := range pids {
		args := r.baseArgs(false)
		args = append(args, "exec", containerID, "/usr/local/bin/cuda-checkpoint", action, strconv.Itoa(pid))

		cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
		if outputWriter != nil {
			cmd.Stdout = outputWriter
			cmd.Stderr = outputWriter
		}

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("cuda-checkpoint %s failed for PID %d: %w", action, pid, err)
		}
	}

	return nil
}

// findCUDAProcesses finds container PIDs with nvidia device file descriptors
func (r *Runsc) findCUDAProcesses(ctx context.Context, containerID string) ([]int, error) {
	args := r.baseArgs(false)
	args = append(args, "exec", containerID, "sh", "-c",
		"for pid in /proc/[0-9]*; do "+
			"[ -d \"$pid/fd\" ] && ls -l $pid/fd 2>/dev/null | grep -q nvidia && basename $pid; "+
			"done")

	output, err := exec.CommandContext(ctx, r.cfg.RunscPath, args...).Output()
	if err != nil {
		return []int{1}, nil // Fallback to PID 1
	}

	var pids []int
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if pid, err := strconv.Atoi(strings.TrimSpace(line)); err == nil && pid > 0 {
			pids = append(pids, pid)
		}
	}

	if len(pids) == 0 {
		return []int{1}, nil // Fallback to PID 1
	}

	return pids, nil
}

func (r *Runsc) Close() error {
	return nil
}

// baseArgs returns the common arguments for all runsc commands
func (r *Runsc) baseArgs(dockerEnabled bool) []string {
	args := []string{
		"--root", r.cfg.RunscRoot,
	}

	if r.cfg.Debug {
		args = append(args, "--debug", "--debug-log", filepath.Join(r.cfg.RunscRoot, "debug.log"))
	}

	if r.cfg.RunscPlatform != "" {
		args = append(args, "--platform", r.cfg.RunscPlatform)
	}

	// flags for rootfs propagation and external modification detection
	args = append(args, "--overlay2=none", "--file-access=shared")

	// Add --net-raw flag if Docker-in-Docker is enabled
	// This is required for Docker to function properly inside gVisor
	if dockerEnabled {
		args = append(args, "--net-raw")
	}

	return args
}

// AddDockerInDockerCapabilities adds the capabilities required for running Docker inside gVisor.
// According to gVisor documentation, Docker requires: audit_write, chown, dac_override, fowner,
// fsetid, kill, mknod, net_bind_service, net_admin, net_raw, setfcap, setgid, setpcap, setuid,
// sys_admin, sys_chroot, sys_ptrace
func (r *Runsc) AddDockerInDockerCapabilities(spec *specs.Spec) {
	if spec.Process == nil {
		spec.Process = &specs.Process{}
	}

	if spec.Process.Capabilities == nil {
		spec.Process.Capabilities = &specs.LinuxCapabilities{}
	}

	// Capabilities required for Docker-in-Docker according to gVisor documentation
	dockerCaps := []string{
		"CAP_AUDIT_WRITE",
		"CAP_CHOWN",
		"CAP_DAC_OVERRIDE",
		"CAP_FOWNER",
		"CAP_FSETID",
		"CAP_KILL",
		"CAP_MKNOD",
		"CAP_NET_BIND_SERVICE",
		"CAP_NET_ADMIN",
		"CAP_NET_RAW",
		"CAP_SETFCAP",
		"CAP_SETGID",
		"CAP_SETPCAP",
		"CAP_SETUID",
		"CAP_SYS_ADMIN",
		"CAP_SYS_CHROOT",
		"CAP_SYS_PTRACE",
	}

	// Add capabilities to all capability sets
	spec.Process.Capabilities.Bounding = mergeCapabilities(spec.Process.Capabilities.Bounding, dockerCaps)
	spec.Process.Capabilities.Effective = mergeCapabilities(spec.Process.Capabilities.Effective, dockerCaps)
	spec.Process.Capabilities.Permitted = mergeCapabilities(spec.Process.Capabilities.Permitted, dockerCaps)
	spec.Process.Capabilities.Inheritable = mergeCapabilities(spec.Process.Capabilities.Inheritable, dockerCaps)
}

// mergeCapabilities merges two capability lists, avoiding duplicates
func mergeCapabilities(existing []string, toAdd []string) []string {
	capSet := make(map[string]bool)

	// Add existing capabilities to the set
	for _, cap := range existing {
		capSet[cap] = true
	}

	// Add new capabilities to the set
	for _, cap := range toAdd {
		capSet[cap] = true
	}

	// Convert set back to slice
	result := make([]string, 0, len(capSet))
	for cap := range capSet {
		result = append(result, cap)
	}

	return result
}
