package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// Runsc implements Runtime using the gVisor runsc runtime
//
// CUDA Checkpoint/Restore Workflow:
// gVisor requires a two-step process for CUDA checkpoints, similar to runc+CRIU:
//
// 1. Checkpoint:
//    a. Find CUDA processes using nvidia-smi from the HOST
//       nvidia-smi --query-compute-apps=pid gives us host PIDs using GPU
//    b. Run cuda-checkpoint from the HOST on each CUDA process (using host PIDs)
//       cuda-checkpoint checkpoint <host_pid>
//       This freezes the GPU state (memory, contexts, streams) at the driver level
//    c. Then run `runsc checkpoint` to create the checkpoint image
//       This captures the container state with frozen GPU operations
//
// 2. Restore:
//    a. Run `runsc restore` to restore the sandbox from checkpoint
//       This brings back the container with frozen CUDA state
//    b. Find CUDA processes again using nvidia-smi from the HOST
//    c. Run cuda-checkpoint from the HOST on each CUDA process (using host PIDs)
//       cuda-checkpoint restore <host_pid>
//       This unfreezes GPU operations at the driver level
//
// Key Points:
//   - cuda-checkpoint runs from the HOST (installed on worker), not inside the container
//   - PIDs are HOST PIDs, not container PIDs (similar to CRIU)
//   - Process detection uses nvidia-smi on host (cleaner than lsof/proc)
//   - nvproxy must be enabled (--nvproxy=true) for GPU passthrough
//   - NVIDIA driver >= 570 recommended for full checkpoint support
//   - GPU devices must be in OCI spec (CDI or direct)
//
// This approach is analogous to how runc+CRIU works:
//   - CRIU runs on host and operates on host PIDs
//   - cuda-checkpoint (as CRIU plugin) runs on host
//   - For gVisor, we manually orchestrate what CRIU would do automatically
type Runsc struct {
	cfg            Config
	nvproxyEnabled bool // Whether GPU support via nvproxy is enabled
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
		CheckpointRestore: true, // gVisor has native checkpoint/restore support
		GPU:               true,
		OOMEvents:         false,
		JoinExistingNetNS: true,
		CDI:               true,
	}
}

// Prepare mutates the OCI spec to be compatible with gVisor
func (r *Runsc) Prepare(ctx context.Context, spec *specs.Spec) error {
	if spec == nil {
		return fmt.Errorf("spec is nil")
	}

	// Remove seccomp profiles - gVisor is a userspace kernel and doesn't use seccomp
	if spec.Linux != nil {
		spec.Linux.Seccomp = nil

		// Check if this spec has GPU devices (CDI or direct)
		// If so, enable nvproxy support
		r.nvproxyEnabled = r.hasGPUDevices(spec)

		if r.nvproxyEnabled {
			// For nvproxy, we keep the device specifications
			// gVisor will intercept GPU calls and proxy them to the host driver
			// The devices will be available through nvproxy, not direct passthrough
		} else {
			// Clear devices list if no GPU - gVisor virtualizes /dev
			spec.Linux.Devices = nil
		}
	}

	return nil
}

// hasGPUDevices checks if the spec contains GPU device configurations
func (r *Runsc) hasGPUDevices(spec *specs.Spec) bool {
	if spec.Linux == nil {
		return false
	}

	// Check for NVIDIA GPU devices
	for _, device := range spec.Linux.Devices {
		path := device.Path
		// NVIDIA GPU devices typically start with /dev/nvidia
		if len(path) >= 11 && path[:11] == "/dev/nvidia" {
			return true
		}
		if len(path) >= 13 && path[:13] == "/dev/nvidiactl" {
			return true
		}
		if len(path) >= 15 && path[:15] == "/dev/nvidia-uvm" {
			return true
		}
	}

	// Check for CDI device annotations
	if spec.Annotations != nil {
		for key := range spec.Annotations {
			// CDI devices are specified via annotations like "cdi.k8s.io/nvidia"
			if len(key) >= 10 && key[:10] == "cdi.k8s.io" {
				return true
			}
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
	}
	args = append(args, "run", "--bundle", bundlePath, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Kill entire process tree

	if opts != nil && opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = opts.OutputWriter
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

	// Step 1: If GPU is enabled, run cuda-checkpoint INSIDE the sandbox to freeze CUDA processes
	// This must happen before the runsc checkpoint command
	if r.nvproxyEnabled {
		if err := r.freezeCUDAProcesses(ctx, containerID, opts.OutputWriter); err != nil {
			return fmt.Errorf("failed to freeze CUDA processes before checkpoint: %w", err)
		}
	}

	// Step 2: Run runsc checkpoint to create the checkpoint image
	args := r.baseArgs(false)
	args = append(args, "checkpoint")

	// Add checkpoint options
	if opts.ImagePath != "" {
		args = append(args, "--image-path", opts.ImagePath)
	}

	// Work directory for checkpoint files (logs, cache, sockets)
	if opts.WorkDir != "" {
		args = append(args, "--work-dir", opts.WorkDir)
	}

	// Leave container running after checkpoint (important for hot checkpointing)
	if opts.LeaveRunning {
		args = append(args, "--leave-running")
	}

	// Allow open TCP connections during checkpoint
	if opts.AllowOpenTCP {
		args = append(args, "--allow-open-tcp")
	}

	// Skip in-flight TCP connections (similar to CRIU tcp-skip-in-flight)
	if opts.SkipInFlight {
		args = append(args, "--skip-in-flight")
	}

	// gVisor checkpoint command
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)

	if opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = opts.OutputWriter
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("checkpoint failed: %w", err)
	}

	return nil
}

// freezeCUDAProcesses runs cuda-checkpoint inside the gVisor sandbox to freeze CUDA state
// This must be called BEFORE running `runsc checkpoint`
func (r *Runsc) freezeCUDAProcesses(ctx context.Context, containerID string, outputWriter OutputWriter) error {
	// Find all CUDA processes in the container
	cudaPIDs, err := r.findCUDAProcesses(ctx, containerID)
	if err != nil {
		return fmt.Errorf("failed to find CUDA processes: %w", err)
	}

	if len(cudaPIDs) == 0 {
		// No CUDA processes found, skip cuda-checkpoint
		return nil
	}

	// Run cuda-checkpoint on each CUDA process to freeze GPU state
	for _, pid := range cudaPIDs {
		if err := r.runCUDACheckpoint(ctx, containerID, pid, "checkpoint", outputWriter); err != nil {
			return fmt.Errorf("failed to freeze CUDA process %d: %w", pid, err)
		}
	}

	return nil
}

func (r *Runsc) Restore(ctx context.Context, containerID string, opts *RestoreOpts) (int, error) {
	if opts == nil {
		return -1, fmt.Errorf("restore options cannot be nil")
	}

	// Cleanup any existing container with this ID
	defer func() {
		deleteArgs := r.baseArgs(false)
		deleteArgs = append(deleteArgs, "delete", "--force", containerID)
		_ = exec.Command(r.cfg.RunscPath, deleteArgs...).Run()
	}()

	// Step 1: Run runsc restore to restore the sandbox from checkpoint
	args := r.baseArgs(false)
	
	// Enable nvproxy for GPU workloads (must be set before restore command)
	if r.nvproxyEnabled {
		args = append(args, "--nvproxy=true")
	}

	args = append(args, "restore")

	// Add restore options
	if opts.ImagePath != "" {
		args = append(args, "--image-path", opts.ImagePath)
	}

	// Work directory for restore files
	if opts.WorkDir != "" {
		args = append(args, "--work-dir", opts.WorkDir)
	}

	if opts.BundlePath != "" {
		args = append(args, "--bundle", opts.BundlePath)
	}

	// Container ID
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Kill entire process tree

	if opts.OutputWriter != nil {
		cmd.Stdout = opts.OutputWriter
		cmd.Stderr = opts.OutputWriter
	}

	if err := cmd.Start(); err != nil {
		return -1, fmt.Errorf("restore failed to start: %w", err)
	}

	if opts.Started != nil {
		opts.Started <- cmd.Process.Pid
	}

	// Handle cancellation by killing process group
	go func() {
		<-ctx.Done()
		if pgid, _ := syscall.Getpgid(cmd.Process.Pid); pgid > 0 {
			syscall.Kill(-pgid, syscall.SIGKILL)
		}
	}()

	// Wait for restore to complete
	err := cmd.Wait()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				return ws.ExitStatus(), nil
			}
		}
		return -1, err
	}

	// Step 2: If GPU is enabled, run cuda-checkpoint INSIDE the restored sandbox to unfreeze CUDA processes
	// This must happen AFTER the runsc restore command
	if r.nvproxyEnabled {
		if err := r.unfreezeCUDAProcesses(ctx, containerID, opts.OutputWriter); err != nil {
			return -1, fmt.Errorf("failed to unfreeze CUDA processes after restore: %w", err)
		}
	}

	return 0, nil
}

// unfreezeCUDAProcesses runs cuda-checkpoint inside the gVisor sandbox to unfreeze CUDA state
// This must be called AFTER running `runsc restore`
func (r *Runsc) unfreezeCUDAProcesses(ctx context.Context, containerID string, outputWriter OutputWriter) error {
	// Find all CUDA processes in the restored container
	cudaPIDs, err := r.findCUDAProcesses(ctx, containerID)
	if err != nil {
		return fmt.Errorf("failed to find CUDA processes: %w", err)
	}

	if len(cudaPIDs) == 0 {
		// No CUDA processes found, skip cuda-checkpoint
		return nil
	}

	// Run cuda-checkpoint on each CUDA process to unfreeze GPU state
	for _, pid := range cudaPIDs {
		if err := r.runCUDACheckpoint(ctx, containerID, pid, "restore", outputWriter); err != nil {
			return fmt.Errorf("failed to unfreeze CUDA process %d: %w", pid, err)
		}
	}

	return nil
}

// findCUDAProcesses finds all processes in the container that are using CUDA
// Returns a list of PIDs (as seen from the HOST perspective) that have CUDA contexts
func (r *Runsc) findCUDAProcesses(ctx context.Context, containerID string) ([]int, error) {
	// Use nvidia-smi on the host to find all processes using the GPU
	// This is cleaner and more reliable than parsing /proc or using lsof
	cmd := exec.CommandContext(ctx, "nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader")
	output, err := cmd.Output()
	if err != nil {
		// nvidia-smi not available or no GPU processes
		return []int{}, nil
	}

	// Parse PIDs from output
	var hostPIDs []int
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if pid, err := strconv.Atoi(line); err == nil {
			hostPIDs = append(hostPIDs, pid)
		}
	}

	if len(hostPIDs) == 0 {
		return []int{}, nil
	}

	// Filter to only PIDs that belong to this container
	// by checking if the PID's parent chain includes the container's sandbox process
	containerPIDs := []int{}
	for _, pid := range hostPIDs {
		if belongs, err := r.pidBelongsToContainer(ctx, pid, containerID); err == nil && belongs {
			containerPIDs = append(containerPIDs, pid)
		}
	}

	return containerPIDs, nil
}

// pidBelongsToContainer checks if a given PID (from host perspective) belongs to the specified container
func (r *Runsc) pidBelongsToContainer(ctx context.Context, pid int, containerID string) (bool, error) {
	// Get the container's sandbox PID
	stateCmd := exec.CommandContext(ctx, r.cfg.RunscPath, "--root", r.cfg.RunscRoot, "state", containerID)
	output, err := stateCmd.Output()
	if err != nil {
		return false, err
	}

	// Parse the state JSON to get the sandbox PID
	var state struct {
		Pid int `json:"pid"`
	}
	if err := json.Unmarshal(output, &state); err != nil {
		return false, err
	}

	sandboxPID := state.Pid
	if sandboxPID == 0 {
		return false, fmt.Errorf("container not running")
	}

	// Check if the PID is in the same PID namespace as the sandbox
	// by comparing cgroup or checking parent chain
	pidCgroup, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return false, err
	}

	sandboxCgroup, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", sandboxPID))
	if err != nil {
		return false, err
	}

	// If cgroups match, the PID belongs to this container
	return bytes.Contains(pidCgroup, []byte(containerID)) || 
	       bytes.Equal(pidCgroup, sandboxCgroup), nil
}

// runCUDACheckpoint runs the cuda-checkpoint binary from the HOST on a specific process
// The PID is from the host perspective (not container PID namespace)
// action should be "checkpoint" (freeze) or "restore" (unfreeze)
func (r *Runsc) runCUDACheckpoint(ctx context.Context, containerID string, pid int, action string, outputWriter OutputWriter) error {
	// cuda-checkpoint runs from the HOST and targets host PIDs
	// This is similar to how CRIU operates with runc
	// 
	// The cuda-checkpoint binary is installed on the worker/host, not in the container
	// It operates at the host level, checkpointing GPU state for specific PIDs
	//
	// Example: cuda-checkpoint checkpoint 1234
	//          cuda-checkpoint restore 1234
	
	cmd := exec.CommandContext(ctx, "cuda-checkpoint", action, fmt.Sprintf("%d", pid))
	
	if outputWriter != nil {
		cmd.Stdout = outputWriter
		cmd.Stderr = outputWriter
	}

	if err := cmd.Run(); err != nil {
		// cuda-checkpoint might not be installed or the process might not have CUDA contexts
		// Log but don't fail - this allows graceful degradation
		return fmt.Errorf("cuda-checkpoint %s failed for host PID %d: %w", action, pid, err)
	}

	return nil
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
