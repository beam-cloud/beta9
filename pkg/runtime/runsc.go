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
	"strings"
	"syscall"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	runscRestoreStateTimeout      = 30 * time.Second
	runscRestoreStatePollInterval = 25 * time.Millisecond
	runscDeleteTimeout            = 5 * time.Second
	runscGPUAnnotation            = "com.beam.gvisor.nvproxy"
	cudaCheckpointContainerPath   = "/usr/local/bin/cuda-checkpoint"
)

// Runsc implements Runtime using the gVisor runsc runtime
//
// CUDA Checkpoint/Restore is delegated to runsc. GPU bundles include the
// cuda-checkpoint helper and are marked so each runtime operation can select
// nvproxy without sharing mutable state across containers.
type Runsc struct {
	cfg                   Config
	dockerPacketWriteFlag string
}

type runscState struct {
	ID     string `json:"id"`
	Pid    int    `json:"pid"`
	Status string `json:"status"`
	Bundle string `json:"bundle"`
}

type runscCommandResult struct {
	exitCode int
	err      error
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
		cfg:                   cfg,
		dockerPacketWriteFlag: selectDockerPacketWriteFlag(runscFlags(cfg.RunscPath)),
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
		CDI:               true,
	}
}

// Prepare mutates the OCI spec to be compatible with gVisor
func (r *Runsc) Prepare(ctx context.Context, spec *specs.Spec) error {
	if spec == nil || spec.Linux == nil {
		return fmt.Errorf("spec is nil")
	}

	spec.Linux.Seccomp = nil
	if r.hasGPUDevices(spec) {
		if spec.Annotations == nil {
			spec.Annotations = make(map[string]string)
		}
		spec.Annotations[runscGPUAnnotation] = "true"
		r.mountCudaCheckpoint(spec)
	} else if spec.Annotations != nil {
		delete(spec.Annotations, runscGPUAnnotation)
	}

	// gVisor does not use spec.Linux.Devices for device passthrough.
	// For GPU workloads, nvproxy handles GPU access via its own virtualization layer
	// using CDI annotations and mounts, not device entries.
	// Clear devices to prevent conflicts with nvproxy
	spec.Linux.Devices = nil

	return nil
}

// mountCudaCheckpoint bind-mounts cuda-checkpoint binary into the container
func (r *Runsc) mountCudaCheckpoint(spec *specs.Spec) {
	cudaCheckpointPath, err := exec.LookPath("cuda-checkpoint")
	if err != nil {
		return // Not found, skip
	}

	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: cudaCheckpointContainerPath,
		Type:        "bind",
		Source:      cudaCheckpointPath,
		Options:     []string{"bind", "ro"},
	})
}

// hasGPUDevices checks if the spec contains GPU device configurations
func (r *Runsc) hasGPUDevices(spec *specs.Spec) bool {
	if spec == nil || spec.Linux == nil {
		return false
	}

	for _, device := range spec.Linux.Devices {
		if strings.HasPrefix(device.Path, "/dev/nvidia") {
			return true
		}
	}

	for key := range spec.Annotations {
		if strings.HasPrefix(key, "cdi.k8s.io") {
			return true
		}
	}

	return false
}

func (r *Runsc) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
	dockerEnabled := opts != nil && opts.DockerEnabled
	nvproxyEnabled, err := r.bundleUsesGPU(bundlePath)
	if err != nil {
		return -1, err
	}

	defer func() {
		r.forceDelete(containerID, dockerEnabled)
	}()

	args := r.baseArgs(dockerEnabled)
	if nvproxyEnabled {
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
	err = cmd.Wait()
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

func (r *Runsc) UpdateResources(ctx context.Context, containerID string, resources *specs.LinuxResources) error {
	if resources == nil {
		return fmt.Errorf("resources cannot be nil")
	}

	file, err := os.CreateTemp("", "runsc-resources-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	if err := json.NewEncoder(file).Encode(resources); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	args := append(r.baseArgs(false), "update", "--resources", file.Name(), containerID)
	if output, err := exec.CommandContext(ctx, r.cfg.RunscPath, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to update container resources: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
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
	state, err := r.loadState(ctx, containerID)
	if err != nil {
		return State{}, err
	}

	return State{
		ID:     state.ID,
		Pid:    state.Pid,
		Status: state.Status,
	}, nil
}

func (r *Runsc) loadState(ctx context.Context, containerID string) (runscState, error) {
	args := r.baseArgs(false)
	args = append(args, "state", containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)

	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return runscState{}, ErrContainerNotFound{ContainerID: containerID}
		}
		return runscState{}, fmt.Errorf("failed to get state: %w", err)
	}

	var state runscState
	if err := json.Unmarshal(stdout.Bytes(), &state); err != nil {
		return runscState{}, fmt.Errorf("failed to parse state output: %w", err)
	}
	return state, nil
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

	nvproxyEnabled, err := r.containerUsesGPU(ctx, containerID)
	if err != nil {
		return fmt.Errorf("failed to inspect container bundle: %w", err)
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
	if nvproxyEnabled {
		args = append(args, "--cuda-checkpoint-path", cudaCheckpointContainerPath)
	}
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

	nvproxyEnabled, err := r.bundleUsesGPU(opts.BundlePath)
	if err != nil {
		return -1, err
	}

	// Ensure directories exist
	if opts.WorkDir != "" {
		if err := os.MkdirAll(opts.WorkDir, 0755); err != nil {
			return -1, fmt.Errorf("failed to create work dir: %w", err)
		}
	}

	args := r.baseArgs(false)
	if nvproxyEnabled {
		args = append(args, "--nvproxy=true")
	}
	args = append(args, "restore", "--background", "--direct")
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
	cleanupOnFailure := true
	defer func() {
		if cleanupOnFailure {
			r.forceDelete(containerID, false)
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
	}()

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

	go func() {
		<-ctx.Done()
		r.forceDelete(containerID, false)
		_ = cmd.Process.Kill()
	}()

	restoreDone := make(chan runscCommandResult, 1)
	go func() {
		restoreDone <- runscWaitResult(cmd.Wait(), stderr.String(), "restore")
	}()

	pid, err := r.waitForRestoredContainerPID(ctx, containerID, restoreDone)
	if err != nil {
		return -1, err
	}

	if err := r.waitForRestore(ctx, containerID); err != nil {
		return -1, err
	}

	if opts.Started != nil {
		select {
		case opts.Started <- pid:
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}

	cleanupOnFailure = false
	return 0, nil
}

func (r *Runsc) forceDelete(containerID string, dockerEnabled bool) {
	ctx, cancel := context.WithTimeout(context.Background(), runscDeleteTimeout)
	defer cancel()

	args := append(r.baseArgs(dockerEnabled), "delete", "--force", containerID)
	_ = exec.CommandContext(ctx, r.cfg.RunscPath, args...).Run()
}

func (r *Runsc) waitForRestore(ctx context.Context, containerID string) error {
	args := append(r.baseArgs(false), "wait", "--restore", containerID)
	if output, err := exec.CommandContext(ctx, r.cfg.RunscPath, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("failed waiting for restore completion: %w (output: %s)", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (r *Runsc) waitForRestoredContainerPID(ctx context.Context, containerID string, restoreDone <-chan runscCommandResult) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, runscRestoreStateTimeout)
	defer cancel()

	ticker := time.NewTicker(runscRestoreStatePollInterval)
	defer ticker.Stop()

	var lastErr error
	var restoreResult *runscCommandResult
	for {
		state, err := r.State(ctx, containerID)
		if err == nil && state.Pid > 0 {
			return state.Pid, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("restored container state has no pid")
		}

		select {
		case <-ctx.Done():
			if restoreResult != nil && restoreResult.err != nil {
				return restoreResult.exitCode, restoreResult.err
			}
			return -1, fmt.Errorf("restore succeeded but restored container state was unavailable: %w", lastErr)
		case result := <-restoreDone:
			restoreResult = &result
			restoreDone = nil
			if result.err != nil {
				return result.exitCode, result.err
			}
		case <-ticker.C:
		}
	}
}

func runscWaitResult(err error, stderr, operation string) runscCommandResult {
	if err == nil {
		return runscCommandResult{exitCode: 0}
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			exitCode := ws.ExitStatus()
			if stderr != "" {
				return runscCommandResult{
					exitCode: exitCode,
					err:      fmt.Errorf("%s failed with exit code %d (stderr: %s)", operation, exitCode, stderr),
				}
			}
			return runscCommandResult{exitCode: exitCode, err: err}
		}
	}

	if stderr != "" {
		return runscCommandResult{
			exitCode: -1,
			err:      fmt.Errorf("%s failed: %w (stderr: %s)", operation, err, stderr),
		}
	}

	return runscCommandResult{exitCode: -1, err: err}
}

func (r *Runsc) containerUsesGPU(ctx context.Context, containerID string) (bool, error) {
	state, err := r.loadState(ctx, containerID)
	if err != nil {
		return false, err
	}
	return r.bundleUsesGPU(state.Bundle)
}

func (r *Runsc) bundleUsesGPU(bundlePath string) (bool, error) {
	if bundlePath == "" {
		return false, fmt.Errorf("container bundle path is empty")
	}

	config, err := os.Open(filepath.Join(bundlePath, "config.json"))
	if err != nil {
		return false, fmt.Errorf("failed to open container bundle: %w", err)
	}
	defer config.Close()

	var spec specs.Spec
	if err := json.NewDecoder(config).Decode(&spec); err != nil {
		return false, fmt.Errorf("failed to decode container bundle: %w", err)
	}
	return spec.Annotations[runscGPUAnnotation] == "true" || r.hasGPUDevices(&spec), nil
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

	args = append(args, r.cfg.RunscExtraArgs...)

	// Add flags required for Docker to function properly inside gVisor.
	if dockerEnabled {
		args = append(args, "--net-raw")
		if r.dockerPacketWriteFlag != "" {
			args = append(args, r.dockerPacketWriteFlag)
		}
	}

	return args
}

func runscFlags(runscPath string) string {
	out, err := exec.Command(runscPath, "flags").Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func selectDockerPacketWriteFlag(flags string) string {
	switch {
	case strings.Contains(flags, "-allow-packet-socket-write"):
		return "--allow-packet-socket-write"
	case strings.Contains(flags, "-TESTONLY-allow-packet-endpoint-write"):
		return "--TESTONLY-allow-packet-endpoint-write"
	default:
		return ""
	}
}

// AddDockerInDockerCapabilities adds the capabilities required for running Docker inside gVisor.
func (r *Runsc) AddDockerInDockerCapabilities(spec *specs.Spec) {
	AddDockerInDockerCapabilities(spec)
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
