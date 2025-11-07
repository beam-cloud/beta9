package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// Runsc implements Runtime using the gVisor runsc runtime
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
			Runtime: "gvisor",
			Reason:  "runsc binary not found in PATH",
		}
	}

	return &Runsc{
		cfg: cfg,
	}, nil
}

func (r *Runsc) Name() string {
	return "gvisor"
}

func (r *Runsc) Capabilities() Capabilities {
	return Capabilities{
		CheckpointRestore: false, // NOTE: We don't support CRIU for gvisor yet
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
