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

	"github.com/opencontainers/runtime-spec/specs-go"
)

// Runsc implements Runtime using the gVisor runsc runtime
type Runsc struct {
	cfg        Config
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
		CheckpointRestore: false, // gVisor doesn't support CRIU
		GPU:               true,  // gVisor supports GPU via nvproxy
		OOMEvents:         false, // Use cgroup poller instead
		JoinExistingNetNS: true,  // gVisor can join network namespaces
		CDI:               true,  // gVisor supports CDI with nvproxy
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

	// Ensure no_new_privs is set
	if spec.Process != nil {
		noNewPrivs := true
		spec.Process.NoNewPrivileges = noNewPrivs
	}

	// Keep Linux namespaces (especially network) for joining existing netns
	// Keep bind mounts and tmpfs - these work fine with gVisor

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
	// Use create + start + wait pattern for proper PID reporting and exit code handling
	// This matches the OCI runtime spec and how runc works internally
	
	// Step 1: Create the container (creates but doesn't start)
	pidFile := filepath.Join(bundlePath, fmt.Sprintf("%s.pid", containerID))
	createArgs := r.baseArgs()
	if r.nvproxyEnabled {
		createArgs = append(createArgs, "--nvproxy=true")
	}
	createArgs = append(createArgs, "create")
	createArgs = append(createArgs, "--bundle", bundlePath)
	createArgs = append(createArgs, "--pid-file", pidFile)
	createArgs = append(createArgs, containerID)

	createCmd := exec.CommandContext(ctx, r.cfg.RunscPath, createArgs...)
	if opts != nil && opts.OutputWriter != nil {
		createCmd.Stdout = opts.OutputWriter
		createCmd.Stderr = opts.OutputWriter
	}
	
	if err := createCmd.Run(); err != nil {
		return -1, fmt.Errorf("failed to create container: %w", err)
	}

	// Read the container PID
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		_ = r.Delete(ctx, containerID, &DeleteOpts{Force: true})
		return -1, fmt.Errorf("failed to read container PID: %w", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		_ = r.Delete(ctx, containerID, &DeleteOpts{Force: true})
		return -1, fmt.Errorf("failed to parse container PID: %w", err)
	}
	_ = os.Remove(pidFile)

	// Notify that container has been created (send actual container PID)
	if opts != nil && opts.Started != nil {
		select {
		case opts.Started <- pid:
		default:
		}
	}

	// Step 2: Start the container (begins execution)
	startArgs := r.baseArgs()
	startArgs = append(startArgs, "start", containerID)

	startCmd := exec.CommandContext(ctx, r.cfg.RunscPath, startArgs...)
	if err := startCmd.Run(); err != nil {
		_ = r.Delete(ctx, containerID, &DeleteOpts{Force: true})
		return -1, fmt.Errorf("failed to start container: %w", err)
	}

	// Step 3: Wait for the container to exit
	waitArgs := r.baseArgs()
	waitArgs = append(waitArgs, "wait", containerID)

	waitCmd := exec.CommandContext(ctx, r.cfg.RunscPath, waitArgs...)
	
	// runsc wait outputs the exit code to stdout
	var outBuf bytes.Buffer
	waitCmd.Stdout = &outBuf
	waitCmd.Stderr = &outBuf

	err = waitCmd.Run()
	output := strings.TrimSpace(outBuf.String())
	
	// Parse exit code from runsc wait output
	exitCode := 0
	if output != "" {
		if parsedCode, parseErr := strconv.Atoi(output); parseErr == nil {
			exitCode = parsedCode
		}
	}

	// If wait command failed but we got an exit code, that's OK (container exited with non-zero)
	if err != nil && exitCode == 0 {
		// Couldn't determine exit code, default to 1
		exitCode = 1
	}

	return exitCode, nil
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

	args := r.baseArgs()
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
	args := r.baseArgs()
	args = append(args, "kill")
	
	if opts != nil && opts.All {
		args = append(args, "--all")
	}
	
	args = append(args, containerID, fmt.Sprintf("%d", sig))

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	return cmd.Run()
}

func (r *Runsc) Delete(ctx context.Context, containerID string, opts *DeleteOpts) error {
	args := r.baseArgs()
	args = append(args, "delete")
	
	if opts != nil && opts.Force {
		args = append(args, "--force")
	}
	
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	return cmd.Run()
}

func (r *Runsc) State(ctx context.Context, containerID string) (State, error) {
	args := r.baseArgs()
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
	// No resources to clean up
	return nil
}

// baseArgs returns the common arguments for all runsc commands
func (r *Runsc) baseArgs() []string {
	args := []string{
		"--root", r.cfg.RunscRoot,
	}

	if r.cfg.Debug {
		args = append(args, "--debug", "--debug-log", filepath.Join(r.cfg.RunscRoot, "debug.log"))
	}

	if r.cfg.RunscPlatform != "" {
		args = append(args, "--platform", r.cfg.RunscPlatform)
	}

	return args
}
