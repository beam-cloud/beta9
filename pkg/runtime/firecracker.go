package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// Firecracker implements Runtime using Firecracker microVMs
type Firecracker struct {
	cfg         Config
	root        string
	vms         sync.Map // containerID -> *vmState
	nextVsockID uint32   // Atomic counter for vsock ports
}

// vmState tracks the state of a single microVM
type vmState struct {
	ID            string
	Pid           int              // Firecracker process PID
	APISock       string           // Path to Firecracker API socket
	VsockPort     uint32           // Vsock port for guest communication
	VsockSock     string           // Path to vsock Unix socket proxy
	BlockPath     string           // Path to rootfs block device
	TapDevice     string           // TAP device name
	VMDir         string           // VM working directory
	State         atomic.Value     // Current state: "starting", "running", "stopped"
	Events        chan Event       // Event channel
	Cancel        context.CancelFunc
	GuestConn     net.Conn         // Connection to guest init
	mu            sync.Mutex
}

// NewFirecracker creates a new Firecracker runtime
func NewFirecracker(cfg Config) (*Firecracker, error) {
	// Set defaults
	if cfg.FirecrackerBin == "" {
		cfg.FirecrackerBin = "firecracker"
	}
	if cfg.MicroVMRoot == "" {
		cfg.MicroVMRoot = "/var/lib/beta9/microvm"
	}
	if cfg.DefaultCPUs == 0 {
		cfg.DefaultCPUs = 1
	}
	if cfg.DefaultMemMiB == 0 {
		cfg.DefaultMemMiB = 512
	}

	// Check if firecracker is available
	if _, err := exec.LookPath(cfg.FirecrackerBin); err != nil {
		return nil, ErrRuntimeNotAvailable{
			Runtime: "firecracker",
			Reason:  "firecracker binary not found in PATH",
		}
	}

	// Ensure root directory exists
	if err := os.MkdirAll(cfg.MicroVMRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create microvm root directory: %w", err)
	}

	// Validate kernel image
	if cfg.KernelImage == "" {
		return nil, fmt.Errorf("kernel image path is required for firecracker runtime")
	}
	if _, err := os.Stat(cfg.KernelImage); err != nil {
		return nil, fmt.Errorf("kernel image not found: %w", err)
	}

	return &Firecracker{
		cfg:         cfg,
		root:        cfg.MicroVMRoot,
		nextVsockID: 10000, // Start vsock ports at 10000
	}, nil
}

func (f *Firecracker) Name() string {
	return "firecracker"
}

func (f *Firecracker) Capabilities() Capabilities {
	return Capabilities{
		CheckpointRestore: false, // Phase 1: not yet implemented
		GPU:               false, // microVMs don't support GPU passthrough
		OOMEvents:         false, // Use cgroup poller as fallback
		JoinExistingNetNS: true,  // Via TAP in pod netns
		CDI:               false, // Not applicable for microVMs
	}
}

// Prepare validates and adjusts the OCI spec for microVM compatibility
func (f *Firecracker) Prepare(ctx context.Context, spec *specs.Spec) error {
	if spec == nil {
		return fmt.Errorf("spec is nil")
	}

	// microVMs have their own kernel and init system
	// We need to strip out host-specific configurations
	if spec.Linux != nil {
		// Remove namespaces - microVM provides its own isolation
		spec.Linux.Namespaces = nil

		// Remove cgroups - managed at VM level
		spec.Linux.CgroupsPath = ""
		spec.Linux.Resources = nil

		// Remove seccomp - not applicable in guest
		spec.Linux.Seccomp = nil

		// Remove devices - these will be mapped differently in microVM
		spec.Linux.Devices = nil

		// Clear masked and readonly paths - guest handles this
		spec.Linux.MaskedPaths = nil
		spec.Linux.ReadonlyPaths = nil
	}

	// Filter mounts - only those that can be baked into rootfs
	if spec.Mounts != nil {
		filteredMounts := []specs.Mount{}
		for _, mount := range spec.Mounts {
			// Keep essential mounts that will be in the guest rootfs
			switch mount.Destination {
			case "/proc", "/sys", "/dev", "/dev/pts", "/dev/shm", "/dev/mqueue":
				filteredMounts = append(filteredMounts, mount)
			}
		}
		spec.Mounts = filteredMounts
	}

	return nil
}

// Run starts a microVM with the given configuration
func (f *Firecracker) Run(ctx context.Context, containerID, bundlePath string, opts *RunOpts) (int, error) {
	// Create VM working directory
	vmDir := filepath.Join(f.root, containerID)
	if err := os.MkdirAll(vmDir, 0755); err != nil {
		return -1, fmt.Errorf("failed to create vm directory: %w", err)
	}

	// Read OCI spec
	specPath := filepath.Join(bundlePath, "config.json")
	specData, err := os.ReadFile(specPath)
	if err != nil {
		return -1, fmt.Errorf("failed to read OCI spec: %w", err)
	}

	var spec specs.Spec
	if err := json.Unmarshal(specData, &spec); err != nil {
		return -1, fmt.Errorf("failed to parse OCI spec: %w", err)
	}

	// Prepare rootfs
	rootfsPath := filepath.Join(bundlePath, "rootfs")
	blockPath, err := f.prepareRootfs(ctx, rootfsPath, vmDir)
	if err != nil {
		return -1, fmt.Errorf("failed to prepare rootfs: %w", err)
	}

	// Allocate vsock port
	vsockPort := atomic.AddUint32(&f.nextVsockID, 1)
	
	// Create sockets
	apiSock := filepath.Join(vmDir, "firecracker.sock")
	vsockSock := filepath.Join(vmDir, "vsock.sock")

	// Create VM state
	vmCtx, cancel := context.WithCancel(ctx)
	vm := &vmState{
		ID:        containerID,
		APISock:   apiSock,
		VsockPort: vsockPort,
		VsockSock: vsockSock,
		BlockPath: blockPath,
		VMDir:     vmDir,
		Events:    make(chan Event, 10),
		Cancel:    cancel,
	}
	vm.State.Store("starting")

	// Store VM state
	f.vms.Store(containerID, vm)

	// Start Firecracker process
	pid, err := f.startFirecracker(vmCtx, vm, &spec)
	if err != nil {
		vm.Cancel()
		f.vms.Delete(containerID)
		return -1, fmt.Errorf("failed to start firecracker: %w", err)
	}
	vm.Pid = pid

	// Wait for guest connection
	if err := f.waitForGuest(vmCtx, vm); err != nil {
		vm.Cancel()
		f.cleanupVM(vm)
		return -1, fmt.Errorf("failed to connect to guest: %w", err)
	}

	// Send process spec to guest
	if err := f.startGuestProcess(vmCtx, vm, &spec, opts); err != nil {
		vm.Cancel()
		f.cleanupVM(vm)
		return -1, fmt.Errorf("failed to start guest process: %w", err)
	}

	vm.State.Store("running")

	// Notify that container started
	if opts != nil && opts.Started != nil {
		opts.Started <- pid
	}

	// Start monitoring goroutines
	go f.monitorVM(vmCtx, vm, opts)

	return pid, nil
}

// startFirecracker launches the Firecracker process
func (f *Firecracker) startFirecracker(ctx context.Context, vm *vmState, spec *specs.Spec) (int, error) {
	// Set up networking (TAP device)
	if err := f.setupNetworking(vm); err != nil {
		return -1, fmt.Errorf("failed to setup networking: %w", err)
	}

	// Create Firecracker configuration
	config := f.buildFirecrackerConfig(vm, spec)
	
	// Add network configuration
	f.updateFirecrackerConfigWithNetwork(config, vm)
	
	configPath := filepath.Join(vm.VMDir, "config.json")
	
	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return -1, fmt.Errorf("failed to marshal firecracker config: %w", err)
	}
	
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return -1, fmt.Errorf("failed to write firecracker config: %w", err)
	}

	// Start Firecracker
	cmd := exec.CommandContext(ctx, f.cfg.FirecrackerBin,
		"--api-sock", vm.APISock,
		"--config-file", configPath,
	)

	if f.cfg.Debug {
		logPath := filepath.Join(vm.VMDir, "firecracker.log")
		logFile, err := os.Create(logPath)
		if err != nil {
			return -1, fmt.Errorf("failed to create log file: %w", err)
		}
		defer logFile.Close()
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}

	if err := cmd.Start(); err != nil {
		return -1, fmt.Errorf("failed to start firecracker: %w", err)
	}

	// Wait for API socket to be ready
	if err := f.waitForAPISocket(ctx, vm.APISock); err != nil {
		cmd.Process.Kill()
		return -1, fmt.Errorf("firecracker API socket not ready: %w", err)
	}

	return cmd.Process.Pid, nil
}

// buildFirecrackerConfig creates the Firecracker VM configuration
func (f *Firecracker) buildFirecrackerConfig(vm *vmState, spec *specs.Spec) map[string]interface{} {
	// Extract resource limits from spec
	vcpuCount := int64(f.cfg.DefaultCPUs)
	memSizeMib := int64(f.cfg.DefaultMemMiB)

	if spec.Linux != nil && spec.Linux.Resources != nil {
		if spec.Linux.Resources.CPU != nil && spec.Linux.Resources.CPU.Quota != nil {
			vcpuCount = *spec.Linux.Resources.CPU.Quota / 100000
			if vcpuCount < 1 {
				vcpuCount = 1
			}
		}
		if spec.Linux.Resources.Memory != nil && spec.Linux.Resources.Memory.Limit != nil {
			memSizeMib = *spec.Linux.Resources.Memory.Limit / (1024 * 1024)
			if memSizeMib < 128 {
				memSizeMib = 128
			}
		}
	}

	config := map[string]interface{}{
		"boot-source": map[string]interface{}{
			"kernel_image_path": f.cfg.KernelImage,
			"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off ip=dhcp",
		},
		"drives": []map[string]interface{}{
			{
				"drive_id":      "rootfs",
				"path_on_host":  vm.BlockPath,
				"is_root_device": true,
				"is_read_only":  false,
			},
		},
		"machine-config": map[string]interface{}{
			"vcpu_count":   vcpuCount,
			"mem_size_mib": memSizeMib,
			"ht_enabled":   false,
		},
		"vsock": map[string]interface{}{
			"guest_cid": vm.VsockPort,
			"uds_path":  vm.VsockSock,
		},
	}

	// Add initrd if configured
	if f.cfg.InitrdImage != "" {
		config["boot-source"].(map[string]interface{})["initrd_path"] = f.cfg.InitrdImage
	}

	return config
}

// waitForAPISocket waits for the Firecracker API socket to become available
func (f *Firecracker) waitForAPISocket(ctx context.Context, sockPath string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sockPath); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for API socket")
}

// waitForGuest waits for the guest init to connect via vsock
func (f *Firecracker) waitForGuest(ctx context.Context, vm *vmState) error {
	listener, err := net.Listen("unix", vm.VsockSock)
	if err != nil {
		return fmt.Errorf("failed to listen on vsock socket: %w", err)
	}
	defer listener.Close()

	// Wait for connection with timeout
	connCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		connCh <- conn
	}()

	select {
	case conn := <-connCh:
		vm.GuestConn = conn
		return nil
	case err := <-errCh:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for guest connection")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startGuestProcess sends the process spec to the guest and starts it
func (f *Firecracker) startGuestProcess(ctx context.Context, vm *vmState, spec *specs.Spec, opts *RunOpts) error {
	if spec.Process == nil {
		return fmt.Errorf("process spec is nil")
	}

	// Build guest process message
	msg := guestMessage{
		Type: "run",
		Process: &guestProcess{
			Args: spec.Process.Args,
			Env:  spec.Process.Env,
			Cwd:  spec.Process.Cwd,
			Terminal: spec.Process.Terminal,
		},
	}

	// Send message to guest
	encoder := json.NewEncoder(vm.GuestConn)
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to send process spec to guest: %w", err)
	}

	return nil
}

// monitorVM monitors the VM for events and output
func (f *Firecracker) monitorVM(ctx context.Context, vm *vmState, opts *RunOpts) {
	defer close(vm.Events)

	// Start output forwarder if needed
	if opts != nil && opts.OutputWriter != nil {
		go f.forwardOutput(ctx, vm, opts.OutputWriter)
	}

	// Wait for process exit or VM termination
	go f.waitForExit(ctx, vm)
}

// forwardOutput forwards guest process output to the output writer
func (f *Firecracker) forwardOutput(ctx context.Context, vm *vmState, writer OutputWriter) {
	if vm.GuestConn == nil {
		return
	}

	decoder := json.NewDecoder(vm.GuestConn)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var msg guestMessage
			if err := decoder.Decode(&msg); err != nil {
				if err != io.EOF {
					vm.Events <- Event{Type: "error", Err: fmt.Errorf("guest connection error: %w", err)}
				}
				return
			}

			switch msg.Type {
			case "output":
				if msg.Output != nil {
					writer.Write([]byte(*msg.Output))
				}
			case "exit":
				vm.State.Store("stopped")
				vm.Events <- Event{Type: "exit"}
				return
			case "error":
				if msg.Error != nil {
					vm.Events <- Event{Type: "error", Err: fmt.Errorf(*msg.Error)}
				}
				return
			}
		}
	}
}

// waitForExit waits for the Firecracker process to exit
func (f *Firecracker) waitForExit(ctx context.Context, vm *vmState) {
	// Wait for Firecracker process
	if vm.Pid > 0 {
		process, err := os.FindProcess(vm.Pid)
		if err == nil {
			process.Wait()
		}
	}

	vm.State.Store("stopped")
	vm.Events <- Event{Type: "exit"}
}

// Exec executes a command in the running microVM
func (f *Firecracker) Exec(ctx context.Context, containerID string, proc specs.Process, opts *ExecOpts) error {
	value, ok := f.vms.Load(containerID)
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}

	vm := value.(*vmState)

	// Check VM state
	state := vm.State.Load().(string)
	if state != "running" {
		return fmt.Errorf("container not running: %s", state)
	}

	// Build exec message
	msg := guestMessage{
		Type: "exec",
		Process: &guestProcess{
			Args:     proc.Args,
			Env:      proc.Env,
			Cwd:      proc.Cwd,
			Terminal: proc.Terminal,
		},
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Send exec command to guest
	encoder := json.NewEncoder(vm.GuestConn)
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to send exec command: %w", err)
	}

	// TODO: Handle exec output and exit status properly
	// For now, return success to match the interface
	return nil
}

// Kill sends a signal to the microVM
func (f *Firecracker) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *KillOpts) error {
	value, ok := f.vms.Load(containerID)
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}

	vm := value.(*vmState)

	// If SIGKILL and All flag, just kill the Firecracker process
	if opts != nil && opts.All && sig == syscall.SIGKILL {
		if vm.Pid > 0 {
			if err := syscall.Kill(vm.Pid, syscall.SIGKILL); err != nil {
				return fmt.Errorf("failed to kill firecracker process: %w", err)
			}
		}
		vm.State.Store("stopped")
		return nil
	}

	// Try to send kill command to guest
	msg := guestMessage{
		Type:   "kill",
		Signal: &sig,
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.GuestConn != nil {
		encoder := json.NewEncoder(vm.GuestConn)
		if err := encoder.Encode(msg); err != nil {
			// If sending fails, fall back to killing Firecracker
			if vm.Pid > 0 {
				return syscall.Kill(vm.Pid, syscall.SIGKILL)
			}
			return fmt.Errorf("failed to send kill command: %w", err)
		}
		return nil
	}

	// No connection, kill Firecracker process
	if vm.Pid > 0 {
		return syscall.Kill(vm.Pid, syscall.SIGKILL)
	}

	return fmt.Errorf("no way to kill container")
}

// Delete removes the microVM and cleans up resources
func (f *Firecracker) Delete(ctx context.Context, containerID string, opts *DeleteOpts) error {
	value, ok := f.vms.Load(containerID)
	if !ok {
		return ErrContainerNotFound{ContainerID: containerID}
	}

	vm := value.(*vmState)

	// Check if VM is still running
	state := vm.State.Load().(string)
	if state == "running" {
		if opts != nil && opts.Force {
			// Force kill the VM
			f.Kill(ctx, containerID, syscall.SIGKILL, &KillOpts{All: true})
		} else {
			return fmt.Errorf("container is still running, use force to delete")
		}
	}

	// Cleanup VM resources
	f.cleanupVM(vm)

	// Remove from map
	f.vms.Delete(containerID)

	return nil
}

// cleanupVM cleans up all resources associated with a VM
func (f *Firecracker) cleanupVM(vm *vmState) {
	// Cancel context
	if vm.Cancel != nil {
		vm.Cancel()
	}

	// Close guest connection
	if vm.GuestConn != nil {
		vm.GuestConn.Close()
	}

	// Kill Firecracker process if still running
	if vm.Pid > 0 {
		syscall.Kill(vm.Pid, syscall.SIGKILL)
	}

	// Remove TAP device if exists
	if vm.TapDevice != "" {
		// TAP device cleanup - best effort
		exec.Command("ip", "link", "delete", vm.TapDevice).Run()
	}

	// Unmount and remove rootfs
	if vm.BlockPath != "" {
		os.Remove(vm.BlockPath)
	}

	// Remove VM directory
	if vm.VMDir != "" {
		os.RemoveAll(vm.VMDir)
	}
}

// State returns the current state of a microVM
func (f *Firecracker) State(ctx context.Context, containerID string) (State, error) {
	value, ok := f.vms.Load(containerID)
	if !ok {
		return State{}, ErrContainerNotFound{ContainerID: containerID}
	}

	vm := value.(*vmState)

	// Check if Firecracker process is alive
	status := vm.State.Load().(string)
	if vm.Pid > 0 {
		process, err := os.FindProcess(vm.Pid)
		if err != nil || process.Signal(syscall.Signal(0)) != nil {
			status = "stopped"
			vm.State.Store("stopped")
		}
	}

	return State{
		ID:     containerID,
		Pid:    vm.Pid,
		Status: status,
	}, nil
}

// Events returns a channel for receiving container events
func (f *Firecracker) Events(ctx context.Context, containerID string) (<-chan Event, error) {
	value, ok := f.vms.Load(containerID)
	if !ok {
		return nil, ErrContainerNotFound{ContainerID: containerID}
	}

	vm := value.(*vmState)
	return vm.Events, nil
}

// Close cleans up all resources held by the runtime
func (f *Firecracker) Close() error {
	// Iterate through all VMs and clean them up
	f.vms.Range(func(key, value interface{}) bool {
		vm := value.(*vmState)
		f.cleanupVM(vm)
		return true
	})

	return nil
}

// guestMessage represents a message sent to/from the guest init
type guestMessage struct {
	Type    string         `json:"type"` // "run", "exec", "kill", "output", "exit", "error"
	Process *guestProcess  `json:"process,omitempty"`
	Signal  *syscall.Signal `json:"signal,omitempty"`
	Output  *string        `json:"output,omitempty"`
	Error   *string        `json:"error,omitempty"`
	ExitCode *int          `json:"exit_code,omitempty"`
}

// guestProcess represents a process to run in the guest
type guestProcess struct {
	Args     []string `json:"args"`
	Env      []string `json:"env"`
	Cwd      string   `json:"cwd"`
	Terminal bool     `json:"terminal"`
}
