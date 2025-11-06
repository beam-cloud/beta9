package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

func TestNewFirecracker(t *testing.T) {
	// Check if firecracker is available
	_, fcErr := exec.LookPath("firecracker")
	
	tests := []struct {
		name      string
		cfg       Config
		wantErr   bool
		errSubstr string
		skipIfNoFC bool
	}{
		{
			name: "valid config with defaults",
			cfg: Config{
				FirecrackerBin: "firecracker",
				KernelImage:    createTempKernel(t),
			},
			wantErr:    false,
			skipIfNoFC: true,
		},
		{
			name: "missing kernel image",
			cfg: Config{
				FirecrackerBin: "firecracker",
			},
			wantErr:    true,
			errSubstr:  "kernel image path is required",
			skipIfNoFC: true, // Skip if no FC - validation order means FC check happens first
		},
		{
			name: "kernel image not found",
			cfg: Config{
				FirecrackerBin: "firecracker",
				KernelImage:    "/nonexistent/kernel",
			},
			wantErr:    true,
			errSubstr:  "kernel image not found",
			skipIfNoFC: true, // Skip if no FC - validation order means FC check happens first
		},
		{
			name: "firecracker binary not found",
			cfg: Config{
				FirecrackerBin: "/nonexistent/firecracker",
				KernelImage:    createTempKernel(t),
			},
			wantErr:   true,
			errSubstr: "firecracker binary not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Skip test if firecracker not available and test requires it
			if tt.skipIfNoFC && fcErr != nil {
				t.Skip("firecracker binary not available")
			}

			// Set a temp root
			if tt.cfg.MicroVMRoot == "" {
				tt.cfg.MicroVMRoot = t.TempDir()
			}

			fc, err := NewFirecracker(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewFirecracker() expected error, got nil")
				} else if tt.errSubstr != "" && !contains(err.Error(), tt.errSubstr) {
					t.Errorf("NewFirecracker() error = %v, want substring %v", err, tt.errSubstr)
				}
				return
			}

			if err != nil {
				t.Errorf("NewFirecracker() unexpected error = %v", err)
				return
			}

			if fc == nil {
				t.Error("NewFirecracker() returned nil runtime")
				return
			}

			// Check defaults
			if fc.cfg.DefaultCPUs == 0 {
				t.Error("DefaultCPUs should be set to default value")
			}
			if fc.cfg.DefaultMemMiB == 0 {
				t.Error("DefaultMemMiB should be set to default value")
			}
		})
	}
}

func TestFirecrackerName(t *testing.T) {
	fc := &Firecracker{}
	if name := fc.Name(); name != "firecracker" {
		t.Errorf("Name() = %v, want %v", name, "firecracker")
	}
}

func TestFirecrackerCapabilities(t *testing.T) {
	fc := &Firecracker{}
	caps := fc.Capabilities()

	// Verify expected capabilities
	if caps.CheckpointRestore {
		t.Error("CheckpointRestore should be false for Phase 1")
	}
	if caps.GPU {
		t.Error("GPU should be false for microVMs")
	}
	if !caps.JoinExistingNetNS {
		t.Error("JoinExistingNetNS should be true")
	}
	if caps.CDI {
		t.Error("CDI should be false for microVMs")
	}
}

func TestFirecrackerPrepare(t *testing.T) {
	fc := &Firecracker{}

	tests := []struct {
		name    string
		spec    *specs.Spec
		wantErr bool
	}{
		{
			name:    "nil spec",
			spec:    nil,
			wantErr: true,
		},
		{
			name: "valid spec with linux config",
			spec: &specs.Spec{
				Linux: &specs.Linux{
					Namespaces: []specs.LinuxNamespace{
						{Type: specs.PIDNamespace},
					},
					Seccomp: &specs.LinuxSeccomp{},
					Devices: []specs.LinuxDevice{
						{Path: "/dev/null"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "spec with mounts",
			spec: &specs.Spec{
				Mounts: []specs.Mount{
					{Destination: "/proc", Type: "proc"},
					{Destination: "/custom", Type: "bind"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := fc.Prepare(ctx, tt.spec)

			if tt.wantErr {
				if err == nil {
					t.Error("Prepare() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Prepare() unexpected error = %v", err)
				return
			}

			// Verify modifications
			if tt.spec.Linux != nil {
				if tt.spec.Linux.Namespaces != nil {
					t.Error("Namespaces should be cleared")
				}
				if tt.spec.Linux.Seccomp != nil {
					t.Error("Seccomp should be cleared")
				}
				if tt.spec.Linux.Devices != nil {
					t.Error("Devices should be cleared")
				}
			}
		})
	}
}

func TestCreateSparseFile(t *testing.T) {
	fc := &Firecracker{}
	tmpDir := t.TempDir()
	
	tests := []struct {
		name    string
		size    int64
		wantErr bool
	}{
		{
			name:    "create 1MB file",
			size:    1024 * 1024,
			wantErr: false,
		},
		{
			name:    "create 100MB file",
			size:    100 * 1024 * 1024,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(tmpDir, tt.name+".img")
			err := fc.createSparseFile(path, tt.size)

			if tt.wantErr {
				if err == nil {
					t.Error("createSparseFile() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("createSparseFile() unexpected error = %v", err)
				return
			}

			// Verify file exists and has correct size
			info, err := os.Stat(path)
			if err != nil {
				t.Errorf("stat failed: %v", err)
				return
			}

			if info.Size() != tt.size {
				t.Errorf("file size = %d, want %d", info.Size(), tt.size)
			}
		})
	}
}

func TestGetMACAddress(t *testing.T) {
	fc := &Firecracker{}

	tests := []struct {
		name        string
		containerID string
		wantPrefix  string
	}{
		{
			name:        "valid container ID",
			containerID: "abcdef0123456789",
			wantPrefix:  "02:",
		},
		{
			name:        "another container ID",
			containerID: "1234567890abcdef",
			wantPrefix:  "02:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mac := fc.getMACAddress(tt.containerID)
			
			if len(mac) != 17 { // XX:XX:XX:XX:XX:XX format
				t.Errorf("MAC address length = %d, want 17", len(mac))
			}

			if mac[:3] != tt.wantPrefix {
				t.Errorf("MAC address prefix = %s, want %s", mac[:3], tt.wantPrefix)
			}
		})
	}
}

func TestBuildFirecrackerConfig(t *testing.T) {
	tmpDir := t.TempDir()
	fc := &Firecracker{
		cfg: Config{
			KernelImage:   "/path/to/kernel",
			DefaultCPUs:   2,
			DefaultMemMiB: 512,
		},
	}

	vm := &vmState{
		ID:        "test-vm-123",
		VsockPort: 10000,
		VsockSock: filepath.Join(tmpDir, "vsock.sock"),
		BlockPath: filepath.Join(tmpDir, "rootfs.ext4"),
	}

	spec := &specs.Spec{
		Linux: &specs.Linux{},
	}

	config := fc.buildFirecrackerConfig(vm, spec)

	// Verify config structure
	if config == nil {
		t.Fatal("config is nil")
	}

	// Check boot-source
	bootSource, ok := config["boot-source"].(map[string]interface{})
	if !ok {
		t.Fatal("boot-source not found or wrong type")
	}
	if bootSource["kernel_image_path"] != fc.cfg.KernelImage {
		t.Errorf("kernel_image_path = %v, want %v", bootSource["kernel_image_path"], fc.cfg.KernelImage)
	}

	// Check drives
	drives, ok := config["drives"].([]map[string]interface{})
	if !ok || len(drives) == 0 {
		t.Fatal("drives not found or empty")
	}
	if drives[0]["path_on_host"] != vm.BlockPath {
		t.Errorf("drive path = %v, want %v", drives[0]["path_on_host"], vm.BlockPath)
	}

	// Check machine-config
	machineConfig, ok := config["machine-config"].(map[string]interface{})
	if !ok {
		t.Fatal("machine-config not found")
	}
	if machineConfig["vcpu_count"] != int64(fc.cfg.DefaultCPUs) {
		t.Errorf("vcpu_count = %v, want %v", machineConfig["vcpu_count"], fc.cfg.DefaultCPUs)
	}
	if machineConfig["mem_size_mib"] != int64(fc.cfg.DefaultMemMiB) {
		t.Errorf("mem_size_mib = %v, want %v", machineConfig["mem_size_mib"], fc.cfg.DefaultMemMiB)
	}

	// Check vsock
	vsock, ok := config["vsock"].(map[string]interface{})
	if !ok {
		t.Fatal("vsock not found")
	}
	if vsock["guest_cid"] != vm.VsockPort {
		t.Errorf("guest_cid = %v, want %v", vsock["guest_cid"], vm.VsockPort)
	}
}

func TestStateTransitions(t *testing.T) {
	fc := &Firecracker{
		root: t.TempDir(),
	}

	containerID := "test-container-123"
	
	// Test getting state of non-existent container
	_, err := fc.State(context.Background(), containerID)
	if err == nil {
		t.Error("State() should return error for non-existent container")
	}

	// Create a VM state with a valid PID (our own process)
	vm := &vmState{
		ID:     containerID,
		Pid:    os.Getpid(), // Use current process PID so it exists
		Events: make(chan Event, 10),
	}
	vm.State.Store("starting")

	fc.vms.Store(containerID, vm)

	// Test getting state - should report starting
	state, err := fc.State(context.Background(), containerID)
	if err != nil {
		t.Errorf("State() unexpected error = %v", err)
	}
	if state.ID != containerID {
		t.Errorf("State.ID = %v, want %v", state.ID, containerID)
	}
	if state.Status != "starting" {
		t.Errorf("State.Status = %v, want starting", state.Status)
	}

	// Test state progression to running
	vm.State.Store("running")
	state, _ = fc.State(context.Background(), containerID)
	if state.Status != "running" {
		t.Errorf("State.Status = %v, want running", state.Status)
	}

	// Test with non-existent PID (will transition to stopped)
	vm.Pid = 99999 // Now use non-existent PID
	vm.State.Store("running")
	state, _ = fc.State(context.Background(), containerID)
	if state.Status != "stopped" {
		t.Errorf("State.Status = %v, want stopped (PID doesn't exist)", state.Status)
	}

	// Explicitly set to stopped
	vm.State.Store("stopped")
	state, _ = fc.State(context.Background(), containerID)
	if state.Status != "stopped" {
		t.Errorf("State.Status = %v, want stopped", state.Status)
	}
}

func TestEvents(t *testing.T) {
	fc := &Firecracker{}
	containerID := "test-container-456"

	// Test getting events of non-existent container
	_, err := fc.Events(context.Background(), containerID)
	if err == nil {
		t.Error("Events() should return error for non-existent container")
	}

	// Create a VM state
	vm := &vmState{
		ID:     containerID,
		Events: make(chan Event, 10),
	}
	vm.State.Store("running")

	fc.vms.Store(containerID, vm)

	// Test getting events
	eventCh, err := fc.Events(context.Background(), containerID)
	if err != nil {
		t.Errorf("Events() unexpected error = %v", err)
	}
	if eventCh == nil {
		t.Error("Events() returned nil channel")
	}

	// Test sending events
	testEvent := Event{Type: "test"}
	vm.Events <- testEvent

	select {
	case event := <-eventCh:
		if event.Type != testEvent.Type {
			t.Errorf("event.Type = %v, want %v", event.Type, testEvent.Type)
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for event")
	}
}

func TestClose(t *testing.T) {
	fc := &Firecracker{
		root: t.TempDir(),
	}

	// Add some VMs
	for i := 0; i < 3; i++ {
		vm := &vmState{
			ID:     "test-vm-" + string(rune('0'+i)),
			Events: make(chan Event, 10),
		}
		vm.State.Store("running")
		fc.vms.Store(vm.ID, vm)
	}

	// Close should not error
	if err := fc.Close(); err != nil {
		t.Errorf("Close() unexpected error = %v", err)
	}
}

// Helper functions

func createTempKernel(t *testing.T) string {
	tmpDir := t.TempDir()
	kernelPath := filepath.Join(tmpDir, "vmlinux")
	if err := os.WriteFile(kernelPath, []byte("fake kernel"), 0644); err != nil {
		t.Fatalf("failed to create temp kernel: %v", err)
	}
	return kernelPath
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
