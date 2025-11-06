// +build integration

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// TestFirecrackerIntegration tests the full lifecycle of a microVM
// Run with: go test -tags=integration -run TestFirecrackerIntegration
func TestFirecrackerIntegration(t *testing.T) {
	// Skip if not running as root
	if os.Geteuid() != 0 {
		t.Skip("Integration tests require root privileges")
	}

	// Check if firecracker is available
	if _, err := exec.LookPath("firecracker"); err != nil {
		t.Skip("firecracker binary not found in PATH")
	}

	// Setup test environment
	tmpDir := t.TempDir()
	kernelPath := os.Getenv("FIRECRACKER_KERNEL")
	if kernelPath == "" {
		t.Skip("FIRECRACKER_KERNEL environment variable not set")
	}

	cfg := Config{
		Type:           "firecracker",
		FirecrackerBin: "firecracker",
		MicroVMRoot:    filepath.Join(tmpDir, "microvms"),
		KernelImage:    kernelPath,
		DefaultCPUs:    1,
		DefaultMemMiB:  256,
		Debug:          true,
	}

	fc, err := NewFirecracker(cfg)
	if err != nil {
		t.Fatalf("NewFirecracker() failed: %v", err)
	}
	defer fc.Close()

	// Create bundle with test container
	bundlePath := filepath.Join(tmpDir, "bundle")
	if err := createTestBundle(t, bundlePath); err != nil {
		t.Fatalf("createTestBundle() failed: %v", err)
	}

	containerID := "test-container-" + time.Now().Format("20060102-150405")

	// Test Run
	t.Run("Run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		started := make(chan int, 1)
		opts := &RunOpts{
			OutputWriter: os.Stdout,
			Started:      started,
		}

		// Run in goroutine since it blocks
		errCh := make(chan error, 1)
		go func() {
			_, err := fc.Run(ctx, containerID, bundlePath, opts)
			errCh <- err
		}()

		// Wait for container to start
		select {
		case pid := <-started:
			t.Logf("Container started with PID %d", pid)
		case err := <-errCh:
			if err != nil {
				t.Fatalf("Run() failed: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Timeout waiting for container to start")
		}

		// Give container some time to run
		time.Sleep(2 * time.Second)

		// Test State
		t.Run("State", func(t *testing.T) {
			state, err := fc.State(ctx, containerID)
			if err != nil {
				t.Errorf("State() failed: %v", err)
			} else {
				t.Logf("Container state: %+v", state)
				if state.Status != "running" {
					t.Errorf("Expected status 'running', got %s", state.Status)
				}
			}
		})

		// Test Events
		t.Run("Events", func(t *testing.T) {
			eventCh, err := fc.Events(ctx, containerID)
			if err != nil {
				t.Errorf("Events() failed: %v", err)
			} else {
				// Just verify we can get the channel
				t.Logf("Event channel obtained: %v", eventCh != nil)
			}
		})

		// Test Kill
		t.Run("Kill", func(t *testing.T) {
			killOpts := &KillOpts{All: true}
			if err := fc.Kill(ctx, containerID, syscall.SIGTERM, killOpts); err != nil {
				t.Errorf("Kill() failed: %v", err)
			}
			
			// Wait for process to exit
			select {
			case err := <-errCh:
				if err != nil && err != context.Canceled {
					t.Logf("Run() exited with: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Log("Timeout waiting for process to exit, forcing kill")
				fc.Kill(ctx, containerID, syscall.SIGKILL, &KillOpts{All: true})
			}
		})

		// Test Delete
		t.Run("Delete", func(t *testing.T) {
			deleteOpts := &DeleteOpts{Force: true}
			if err := fc.Delete(ctx, containerID, deleteOpts); err != nil {
				t.Errorf("Delete() failed: %v", err)
			}

			// Verify container is gone
			_, err := fc.State(ctx, containerID)
			if err == nil {
				t.Error("State() should fail for deleted container")
			}
		})
	})
}

// TestFirecrackerMultipleVMs tests running multiple microVMs concurrently
func TestFirecrackerMultipleVMs(t *testing.T) {
	// Skip if not running as root
	if os.Geteuid() != 0 {
		t.Skip("Integration tests require root privileges")
	}

	// Check if firecracker is available
	if _, err := exec.LookPath("firecracker"); err != nil {
		t.Skip("firecracker binary not found in PATH")
	}

	tmpDir := t.TempDir()
	kernelPath := os.Getenv("FIRECRACKER_KERNEL")
	if kernelPath == "" {
		t.Skip("FIRECRACKER_KERNEL environment variable not set")
	}

	cfg := Config{
		Type:           "firecracker",
		FirecrackerBin: "firecracker",
		MicroVMRoot:    filepath.Join(tmpDir, "microvms"),
		KernelImage:    kernelPath,
		DefaultCPUs:    1,
		DefaultMemMiB:  256,
		Debug:          false,
	}

	fc, err := NewFirecracker(cfg)
	if err != nil {
		t.Fatalf("NewFirecracker() failed: %v", err)
	}
	defer fc.Close()

	// Create test bundle
	bundlePath := filepath.Join(tmpDir, "bundle")
	if err := createTestBundle(t, bundlePath); err != nil {
		t.Fatalf("createTestBundle() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Start multiple VMs
	numVMs := 3
	for i := 0; i < numVMs; i++ {
		containerID := fmt.Sprintf("test-vm-%d-%d", i, time.Now().Unix())
		
		go func(id string) {
			opts := &RunOpts{}
			_, err := fc.Run(ctx, id, bundlePath, opts)
			if err != nil && err != context.Canceled {
				t.Logf("VM %s failed: %v", id, err)
			}
		}(containerID)
	}

	// Let VMs run for a bit
	time.Sleep(5 * time.Second)

	// Cleanup all VMs
	fc.vms.Range(func(key, value interface{}) bool {
		containerID := key.(string)
		fc.Kill(ctx, containerID, syscall.SIGKILL, &KillOpts{All: true})
		fc.Delete(ctx, containerID, &DeleteOpts{Force: true})
		return true
	})
}

// TestFirecrackerExec tests the exec functionality
func TestFirecrackerExec(t *testing.T) {
	// Skip if not running as root
	if os.Geteuid() != 0 {
		t.Skip("Integration tests require root privileges")
	}

	// Check if firecracker is available
	if _, err := exec.LookPath("firecracker"); err != nil {
		t.Skip("firecracker binary not found in PATH")
	}

	tmpDir := t.TempDir()
	kernelPath := os.Getenv("FIRECRACKER_KERNEL")
	if kernelPath == "" {
		t.Skip("FIRECRACKER_KERNEL environment variable not set")
	}

	cfg := Config{
		Type:           "firecracker",
		FirecrackerBin: "firecracker",
		MicroVMRoot:    filepath.Join(tmpDir, "microvms"),
		KernelImage:    kernelPath,
		DefaultCPUs:    1,
		DefaultMemMiB:  256,
		Debug:          true,
	}

	fc, err := NewFirecracker(cfg)
	if err != nil {
		t.Fatalf("NewFirecracker() failed: %v", err)
	}
	defer fc.Close()

	bundlePath := filepath.Join(tmpDir, "bundle")
	if err := createTestBundle(t, bundlePath); err != nil {
		t.Fatalf("createTestBundle() failed: %v", err)
	}

	containerID := "test-exec-" + time.Now().Format("20060102-150405")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start container
	started := make(chan int, 1)
	go func() {
		fc.Run(ctx, containerID, bundlePath, &RunOpts{Started: started})
	}()

	// Wait for start
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for container to start")
	}

	// Test exec
	execProc := specs.Process{
		Args: []string{"/bin/echo", "Hello from exec"},
		Env:  []string{"PATH=/bin:/usr/bin"},
		Cwd:  "/",
	}

	execOpts := &ExecOpts{
		OutputWriter: os.Stdout,
	}

	if err := fc.Exec(ctx, containerID, execProc, execOpts); err != nil {
		t.Errorf("Exec() failed: %v", err)
	}

	// Cleanup
	fc.Kill(ctx, containerID, syscall.SIGKILL, &KillOpts{All: true})
	fc.Delete(ctx, containerID, &DeleteOpts{Force: true})
}

// Helper function to create a test bundle
func createTestBundle(t *testing.T, bundlePath string) error {
	// Create bundle directory structure
	if err := os.MkdirAll(bundlePath, 0755); err != nil {
		return err
	}

	rootfsPath := filepath.Join(bundlePath, "rootfs")
	if err := os.MkdirAll(rootfsPath, 0755); err != nil {
		return err
	}

	// Create minimal rootfs structure
	dirs := []string{
		"bin", "sbin", "usr/bin", "usr/sbin",
		"proc", "sys", "dev", "tmp", "var", "etc",
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(filepath.Join(rootfsPath, dir), 0755); err != nil {
			return err
		}
	}

	// Copy essential binaries (if available)
	copyIfExists := func(src, dst string) {
		if _, err := os.Stat(src); err == nil {
			exec.Command("cp", src, dst).Run()
		}
	}
	copyIfExists("/bin/sh", filepath.Join(rootfsPath, "bin/sh"))
	copyIfExists("/bin/echo", filepath.Join(rootfsPath, "bin/echo"))
	copyIfExists("/bin/sleep", filepath.Join(rootfsPath, "bin/sleep"))

	// Create a minimal OCI spec
	spec := &specs.Spec{
		Version: "1.0.0",
		Process: &specs.Process{
			Args: []string{"/bin/sleep", "30"},
			Env: []string{
				"PATH=/bin:/usr/bin:/sbin:/usr/sbin",
				"TERM=xterm",
			},
			Cwd: "/",
		},
		Root: &specs.Root{
			Path:     "rootfs",
			Readonly: false,
		},
		Hostname: "test-vm",
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs"},
		},
		Linux: &specs.Linux{},
	}

	// Write config.json
	specData, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}

	configPath := filepath.Join(bundlePath, "config.json")
	if err := os.WriteFile(configPath, specData, 0644); err != nil {
		return err
	}

	return nil
}
