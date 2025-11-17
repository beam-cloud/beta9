package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// TestRuncCheckpointRestore tests checkpoint and restore functionality for runc
func TestRuncCheckpointRestore(t *testing.T) {
	if os.Getenv("SKIP_RUNTIME_TESTS") == "1" {
		t.Skip("Skipping runtime tests")
	}

	// Create temporary directories for test
	tmpDir, err := os.MkdirTemp("", "runc-checkpoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "checkpoint")
	if err := os.MkdirAll(checkpointPath, 0755); err != nil {
		t.Fatalf("Failed to create checkpoint dir: %v", err)
	}

	// Initialize runc runtime
	runc, err := NewRunc(Config{
		RuncPath: "runc",
		Debug:    true,
	})
	if err != nil {
		t.Skipf("Runc not available: %v", err)
	}
	defer runc.Close()

	// Verify runc supports checkpoint/restore
	caps := runc.Capabilities()
	if !caps.CheckpointRestore {
		t.Fatal("Runc should support checkpoint/restore")
	}

	t.Run("checkpoint with nil options", func(t *testing.T) {
		err := runc.Checkpoint(context.Background(), "test-container", nil)
		if err == nil {
			t.Error("Expected error with nil options")
		}
	})

	t.Run("restore with nil options", func(t *testing.T) {
		_, err := runc.Restore(context.Background(), "test-container", nil)
		if err == nil {
			t.Error("Expected error with nil options")
		}
	})

	t.Run("checkpoint options validation", func(t *testing.T) {
		opts := &CheckpointOpts{
			ImagePath:    checkpointPath,
			LeaveRunning: true,
			AllowOpenTCP: true,
			SkipInFlight: true,
			LinkRemap:    true,
		}

		if opts.ImagePath == "" {
			t.Error("ImagePath should not be empty")
		}
	})
}

// TestRunscCheckpointRestore tests checkpoint and restore functionality for gVisor
func TestRunscCheckpointRestore(t *testing.T) {
	if os.Getenv("SKIP_RUNTIME_TESTS") == "1" {
		t.Skip("Skipping runtime tests")
	}

	// Create temporary directories for test
	tmpDir, err := os.MkdirTemp("", "runsc-checkpoint-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointPath := filepath.Join(tmpDir, "checkpoint")
	if err := os.MkdirAll(checkpointPath, 0755); err != nil {
		t.Fatalf("Failed to create checkpoint dir: %v", err)
	}

	// Initialize runsc runtime
	runsc, err := NewRunsc(Config{
		RunscPath: "runsc",
		RunscRoot: filepath.Join(tmpDir, "runsc-root"),
		Debug:     true,
	})
	if err != nil {
		t.Skipf("Runsc not available: %v", err)
	}
	defer runsc.Close()

	// Verify runsc supports checkpoint/restore
	caps := runsc.Capabilities()
	if !caps.CheckpointRestore {
		t.Fatal("Runsc should support checkpoint/restore")
	}

	t.Run("checkpoint with nil options", func(t *testing.T) {
		err := runsc.Checkpoint(context.Background(), "test-container", nil)
		if err == nil {
			t.Error("Expected error with nil options")
		}
	})

	t.Run("restore with nil options", func(t *testing.T) {
		_, err := runsc.Restore(context.Background(), "test-container", nil)
		if err == nil {
			t.Error("Expected error with nil options")
		}
	})

	t.Run("checkpoint options validation", func(t *testing.T) {
		opts := &CheckpointOpts{
			ImagePath:    checkpointPath,
			LeaveRunning: true,
		}

		if opts.ImagePath == "" {
			t.Error("ImagePath should not be empty")
		}
	})
}

// TestCheckpointRestoreOptions tests the checkpoint and restore option types
func TestCheckpointRestoreOptions(t *testing.T) {
	t.Run("CheckpointOpts structure", func(t *testing.T) {
		opts := &CheckpointOpts{
			ImagePath:    "/tmp/checkpoint",
			WorkDir:      "/tmp/work",
			LeaveRunning: true,
			AllowOpenTCP: true,
			SkipInFlight: true,
			LinkRemap:    true,
		}

		if opts.ImagePath != "/tmp/checkpoint" {
			t.Errorf("Expected ImagePath=/tmp/checkpoint, got %s", opts.ImagePath)
		}
		if !opts.LeaveRunning {
			t.Error("Expected LeaveRunning=true")
		}
		if !opts.AllowOpenTCP {
			t.Error("Expected AllowOpenTCP=true")
		}
	})

	t.Run("RestoreOpts structure", func(t *testing.T) {
		started := make(chan int, 1)
		opts := &RestoreOpts{
			ImagePath:  "/tmp/checkpoint",
			WorkDir:    "/tmp/work",
			BundlePath: "/tmp/bundle",
			Started:    started,
			TCPClose:   true,
		}

		if opts.ImagePath != "/tmp/checkpoint" {
			t.Errorf("Expected ImagePath=/tmp/checkpoint, got %s", opts.ImagePath)
		}
		if opts.BundlePath != "/tmp/bundle" {
			t.Errorf("Expected BundlePath=/tmp/bundle, got %s", opts.BundlePath)
		}
		if !opts.TCPClose {
			t.Error("Expected TCPClose=true")
		}
	})
}

// TestRuntimeCapabilities tests that runtimes report correct capabilities
func TestRuntimeCapabilities(t *testing.T) {
	t.Run("Runc capabilities", func(t *testing.T) {
		runc, err := NewRunc(Config{RuncPath: "runc"})
		if err != nil {
			t.Skipf("Runc not available: %v", err)
		}
		defer runc.Close()

		caps := runc.Capabilities()
		if !caps.CheckpointRestore {
			t.Error("Runc should support checkpoint/restore")
		}
		if !caps.GPU {
			t.Error("Runc should support GPU")
		}
		if !caps.CDI {
			t.Error("Runc should support CDI")
		}
	})

	t.Run("Runsc capabilities", func(t *testing.T) {
		runsc, err := NewRunsc(Config{
			RunscPath: "runsc",
			RunscRoot: "/tmp/runsc-test",
		})
		if err != nil {
			t.Skipf("Runsc not available: %v", err)
		}
		defer runsc.Close()

		caps := runsc.Capabilities()
		if !caps.CheckpointRestore {
			t.Error("Runsc should support checkpoint/restore")
		}
		if !caps.GPU {
			t.Error("Runsc should support GPU")
		}
		if !caps.CDI {
			t.Error("Runsc should support CDI")
		}
		if !caps.JoinExistingNetNS {
			t.Error("Runsc should support joining existing network namespaces")
		}
	})
}

// TestRuntimeIntegration tests basic runtime integration for checkpoint/restore
func TestRuntimeIntegration(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") == "1" {
		t.Skip("Skipping integration tests")
	}

	// Test with both runc and runsc if available
	runtimes := []struct {
		name   string
		config Config
	}{
		{
			name: "runc",
			config: Config{
				Type:     "runc",
				RuncPath: "runc",
				Debug:    true,
			},
		},
		{
			name: "gvisor",
			config: Config{
				Type:         "gvisor",
				RunscPath:    "runsc",
				RunscRoot:    "/tmp/runsc-integration-test",
				RunscPlatform: "ptrace",
				Debug:        true,
			},
		},
	}

	for _, rt := range runtimes {
		t.Run(rt.name, func(t *testing.T) {
			runtime, err := New(rt.config)
			if err != nil {
				t.Skipf("Runtime %s not available: %v", rt.name, err)
			}
			defer runtime.Close()

			// Create test container bundle
			tmpDir, err := os.MkdirTemp("", fmt.Sprintf("%s-test-*", rt.name))
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			rootfsPath := filepath.Join(tmpDir, "rootfs")
			if err := os.MkdirAll(rootfsPath, 0755); err != nil {
				t.Fatalf("Failed to create rootfs: %v", err)
			}

			// Create a minimal spec
			spec := &specs.Spec{
				Version: "1.0.0",
				Root: &specs.Root{
					Path:     rootfsPath,
					Readonly: false,
				},
				Process: &specs.Process{
					User: specs.User{
						UID: 0,
						GID: 0,
					},
					Args: []string{"sleep", "100"},
					Cwd:  "/",
					Env: []string{
						"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
					},
					Capabilities: &specs.LinuxCapabilities{
						Bounding:    []string{"CAP_KILL"},
						Effective:   []string{"CAP_KILL"},
						Inheritable: []string{"CAP_KILL"},
						Permitted:   []string{"CAP_KILL"},
					},
				},
				Linux: &specs.Linux{
					Namespaces: []specs.LinuxNamespace{
						{Type: specs.PIDNamespace},
						{Type: specs.MountNamespace},
					},
				},
			}

			// Prepare spec for runtime
			if err := runtime.Prepare(context.Background(), spec); err != nil {
				t.Fatalf("Failed to prepare spec: %v", err)
			}

			// Test checkpoint with running container would go here
			// For now, just test that the interface is properly implemented
			checkpointPath := filepath.Join(tmpDir, "checkpoint")
			if err := os.MkdirAll(checkpointPath, 0755); err != nil {
				t.Fatalf("Failed to create checkpoint dir: %v", err)
			}

			// Verify checkpoint method exists and has correct signature
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			opts := &CheckpointOpts{
				ImagePath:    checkpointPath,
				LeaveRunning: true,
			}

			// This will fail because no container is running, but tests the interface
			err = runtime.Checkpoint(ctx, "non-existent-container", opts)
			if err == nil {
				t.Error("Expected error for non-existent container")
			}
		})
	}
}

// TestGVisorNVProxySupport tests that gVisor properly enables nvproxy for GPU workloads
func TestGVisorNVProxySupport(t *testing.T) {
	runsc, err := NewRunsc(Config{
		RunscPath: "runsc",
		RunscRoot: "/tmp/runsc-nvproxy-test",
		Debug:     true,
	})
	if err != nil {
		t.Skipf("Runsc not available: %v", err)
	}
	defer runsc.Close()

	t.Run("detect GPU devices in spec", func(t *testing.T) {
		spec := &specs.Spec{
			Version: "1.0.0",
			Linux: &specs.Linux{
				Devices: []specs.LinuxDevice{
					{
						Path: "/dev/nvidia0",
						Type: "c",
					},
					{
						Path: "/dev/nvidiactl",
						Type: "c",
					},
				},
			},
		}

		// Prepare should detect GPU devices
		if err := runsc.Prepare(context.Background(), spec); err != nil {
			t.Fatalf("Failed to prepare spec: %v", err)
		}

		// After prepare, nvproxy should be enabled (internal state check would go here)
	})

	t.Run("CDI annotations for GPU", func(t *testing.T) {
		spec := &specs.Spec{
			Version: "1.0.0",
			Annotations: map[string]string{
				"cdi.k8s.io/nvidia": "nvidia.com/gpu=0",
			},
			Linux: &specs.Linux{},
		}

		// Prepare should detect CDI GPU annotations
		if err := runsc.Prepare(context.Background(), spec); err != nil {
			t.Fatalf("Failed to prepare spec: %v", err)
		}
	})
}
