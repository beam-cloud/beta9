#!/usr/bin/env bash

set -e

# Firecracker Local Test Harness
# This script sets up and tests the Firecracker runtime locally

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEST_DIR="${PROJECT_ROOT}/tmp/firecracker-test"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up test environment..."
    if [ -d "$TEST_DIR" ]; then
        sudo rm -rf "$TEST_DIR" 2>/dev/null || true
    fi
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if running on Linux
    if [ "$(uname)" != "Linux" ]; then
        log_error "This test harness only runs on Linux"
        exit 1
    fi
    
    # Check for KVM support
    if [ ! -e /dev/kvm ]; then
        log_error "/dev/kvm not found - KVM virtualization not available"
        log_info "You may need to enable virtualization in BIOS"
        exit 1
    fi
    
    # Check if running as root
    if [ "$EUID" -ne 0 ]; then
        log_error "This script must be run as root (for KVM and TAP device access)"
        log_info "Try: sudo $0"
        exit 1
    fi
    
    # Check for required tools
    local missing_tools=()
    for tool in curl tar go mkfs.ext4 mksquashfs truncate ip; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        log_info "Install with: apt-get install -y curl tar golang e2fsprogs iproute2 squashfs-tools"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

setup_firecracker() {
    log_info "Setting up Firecracker..."
    
    local fc_bin="$TEST_DIR/bin/firecracker"
    
    if [ -f "$fc_bin" ]; then
        log_info "Firecracker already installed at $fc_bin"
        return 0
    fi
    
    mkdir -p "$TEST_DIR/bin"
    
    # Detect architecture
    local arch="$(uname -m)"
    if [ "$arch" = "x86_64" ]; then
        FC_ARCH="x86_64"
    elif [ "$arch" = "aarch64" ]; then
        FC_ARCH="aarch64"
    else
        log_error "Unsupported architecture: $arch"
        exit 1
    fi
    
    # Download Firecracker
    local fc_version="v1.13.1"
    local release_url="https://github.com/firecracker-microvm/firecracker/releases"
    
    log_info "Downloading Firecracker ${fc_version} for ${FC_ARCH}..."
    curl -fsSL "${release_url}/download/${fc_version}/firecracker-${fc_version}-${FC_ARCH}.tgz" -o "$TEST_DIR/firecracker.tgz"
    
    tar -xzf "$TEST_DIR/firecracker.tgz" -C "$TEST_DIR"
    mv "$TEST_DIR/release-${fc_version}-${FC_ARCH}/firecracker-${fc_version}-${FC_ARCH}" "$fc_bin"
    chmod +x "$fc_bin"
    rm -rf "$TEST_DIR/release-${fc_version}-${FC_ARCH}" "$TEST_DIR/firecracker.tgz"
    
    log_success "Firecracker installed to $fc_bin"
    "$fc_bin" --version
}

setup_kernel() {
    log_info "Setting up kernel image..."
    
    local kernel_path="$TEST_DIR/vmlinux"
    
    if [ -f "$kernel_path" ]; then
        log_info "Kernel already exists at $kernel_path"
        return 0
    fi
    
    # Detect architecture
    local arch="$(uname -m)"
    if [ "$arch" = "x86_64" ]; then
        FC_ARCH="x86_64"
    elif [ "$arch" = "aarch64" ]; then
        FC_ARCH="aarch64"
    else
        log_error "Unsupported architecture: $arch"
        exit 1
    fi
    
    log_info "Downloading kernel image for ${FC_ARCH}..."
    curl -fsSL "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.10/${FC_ARCH}/vmlinux-5.10.223" -o "$kernel_path"
    chmod 644 "$kernel_path"
    
    log_success "Kernel downloaded to $kernel_path"
}

build_vm_init() {
    log_info "Building beta9-vm-init..."
    
    cd "$PROJECT_ROOT"
    
    CGO_ENABLED=0 GOOS=linux go build \
        -ldflags="-s -w -extldflags=-static" \
        -o "$TEST_DIR/bin/beta9-vm-init" \
        ./cmd/vm-init/main.go
    
    chmod +x "$TEST_DIR/bin/beta9-vm-init"
    
    log_success "beta9-vm-init built successfully"
}

create_test_bundle() {
    log_info "Creating test OCI bundle..."
    
    local bundle_dir="$TEST_DIR/test-bundle"
    local rootfs_dir="$bundle_dir/rootfs"
    
    mkdir -p "$rootfs_dir"/{bin,sbin,usr/bin,proc,sys,dev,tmp,etc}
    
    # Copy essential binaries
    if [ -f /bin/sh ]; then
        cp /bin/sh "$rootfs_dir/bin/"
    fi
    if [ -f /bin/sleep ]; then
        cp /bin/sleep "$rootfs_dir/bin/"
    fi
    if [ -f /bin/echo ]; then
        cp /bin/echo "$rootfs_dir/bin/"
    fi
    
    # Copy vm-init
    cp "$TEST_DIR/bin/beta9-vm-init" "$rootfs_dir/sbin/"
    
    # Create OCI config.json
    cat > "$bundle_dir/config.json" <<'EOF'
{
    "ociVersion": "1.0.0",
    "process": {
        "terminal": false,
        "user": {
            "uid": 0,
            "gid": 0
        },
        "args": [
            "/bin/sleep",
            "30"
        ],
        "env": [
            "PATH=/bin:/usr/bin:/sbin:/usr/sbin",
            "TERM=xterm"
        ],
        "cwd": "/",
        "capabilities": {
            "bounding": [
                "CAP_AUDIT_WRITE",
                "CAP_KILL",
                "CAP_NET_BIND_SERVICE"
            ],
            "effective": [
                "CAP_AUDIT_WRITE",
                "CAP_KILL",
                "CAP_NET_BIND_SERVICE"
            ],
            "inheritable": [
                "CAP_AUDIT_WRITE",
                "CAP_KILL",
                "CAP_NET_BIND_SERVICE"
            ],
            "permitted": [
                "CAP_AUDIT_WRITE",
                "CAP_KILL",
                "CAP_NET_BIND_SERVICE"
            ],
            "ambient": [
                "CAP_AUDIT_WRITE",
                "CAP_KILL",
                "CAP_NET_BIND_SERVICE"
            ]
        },
        "rlimits": [
            {
                "type": "RLIMIT_NOFILE",
                "hard": 1024,
                "soft": 1024
            }
        ],
        "noNewPrivileges": true
    },
    "root": {
        "path": "rootfs",
        "readonly": false
    },
    "hostname": "test-firecracker",
    "mounts": [
        {
            "destination": "/proc",
            "type": "proc",
            "source": "proc"
        },
        {
            "destination": "/dev",
            "type": "tmpfs",
            "source": "tmpfs",
            "options": [
                "nosuid",
                "strictatime",
                "mode=755",
                "size=65536k"
            ]
        },
        {
            "destination": "/sys",
            "type": "sysfs",
            "source": "sysfs",
            "options": [
                "nosuid",
                "noexec",
                "nodev",
                "ro"
            ]
        }
    ],
    "linux": {
        "resources": {
            "memory": {
                "limit": 536870912
            },
            "cpu": {
                "quota": 100000,
                "period": 100000
            }
        },
        "namespaces": [
            {
                "type": "pid"
            },
            {
                "type": "network"
            },
            {
                "type": "ipc"
            },
            {
                "type": "uts"
            },
            {
                "type": "mount"
            }
        ]
    }
}
EOF
    
    log_success "Test bundle created at $bundle_dir"
}

run_unit_tests() {
    log_info "Running unit tests..."
    
    cd "$PROJECT_ROOT"
    
    export PATH="$TEST_DIR/bin:$PATH"
    export FIRECRACKER_KERNEL="$TEST_DIR/vmlinux"
    
    go test -v ./pkg/runtime -run TestFirecracker
    
    log_success "Unit tests passed"
}

run_integration_test() {
    log_info "Running integration test..."
    
    cd "$PROJECT_ROOT"
    
    export PATH="$TEST_DIR/bin:$PATH"
    export FIRECRACKER_KERNEL="$TEST_DIR/vmlinux"
    
    # Run integration tests
    go test -v -tags=integration ./pkg/runtime -run TestFirecrackerIntegration -timeout 5m
    
    log_success "Integration tests passed"
}

run_manual_test() {
    log_info "Running manual test with test harness..."
    
    cd "$PROJECT_ROOT"
    
    # Create a simple Go test program
    cat > "$TEST_DIR/test_runner.go" <<'EOF'
package main

import (
    "context"
    "fmt"
    "os"
    "time"
    "syscall"
    
    "github.com/beam-cloud/beta9/pkg/runtime"
)

func main() {
    // Configuration
    cfg := runtime.Config{
        Type:           "firecracker",
        FirecrackerBin: os.Getenv("FIRECRACKER_BIN"),
        KernelImage:    os.Getenv("FIRECRACKER_KERNEL"),
        MicroVMRoot:    os.Getenv("TEST_DIR") + "/microvms",
        DefaultCPUs:    1,
        DefaultMemMiB:  256,
        Debug:          true,
    }
    
    fmt.Println("Creating Firecracker runtime...")
    rt, err := runtime.New(cfg)
    if err != nil {
        fmt.Printf("Failed to create runtime: %v\n", err)
        os.Exit(1)
    }
    defer rt.Close()
    
    fmt.Println("Runtime created successfully")
    fmt.Printf("Runtime name: %s\n", rt.Name())
    
    // Run a container
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    
    containerID := "test-manual-" + fmt.Sprintf("%d", time.Now().Unix())
    bundlePath := os.Getenv("TEST_DIR") + "/test-bundle"
    
    started := make(chan int, 1)
    opts := &runtime.RunOpts{
        Started: started,
    }
    
    fmt.Printf("Starting microVM %s...\n", containerID)
    
    // Run in goroutine
    errCh := make(chan error, 1)
    go func() {
        _, err := rt.Run(ctx, containerID, bundlePath, opts)
        errCh <- err
    }()
    
    // Wait for start
    select {
    case pid := <-started:
        fmt.Printf("✓ MicroVM started with PID %d\n", pid)
    case err := <-errCh:
        fmt.Printf("✗ Failed to start: %v\n", err)
        os.Exit(1)
    case <-time.After(30 * time.Second):
        fmt.Println("✗ Timeout waiting for start")
        os.Exit(1)
    }
    
    // Check state
    state, err := rt.State(ctx, containerID)
    if err != nil {
        fmt.Printf("Failed to get state: %v\n", err)
    } else {
        fmt.Printf("Container state: ID=%s, Status=%s, PID=%d\n", state.ID, state.Status, state.Pid)
    }
    
    // Let it run for a bit
    fmt.Println("Letting microVM run for 5 seconds...")
    time.Sleep(5 * time.Second)
    
    // Kill
    fmt.Println("Killing microVM...")
    if err := rt.Kill(ctx, containerID, syscall.SIGKILL, &runtime.KillOpts{All: true}); err != nil {
        fmt.Printf("Failed to kill: %v\n", err)
    } else {
        fmt.Println("✓ MicroVM killed")
    }
    
    // Wait for exit
    select {
    case err := <-errCh:
        if err != nil && err != context.Canceled {
            fmt.Printf("Run exited with error: %v\n", err)
        }
    case <-time.After(5 * time.Second):
        fmt.Println("Timeout waiting for exit")
    }
    
    // Delete
    fmt.Println("Deleting microVM...")
    if err := rt.Delete(ctx, containerID, &runtime.DeleteOpts{Force: true}); err != nil {
        fmt.Printf("Failed to delete: %v\n", err)
    } else {
        fmt.Println("✓ MicroVM deleted")
    }
    
    fmt.Println("\n✓ Manual test completed successfully!")
}
EOF
    
    # Set environment variables
    export FIRECRACKER_BIN="$TEST_DIR/bin/firecracker"
    export FIRECRACKER_KERNEL="$TEST_DIR/vmlinux"
    export TEST_DIR="$TEST_DIR"
    export PATH="$TEST_DIR/bin:$PATH"
    
    # Build and run
    go run "$TEST_DIR/test_runner.go"
    
    log_success "Manual test passed"
}

show_usage() {
    cat <<EOF
Firecracker Local Test Harness

Usage: $0 [OPTIONS] [COMMAND]

Commands:
    setup       Set up test environment (Firecracker, kernel, etc.)
    unit        Run unit tests
    integration Run integration tests
    manual      Run manual end-to-end test
    all         Run all tests (default)
    clean       Clean up test environment

Options:
    -h, --help  Show this help message

Examples:
    sudo $0 setup      # Set up test environment
    sudo $0 unit       # Run unit tests
    sudo $0 all        # Run all tests
    sudo $0 clean      # Clean up

EOF
}

main() {
    local command="${1:-all}"
    
    case "$command" in
        -h|--help)
            show_usage
            exit 0
            ;;
        clean)
            cleanup
            log_success "Cleanup complete"
            exit 0
            ;;
    esac
    
    # Check prerequisites
    check_prerequisites
    
    case "$command" in
        setup)
            setup_firecracker
            setup_kernel
            build_vm_init
            create_test_bundle
            log_success "Setup complete! Test environment ready at $TEST_DIR"
            ;;
        unit)
            setup_firecracker
            setup_kernel
            build_vm_init
            run_unit_tests
            ;;
        integration)
            setup_firecracker
            setup_kernel
            build_vm_init
            create_test_bundle
            run_integration_test
            ;;
        manual)
            setup_firecracker
            setup_kernel
            build_vm_init
            create_test_bundle
            run_manual_test
            ;;
        all)
            setup_firecracker
            setup_kernel
            build_vm_init
            create_test_bundle
            run_unit_tests
            run_integration_test
            run_manual_test
            log_success "All tests passed! 🎉"
            ;;
        *)
            log_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

# Handle Ctrl+C
trap cleanup EXIT INT TERM

main "$@"
