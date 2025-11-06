package runtime

import (
	"fmt"
	"os/exec"
	"strings"
	
	"github.com/opencontainers/runtime-spec/specs-go"
)

// setupNetworking creates a TAP device for the microVM
// It integrates with beta9's ContainerNetworkManager by using the existing network namespace
func (f *Firecracker) setupNetworking(vm *vmState, spec *specs.Spec) error {
	// Generate TAP device name based on container ID
	// Use truncated ID to keep under 15 char interface name limit
	truncatedID := vm.ID
	if len(vm.ID) > 8 {
		truncatedID = vm.ID[len(vm.ID)-8:]
	}
	tapName := fmt.Sprintf("b9fc_%s", truncatedID)
	if len(tapName) > 15 {
		tapName = tapName[:15]
	}

	// Extract the network namespace path from the spec
	// The ContainerNetworkManager creates this namespace and updates the spec
	var netnsPath string
	if spec != nil && spec.Linux != nil {
		for _, ns := range spec.Linux.Namespaces {
			if ns.Type == specs.NetworkNamespace && ns.Path != "" {
				netnsPath = ns.Path
				break
			}
		}
	}

	// Create TAP device in the container's network namespace (created by ContainerNetworkManager)
	// This allows the microVM to use the same network setup as containers
	if netnsPath != "" {
		// Create TAP in the container's network namespace
		if err := f.createTapInNamespace(tapName, netnsPath); err != nil {
			return fmt.Errorf("failed to create TAP device in namespace: %w", err)
		}
	} else {
		// Fallback: create TAP in host namespace if no netns specified
		if err := f.createTap(tapName); err != nil {
			return fmt.Errorf("failed to create TAP device: %w", err)
		}
	}

	vm.TapDevice = tapName
	vm.NetnsPath = netnsPath

	// Bring up the TAP device
	if netnsPath != "" {
		if err := f.bringUpInterfaceInNamespace(tapName, netnsPath); err != nil {
			f.deleteTapInNamespace(tapName, netnsPath)
			return fmt.Errorf("failed to bring up TAP device: %w", err)
		}
	} else {
		if err := f.bringUpInterface(tapName); err != nil {
			f.deleteTap(tapName)
			return fmt.Errorf("failed to bring up TAP device: %w", err)
		}
	}

	return nil
}

// createTap creates a TAP network device in the host namespace
func (f *Firecracker) createTap(name string) error {
	// Use ip tuntap to create TAP device
	cmd := exec.Command("ip", "tuntap", "add", "dev", name, "mode", "tap")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip tuntap add failed: %w: %s", err, string(output))
	}

	return nil
}

// createTapInNamespace creates a TAP device in a specific network namespace
func (f *Firecracker) createTapInNamespace(name, netnsPath string) error {
	// Use ip netns exec to create TAP in the specified namespace
	cmd := exec.Command("ip", "netns", "exec", extractNetnsName(netnsPath), "ip", "tuntap", "add", "dev", name, "mode", "tap")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip tuntap add in namespace failed: %w: %s", err, string(output))
	}

	return nil
}

// deleteTap removes a TAP network device from host namespace
func (f *Firecracker) deleteTap(name string) error {
	cmd := exec.Command("ip", "link", "delete", name)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip link delete failed: %w: %s", err, string(output))
	}

	return nil
}

// deleteTapInNamespace removes a TAP device from a specific network namespace
func (f *Firecracker) deleteTapInNamespace(name, netnsPath string) error {
	cmd := exec.Command("ip", "netns", "exec", extractNetnsName(netnsPath), "ip", "link", "delete", name)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip link delete in namespace failed: %w: %s", err, string(output))
	}

	return nil
}

// bringUpInterface brings up a network interface in host namespace
func (f *Firecracker) bringUpInterface(name string) error {
	cmd := exec.Command("ip", "link", "set", name, "up")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip link set up failed: %w: %s", err, string(output))
	}

	return nil
}

// bringUpInterfaceInNamespace brings up a network interface in a specific namespace
func (f *Firecracker) bringUpInterfaceInNamespace(name, netnsPath string) error {
	cmd := exec.Command("ip", "netns", "exec", extractNetnsName(netnsPath), "ip", "link", "set", name, "up")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip link set up in namespace failed: %w: %s", err, string(output))
	}

	return nil
}

// extractNetnsName extracts the namespace name from a path like /var/run/netns/containerId
func extractNetnsName(netnsPath string) string {
	// netnsPath is typically /var/run/netns/<name>
	// Extract just the name part
	parts := strings.Split(netnsPath, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return netnsPath
}

// attachToBridge attaches a TAP device to a bridge (optional)
// This can be used if the worker pod uses a bridge for networking
func (f *Firecracker) attachToBridge(tapName, bridgeName string) error {
	// Check if bridge exists
	cmd := exec.Command("ip", "link", "show", bridgeName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bridge %s does not exist", bridgeName)
	}

	// Attach TAP to bridge
	cmd = exec.Command("ip", "link", "set", tapName, "master", bridgeName)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to attach to bridge: %w: %s", err, string(output))
	}

	return nil
}

// getMACAddress generates a MAC address for the VM
// Using a locally administered MAC address (second least significant bit of first octet is 1)
func (f *Firecracker) getMACAddress(containerID string) string {
	// Generate a stable MAC based on container ID
	// Format: 02:XX:XX:XX:XX:XX (02 indicates locally administered)
	mac := fmt.Sprintf("02:%s:%s:%s:%s:%s",
		containerID[0:2],
		containerID[2:4],
		containerID[4:6],
		containerID[6:8],
		containerID[8:10],
	)
	return strings.ToLower(mac)
}

// updateFirecrackerConfigWithNetwork adds network configuration to Firecracker config
func (f *Firecracker) updateFirecrackerConfigWithNetwork(config map[string]interface{}, vm *vmState) {
	if vm.TapDevice == "" {
		return
	}

	// Add network interface to Firecracker config
	networkInterfaces := []map[string]interface{}{
		{
			"iface_id":             "eth0",
			"host_dev_name":        vm.TapDevice,
			"guest_mac":            f.getMACAddress(vm.ID),
		},
	}

	config["network-interfaces"] = networkInterfaces
}
