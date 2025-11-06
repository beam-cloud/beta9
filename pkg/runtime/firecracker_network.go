package runtime

import (
	"fmt"
	"os/exec"
	"strings"
)

// setupNetworking creates a TAP device for the microVM
// The TAP device will be in the same network namespace as the worker pod
func (f *Firecracker) setupNetworking(vm *vmState) error {
	// Generate TAP device name
	tapName := fmt.Sprintf("tap-%s", vm.ID[:8])
	if len(tapName) > 15 {
		tapName = tapName[:15] // Interface names limited to 15 chars
	}

	// Create TAP device
	if err := f.createTap(tapName); err != nil {
		return fmt.Errorf("failed to create TAP device: %w", err)
	}

	vm.TapDevice = tapName

	// Bring up the TAP device
	if err := f.bringUpInterface(tapName); err != nil {
		f.deleteTap(tapName)
		return fmt.Errorf("failed to bring up TAP device: %w", err)
	}

	return nil
}

// createTap creates a TAP network device
func (f *Firecracker) createTap(name string) error {
	// Use ip tuntap to create TAP device
	cmd := exec.Command("ip", "tuntap", "add", "dev", name, "mode", "tap")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip tuntap add failed: %w: %s", err, string(output))
	}

	return nil
}

// deleteTap removes a TAP network device
func (f *Firecracker) deleteTap(name string) error {
	cmd := exec.Command("ip", "link", "delete", name)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip link delete failed: %w: %s", err, string(output))
	}

	return nil
}

// bringUpInterface brings up a network interface
func (f *Firecracker) bringUpInterface(name string) error {
	cmd := exec.Command("ip", "link", "set", name, "up")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ip link set up failed: %w: %s", err, string(output))
	}

	return nil
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
