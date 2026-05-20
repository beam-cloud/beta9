package worker

import (
	"fmt"
	"net"
	"os"
	"testing"
)

func TestGetIPFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		envName   string
		envValue  string
		want      string
		expectErr bool
	}{
		{
			name:      "No IP set",
			envName:   "EMPTY_IP",
			envValue:  "",
			want:      "",
			expectErr: true,
		},
		{
			name:      "Invalid IP",
			envName:   "INVALID_IP",
			envValue:  "invalid",
			want:      "",
			expectErr: true,
		},
		{
			name:      "Valid IPv4",
			envName:   "VALID_IPV4",
			envValue:  "192.168.0.1",
			want:      "192.168.0.1",
			expectErr: false,
		},
		{
			name:      "Valid IPv6",
			envName:   "VALID_IPV6",
			envValue:  "2001:db8::1",
			want:      "[2001:db8::1]",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv(tt.envName, tt.envValue)
			defer os.Unsetenv(tt.envName)

			got, err := getIPFromEnv(tt.envName)

			if (err != nil) != tt.expectErr {
				t.Errorf("getIPFromEnv() error = %v, expectErr %v", err, tt.expectErr)
				return
			}

			if got != tt.want {
				t.Errorf("getIPFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAssignIpInRange(t *testing.T) {
	allocated := map[string]bool{}

	iterations := 20
	rangeStart := uint8(128)
	rangeEnd := uint8(rangeStart + uint8(iterations))

	results := map[string]bool{}
	for i := 0; i < iterations; i++ {
		addr, err := assignIpInRange(allocated, rangeStart, rangeEnd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ip := addr.IPNet.IP.String()
		if ip == containerBridgeAddress || ip == containerNetworkAddress() {
			t.Errorf("reserved IP was returned: %s", ip)
		}

		last := addr.IPNet.IP.To4()[3]
		if last < rangeStart || last >= rangeEnd {
			t.Errorf("IP out of range: %s", ip)
		}

		results[ip] = true
		allocated[ip] = true
	}

	// Should be able to get all unallocated IPs in the range
	if len(results) != iterations {
		t.Errorf("expected %d unique IPs, got %d: %v", iterations, len(results), results)
	}

	// Now allocate all, should error
	for i := rangeStart; i < rangeEnd; i++ {
		allocated[fmt.Sprintf("192.168.0.%d", i)] = true
	}

	_, err := assignIpInRange(allocated, rangeStart, rangeEnd)
	if err == nil {
		t.Errorf("expected error when all IPs are allocated, got nil")
	}
}

func TestContainerVethNamesAvoidBurstCollisions(t *testing.T) {
	firstHost, firstContainer := containerVethNames("sandbox-a-abcde")
	secondHost, secondContainer := containerVethNames("sandbox-b-abcde")

	if firstHost == secondHost || firstContainer == secondContainer {
		t.Fatalf("veth names should not collide for ids with the same suffix: %s/%s", firstHost, secondHost)
	}
	if len(firstHost) > networkInterfaceNameMaxLength || len(firstContainer) > networkInterfaceNameMaxLength {
		t.Fatalf("veth names exceed Linux interface limit: %s/%s", firstHost, firstContainer)
	}
}

func TestContainerSubnetSupportsThousandContainerBurst(t *testing.T) {
	_, ipNet, err := net.ParseCIDR(containerSubnet)
	if err != nil {
		t.Fatalf("invalid container subnet: %v", err)
	}

	usable := 0
	for ip := ipNet.IP.Mask(ipNet.Mask); ipNet.Contains(ip); ip = nextIP(ip, 1) {
		ipStr := ip.String()
		if ipStr == containerBridgeAddress || ipStr == ipNet.IP.String() {
			continue
		}
		usable++
		if usable >= 1000 {
			return
		}
	}

	t.Fatalf("container subnet only has %d usable addresses, need at least 1000", usable)
}

func TestContainerIPv6AddressUsesFullIPv4HostOffset(t *testing.T) {
	_, ipv6Net, err := net.ParseCIDR(containerSubnetIPv6)
	if err != nil {
		t.Fatalf("invalid IPv6 subnet: %v", err)
	}

	first, err := containerIPv6Address(net.ParseIP("192.168.0.2"), ipv6Net)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	later, err := containerIPv6Address(net.ParseIP("192.168.4.2"), ipv6Net)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first.Equal(later) {
		t.Fatalf("IPv6 addresses should not collide across IPv4 /20 host offsets: %s", first)
	}
}

func TestContainerIdFromIptablesRuleHandlesIPv6Colons(t *testing.T) {
	rule := `-A PREROUTING -p tcp -m tcp --dport 12345 -j DNAT --to-destination [fd00:abcd::3e8]:8080 -m comment --comment "b9habcdef123456:sandbox-123"`

	containerId, ok := containerIdFromIptablesRule(rule)
	if !ok {
		t.Fatal("expected container id in iptables rule")
	}
	if containerId != "sandbox-123" {
		t.Fatalf("expected sandbox-123, got %s", containerId)
	}
}

func containerNetworkAddress() string {
	_, ipNet, _ := net.ParseCIDR(containerSubnet)
	return ipNet.IP.String()
}
