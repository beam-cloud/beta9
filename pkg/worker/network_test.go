package worker

import (
	"fmt"
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
			envValue:  "192.168.1.1",
			want:      "192.168.1.1",
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
		if ip == containerBridgeAddress || ip == "192.168.1.0" {
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
		allocated[fmt.Sprintf("192.168.1.%d", i)] = true
	}

	_, err := assignIpInRange(allocated, rangeStart, rangeEnd)
	if err == nil {
		t.Errorf("expected error when all IPs are allocated, got nil")
	}
}
