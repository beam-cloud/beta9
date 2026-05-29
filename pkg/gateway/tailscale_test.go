package gateway

import "testing"

func TestSanitizedGatewayTailscaleHostname(t *testing.T) {
	tests := []struct {
		name string
		seed string
		want string
	}{
		{
			name: "pod name",
			seed: "beta9-gateway-6f9d7c7f8c-x2abc",
			want: "beam-gateway-beta9-gateway-6f9d7c7f8c-x2abc",
		},
		{
			name: "invalid characters",
			seed: "Gateway_Pod.1",
			want: "beam-gateway-gateway-pod-1",
		},
		{
			name: "empty",
			seed: "   ",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizedGatewayTailscaleHostname(tt.seed); got != tt.want {
				t.Fatalf("sanitizedGatewayTailscaleHostname() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSanitizedGatewayTailscaleHostnameTruncatesToDNSLabel(t *testing.T) {
	got := sanitizedGatewayTailscaleHostname("beta9-gateway-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if len(got) > 63 {
		t.Fatalf("hostname length = %d, want <= 63", len(got))
	}
}
