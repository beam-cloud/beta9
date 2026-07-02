package clients

import "testing"

func TestMarketplaceUsageEndpointDerivesFromManagedComputeEndpoint(t *testing.T) {
	tests := map[string]string{
		"https://api.stage.beam.cloud/v2/payment/managed-compute/":       "https://api.stage.beam.cloud/v2/payment/marketplace/usage/",
		"https://api.stage.beam.cloud/v2/payment/managed-compute/usage/": "https://api.stage.beam.cloud/v2/payment/marketplace/usage/",
		"https://api.stage.beam.cloud/v2/payment":                        "https://api.stage.beam.cloud/v2/payment/marketplace/usage/",
		// Already-normalized endpoints must be idempotent.
		"https://api.stage.beam.cloud/v2/payment/marketplace/usage/": "https://api.stage.beam.cloud/v2/payment/marketplace/usage/",
		"https://api.stage.beam.cloud/v2/payment/marketplace/usage":  "https://api.stage.beam.cloud/v2/payment/marketplace/usage/",
	}

	for endpoint, want := range tests {
		if got := marketplaceUsageEndpoint(endpoint); got != want {
			t.Fatalf("marketplaceUsageEndpoint(%q) = %q, want %q", endpoint, got, want)
		}
	}
}
