package types

import "testing"

func TestMarketplaceContainerRuntimeForGPU(t *testing.T) {
	tests := []struct {
		gpu  string
		want string
	}{
		{gpu: "A10G", want: ContainerRuntimeGvisor.String()},
		{gpu: "NVIDIA A100 80GB PCI", want: ContainerRuntimeGvisor.String()},
		{gpu: "L4", want: ContainerRuntimeGvisor.String()},
		{gpu: "H100", want: ContainerRuntimeGvisor.String()},
		{gpu: "Tesla T4", want: ContainerRuntimeGvisor.String()},
		{gpu: "Tesla V100-SXM2-16GB", want: ContainerRuntimeRunc.String()},
		{gpu: "RTX5090", want: ContainerRuntimeRunc.String()},
	}

	for _, test := range tests {
		if got := MarketplaceContainerRuntimeForGPU(test.gpu); got != test.want {
			t.Fatalf("MarketplaceContainerRuntimeForGPU(%q) = %q, want %q", test.gpu, got, test.want)
		}
	}
}
