package types

import (
	"reflect"
	"testing"
)

func TestGVisorRuntimeDefaultsUseExclusiveRootfs(t *testing.T) {
	config := (RuntimeConfig{}).WithDefaults(ContainerRuntimeGvisor.String())
	want := []string{"--dcache=32768", "--overlay2=none", "--file-access=exclusive"}
	if !reflect.DeepEqual(config.GVisorExtraArgs, want) {
		t.Fatalf("gVisor extra args = %#v, want %#v", config.GVisorExtraArgs, want)
	}
}

func TestGVisorRuntimeDefaultsPreserveExplicitFileAccess(t *testing.T) {
	want := []string{"--file-access=shared"}
	config := (RuntimeConfig{GVisorExtraArgs: want}).WithDefaults(ContainerRuntimeGvisor.String())
	if !reflect.DeepEqual(config.GVisorExtraArgs, want) {
		t.Fatalf("gVisor extra args = %#v, want %#v", config.GVisorExtraArgs, want)
	}
}

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
