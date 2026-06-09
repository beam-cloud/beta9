package types

import "testing"

func TestNormalizeGPUType(t *testing.T) {
	tests := map[string]GpuType{
		"A10":                   GPU_A10,
		"A100_sxm4x8":           GPU_A100,
		"A100_80G_sxm4x8":       GPU_A100_80,
		"A16x16":                GPU_A16,
		"A30x8":                 GPU_A30,
		"A40x8":                 GPU_A40,
		"A4000":                 GPU_A4000,
		"A5000x4":               GPU_A5000,
		"A6000_plusx8":          GPU_A6000,
		"B200_sxm6x8_NVLINK":    GPU_B200,
		"B300x8":                GPU_B300,
		"Gaudi2x8":              GPU_GAUDI2,
		"GH200":                 GPU_GH200,
		"H100_nvlx8_NVLINK":     GPU_H100,
		"H200_sxm5x8_NVLINK_eu": GPU_H200,
		"L4x8":                  GPU_L4,
		"L40":                   GPU_L40,
		"L40Sx8":                GPU_L40S,
		"RTX4000Ada":            GPU_RTX4000_ADA,
		"RTX4090":               GPU_RTX4090,
		"RTX4090x8":             GPU_RTX4090,
		"RTX5090x8":             GPU_RTX5090,
		"RTX6000":               GPU_RTX6000,
		"RTX6000Adax8":          GPU_RTX6000_ADA,
		"RTXPro6000x8_es":       GPU_RTX_PRO6000,
		"V100_32Gx4":            GPU_V100_32,
		"V100x8":                GPU_V100,
		"RTX A4000":             GPU_A4000,
		"NVIDIA RTX A4000":      GPU_A4000,
		"NVIDIA A100 80GB PCI":  GPU_A100_80,
		"A100-SXM4-40GB":        GPU_A100_40,
		"GeForce RTX 4090":      GPU_RTX4090,
		"NVIDIA L4 Tensor GPU":  GPU_L4,
		"NVIDIA L40S":           GPU_L40S,
		"any":                   GPU_ANY,
	}

	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			if got := NormalizeGPUType(input); got != want {
				t.Fatalf("NormalizeGPUType(%q) = %q, want %q", input, got, want)
			}
		})
	}
}

func TestGPUTypesFromStringNormalizesAliases(t *testing.T) {
	got := GPUTypesFromString("NVIDIA RTX A4000, GeForce RTX 4090")
	want := []GpuType{GPU_A4000, GPU_RTX4090}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("gpu[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
