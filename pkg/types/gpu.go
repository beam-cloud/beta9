package types

import (
	"encoding/json"
	"strings"
)

type GpuType string

func (g *GpuType) UnmarshalJSON(data []byte) error {
	var gpuStr string
	err := json.Unmarshal(data, &gpuStr)
	if err == nil {
		*g = GpuType(gpuStr)
		return nil
	}

	var gpuInt int
	err = json.Unmarshal(data, &gpuInt)
	if err != nil {
		return err
	}

	if gpuInt == 0 {
		*g = GpuType("")
	} else if gpuInt > 0 {
		*g = GpuType("T4")
	}

	return nil
}

func (g *GpuType) MarshalJSON() ([]byte, error) {
	if *g == "" {
		return []byte("0"), nil
	}

	return json.Marshal(string(*g))
}

func (g GpuType) String() string {
	return string(g)
}

func GPUTypesFromString(gpu string) []GpuType {
	gpus := []GpuType{}
	gpuString := strings.Trim(gpu, " ")
	if len(gpuString) > 0 {
		for _, g := range strings.Split(gpuString, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				gpus = append(gpus, NormalizeGPUType(g))
			}
		}
	}

	return gpus
}

func GpuTypesToStrings(gpus []GpuType) []string {
	var gpuStrings []string
	for _, gpu := range gpus {
		gpuStrings = append(gpuStrings, string(NormalizeGPUType(string(gpu))))
	}
	return gpuStrings
}

func NormalizeGPUStrings(gpus []string) []string {
	out := make([]string, 0, len(gpus))
	for _, gpu := range gpus {
		normalized := NormalizeGPUType(gpu)
		if normalized != "" {
			out = append(out, string(normalized))
		}
	}
	return out
}

const (
	GPU_A10         GpuType = "A10"
	GPU_A10G        GpuType = "A10G"
	GPU_A100        GpuType = "A100"
	GPU_A100_40     GpuType = "A100-40"
	GPU_A100_80     GpuType = "A100-80"
	GPU_A16         GpuType = "A16"
	GPU_A30         GpuType = "A30"
	GPU_A40         GpuType = "A40"
	GPU_A4000       GpuType = "A4000"
	GPU_A5000       GpuType = "A5000"
	GPU_L4          GpuType = "L4"
	GPU_L40         GpuType = "L40"
	GPU_T4          GpuType = "T4"
	GPU_GAUDI2      GpuType = "GAUDI2"
	GPU_GH200       GpuType = "GH200"
	GPU_H100        GpuType = "H100"
	GPU_H200        GpuType = "H200"
	GPU_B200        GpuType = "B200"
	GPU_B300        GpuType = "B300"
	GPU_A6000       GpuType = "A6000"
	GPU_RTX4000_ADA GpuType = "RTX4000Ada"
	GPU_RTX4090     GpuType = "RTX4090"
	GPU_RTX5090     GpuType = "RTX5090"
	GPU_RTX6000     GpuType = "RTX6000"
	GPU_RTX6000_ADA GpuType = "RTX6000Ada"
	GPU_RTX_PRO6000 GpuType = "RTXPro6000"
	GPU_L40S        GpuType = "L40S"
	GPU_V100        GpuType = "V100"
	GPU_V100_32     GpuType = "V100-32"

	NO_GPU  GpuType = "NO_GPU"
	GPU_ANY GpuType = "any"
)

func AllGPUTypes() []GpuType {
	return []GpuType{
		GPU_A10,
		GPU_A10G,
		GPU_A100,
		GPU_A100_40,
		GPU_A100_80,
		GPU_A16,
		GPU_A30,
		GPU_A40,
		GPU_A4000,
		GPU_A5000,
		GPU_L4,
		GPU_L40,
		GPU_T4,
		GPU_GAUDI2,
		GPU_GH200,
		GPU_H100,
		GPU_H200,
		GPU_B200,
		GPU_B300,
		GPU_A6000,
		GPU_RTX4000_ADA,
		GPU_RTX4090,
		GPU_RTX5090,
		GPU_RTX6000,
		GPU_RTX6000_ADA,
		GPU_RTX_PRO6000,
		GPU_L40S,
		GPU_V100,
		GPU_V100_32,
		GPU_ANY,
	}
}

func NormalizeGPUType(value string) GpuType {
	raw := strings.TrimSpace(value)
	if raw == "" || raw == "0" {
		return GpuType("")
	}

	key := compactGPUName(raw)
	for _, word := range []string{"NVIDIA", "GEFORCE", "TESLA", "QUADRO"} {
		key = strings.ReplaceAll(key, word, "")
	}

	if key == "ANY" {
		return GPU_ANY
	}
	if key == "NOGPU" || key == "NONE" {
		return NO_GPU
	}
	if strings.Contains(key, "A100") && strings.Contains(key, "80G") {
		return GPU_A100_80
	}
	if strings.Contains(key, "A100") && strings.Contains(key, "40G") {
		return GPU_A100_40
	}
	for _, alias := range gpuAliases {
		if strings.Contains(key, alias.match) {
			return alias.gpu
		}
	}
	return GpuType(raw)
}

var gpuAliases = []struct {
	match string
	gpu   GpuType
}{
	{"RTXPRO6000", GPU_RTX_PRO6000},
	{"RTX6000ADA", GPU_RTX6000_ADA},
	{"RTX4000ADA", GPU_RTX4000_ADA},
	{"RTX6000", GPU_RTX6000},
	{"RTX5090", GPU_RTX5090},
	{"RTX4090", GPU_RTX4090},
	{"V10032G", GPU_V100_32},
	{"V10032", GPU_V100_32},
	{"V100", GPU_V100},
	{"GAUDI2", GPU_GAUDI2},
	{"GH200", GPU_GH200},
	{"B300", GPU_B300},
	{"B200", GPU_B200},
	{"H200", GPU_H200},
	{"H100", GPU_H100},
	{"L40S", GPU_L40S},
	{"L40", GPU_L40},
	{"L4", GPU_L4},
	{"T4", GPU_T4},
	{"A10080G", GPU_A100_80},
	{"A10080", GPU_A100_80},
	{"A10040G", GPU_A100_40},
	{"A10040", GPU_A100_40},
	{"A100", GPU_A100},
	{"A6000", GPU_A6000},
	{"A5000", GPU_A5000},
	{"A4000", GPU_A4000},
	{"A40", GPU_A40},
	{"A30", GPU_A30},
	{"A16", GPU_A16},
	{"A10G", GPU_A10G},
	{"A10", GPU_A10},
}

func KnownGPUType(gpu GpuType) bool {
	normalized := NormalizeGPUType(string(gpu))
	for _, known := range AllGPUTypes() {
		if normalized == known {
			return true
		}
	}
	return false
}

func compactGPUName(value string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(value) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func GpuTypesToSlice(gpuTypes []GpuType) []string {
	gpuTypesSlice := []string{}
	for _, gpuType := range gpuTypes {
		gpuTypesSlice = append(gpuTypesSlice, string(NormalizeGPUType(string(gpuType))))
	}
	return gpuTypesSlice
}

func GPUTypesToMap(gpuTypes []GpuType) map[string]int {
	gpuTypesMap := map[string]int{}
	for i, gpuType := range gpuTypes {
		gpuTypesMap[string(NormalizeGPUType(string(gpuType)))] = i
	}
	return gpuTypesMap
}
