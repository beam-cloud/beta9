package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestParseCPU(t *testing.T) {
	tests := []struct {
		name    string
		cpu     string
		want    int64
		wantErr bool
	}{
		{
			name:    "valid CPU",
			cpu:     "200m",
			want:    200,
			wantErr: false,
		},
		{
			name:    "invalid CPU",
			cpu:     "200x",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCPU(tt.cpu)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCPU() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseCPU() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseMemory(t *testing.T) {
	tests := []struct {
		name    string
		memory  string
		want    int64
		wantErr bool
	}{
		{
			name:    "valid memory Mi",
			memory:  "200Mi",
			want:    200,
			wantErr: false,
		},
		{
			name:    "valid memory not units",
			memory:  "200",
			want:    200,
			wantErr: false,
		},
		{
			name:    "valid memory Gi",
			memory:  "2Gi",
			want:    2048,
			wantErr: false,
		},
		{
			name:    "valid memory Ki",
			memory:  "2097152Ki",
			want:    2048,
			wantErr: false,
		},
		{
			name:    "invalid memory",
			memory:  "200Qi",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMemory(tt.memory)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMemory() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseMemory() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseGPU(t *testing.T) {
	tests := []struct {
		name     string
		gpuCount interface{}
		wantVal  uint
		wantErr  bool
	}{
		{
			name:     "valid gpu count of 0",
			gpuCount: 0,
			wantVal:  0,
			wantErr:  false,
		},
		{
			name:     "valid gpu count of 1",
			gpuCount: 1,
			wantVal:  1,
			wantErr:  false,
		},
		{
			name:     "valid gpu count of 4",
			gpuCount: 4,
			wantVal:  4,
			wantErr:  false,
		},
		{
			name:     "invalid gpu count of -1",
			gpuCount: -1,
			wantVal:  0,
			wantErr:  true,
		},
		{
			name:     "invalid gpu count of 0.25",
			gpuCount: 0.25,
			wantVal:  0,
			wantErr:  true,
		},
		{
			name:     "invalid gpu count of true",
			gpuCount: true,
			wantVal:  0,
			wantErr:  true,
		},
		{
			name:     "invalid gpu count of XyZ",
			gpuCount: "XyZ",
			wantVal:  0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ParseGPU(tt.gpuCount)

			if !tt.wantErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}

			assert.Equal(t, tt.wantVal, val)
		})
	}
}

func TestParseGPUType(t *testing.T) {
	tests := []struct {
		name    string
		gpuType interface{}
		wantVal types.GpuType
		wantErr bool
	}{
		{
			name:    "valid gpu t4",
			gpuType: "T4",
			wantVal: types.GPU_T4,
			wantErr: false,
		},
		{
			name:    "valid gpu a10g",
			gpuType: "A10G",
			wantVal: types.GPU_A10G,
			wantErr: false,
		},
		{
			name:    "valid gpu l4",
			gpuType: "L4",
			wantVal: types.GPU_L4,
			wantErr: false,
		},
		{
			name:    "valid gpu a100-40 (with hyphen)",
			gpuType: "A100-40",
			wantVal: types.GPU_A100_40,
			wantErr: false,
		},
		{
			name:    "valid gpu a100-80 (with hyphen)",
			gpuType: "A100-80",
			wantVal: types.GPU_A100_80,
			wantErr: false,
		},
		{
			name:    "valid gpu rtx4090",
			gpuType: "RTX4090",
			wantVal: types.GPU_RTX4090,
			wantErr: false,
		},
		{
			name:    "invalid gpu a100_80 (with underscore)",
			gpuType: "A100_80",
			wantVal: types.GpuType(""),
			wantErr: true,
		},
		{
			name:    "invalid gpu type 3060 (as str)",
			gpuType: "3060",
			wantVal: types.GpuType(""),
			wantErr: true,
		},
		{
			name:    "invalid gpu type 4090 (as int)",
			gpuType: 4090,
			wantVal: types.GpuType(""),
			wantErr: true,
		},
		{
			name:    "invalid gpu type -4070 (as int)",
			gpuType: -4070,
			wantVal: types.GpuType(""),
			wantErr: true,
		},
		{
			name:    "invalid gpu type (no gpu type provided)",
			gpuType: "",
			wantVal: types.GpuType(""),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ParseGPUType(tt.gpuType)

			if !tt.wantErr {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}

			assert.Equal(t, tt.wantVal, val)
		})
	}
}

func TestParseTmpSizeLimit(t *testing.T) {
	tests := []struct {
		name        string
		poolLimit   string
		globalLimit string
		wantVal     resource.Quantity
		wantErr     bool
	}{
		{
			name:        "valid pool specific tmp size limit",
			poolLimit:   "10Gi",
			globalLimit: "",
			wantVal:     resource.MustParse("10Gi"),
			wantErr:     false,
		},
		{
			name:        "valid global tmp size limit",
			poolLimit:   "",
			globalLimit: "10Gi",
			wantVal:     resource.MustParse("10Gi"),
			wantErr:     false,
		},
		{
			name:        "invalid tmp size limit",
			poolLimit:   "10Qi",
			globalLimit: "",
			wantVal:     resource.MustParse("128Gi"),
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTmpSizeLimit(tt.poolLimit, tt.globalLimit)
			assert.Equal(t, tt.wantVal, got)
		})
	}
}
