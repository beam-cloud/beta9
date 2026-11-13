package apiv1

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestSetConfigFieldNumericBooleans(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  bool
	}{
		{"int_zero", int(0), false},
		{"int64_zero", int64(0), false},
		{"uint_zero", uint(0), false},
		{"uint64_zero", uint64(0), false},
		{"float64_zero", float64(0), false},
		{"int_nonzero", int(1), true},
		{"int64_negative", int64(-1), true},
		{"uint_nonzero", uint(1), true},
		{"uint64_nonzero", uint64(1), true},
		{"float64_nonzero", float64(1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &types.StubConfigV1{CheckpointEnabled: !tt.want}
			if err := SetConfigField(config, "checkpoint_enabled", tt.value); err != nil {
				t.Fatalf("SetConfigField(checkpoint_enabled, %T(%v)): %v", tt.value, tt.value, err)
			}
			if config.CheckpointEnabled != tt.want {
				t.Errorf("checkpoint_enabled = %v after setting %T(%v), want %v", config.CheckpointEnabled, tt.value, tt.value, tt.want)
			}
		})
	}
}
