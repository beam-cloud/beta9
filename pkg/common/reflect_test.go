package common

import (
	"reflect"
	"testing"
)

func TestConvertValueNumericBooleans(t *testing.T) {
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
		{"bool_false", false, false},
		{"bool_true", true, true},
		{"string_off", "off", false},
		{"string_on", "on", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertValue(reflect.TypeOf(false), tt.value)
			if err != nil {
				t.Fatalf("ConvertValue(bool, %T(%v)): %v", tt.value, tt.value, err)
			}
			if got.Bool() != tt.want {
				t.Errorf("ConvertValue(bool, %T(%v)) = %v, want %v", tt.value, tt.value, got.Bool(), tt.want)
			}
		})
	}
}
