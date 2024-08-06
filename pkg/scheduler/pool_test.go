package scheduler

import (
	"fmt"
	"testing"

	"github.com/tj/assert"
)

func TestCalculateMemoryQuantity(t *testing.T) {
	tests := []struct {
		percentStr string
		memory     int64
		expected   string
	}{
		{"10%", 1024, "102Mi"},
		{"25%", 1024, "256Mi"},
		{"50%", 1024, "512Mi"},
		{"75", 1024, "768Mi"},
		{"100", 1024, "1Gi"},
		{"-1", 1024, "512Mi"},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%s of %d is %v", test.percentStr, test.memory, test.expected)
		t.Run(name, func(t *testing.T) {
			quantity := calculateMemoryQuantity(test.percentStr, test.memory)
			assert.Equal(t, test.expected, quantity.String())
		})
	}
}

func TestGetPercentageWithDefault(t *testing.T) {
	tests := []struct {
		percentStr string
		expected   float32
	}{
		{"100%", 1},
		{"99%", 0.99},
		{"1%", 0.01},
		{"33", 0.33},
		{"0", 0},
		{"-1", 0},
		{"xx", 0},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%s becomes or defaults to %.2f", test.percentStr, test.expected)
		t.Run(name, func(t *testing.T) {
			value, err := parseMemoryPercentage(test.percentStr)
			if test.expected == 0 {
				assert.Error(t, err)
				assert.Equal(t, test.expected, value)
			}

			assert.Equal(t, test.expected, value)
		})
	}
}
