package scheduler

import (
	"fmt"
	"testing"

	"github.com/tj/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestCalculateMemoryQuantity(t *testing.T) {
	tests := []struct {
		percent  float32
		memory   int64
		expected resource.Quantity
	}{
		{0.1, 1024, resource.MustParse("102Mi")},
		{0.25, 1024, resource.MustParse("256Mi")},
		{0.5, 1024, resource.MustParse("512Mi")},
		{0.75, 1024, resource.MustParse("768Mi")},
		{1, 1024, resource.MustParse("1024Mi")},
	}
	for _, test := range tests {
		t.Run(fmt.Sprint(test.percent), func(t *testing.T) {
			value := calculateMemoryQuantity(test.percent, test.memory)
			assert.Equal(t, test.expected, value)
		})
	}
}

func TestGetPercentageWithDefault(t *testing.T) {
	tests := []struct {
		percentStr     string
		defaultPercent float32
		expected       float32
	}{
		{"100%", 0.5, 1},
		{"99%", 0.1, 0.99},
		{"1%", 0.2, 0.01},
		{"33", 0.4, 0.33},
		{"xx", 0.5, 0.5},
		{"yy", 0.2, 0.2},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%s with default %.2f becomes %.2f", test.percentStr, test.defaultPercent, test.expected)
		t.Run(name, func(t *testing.T) {
			value := getPercentageWithDefault(test.percentStr, test.defaultPercent)
			assert.Equal(t, test.expected, value)
		})
	}
}
