package storage

import "testing"

func TestEffectiveGeeseMemoryLimitMB(t *testing.T) {
	tests := []struct {
		name       string
		configured int64
		worker     string
		want       int64
	}{
		{name: "keeps configured under worker cap", configured: 256, worker: "1024", want: 256},
		{name: "caps configured above worker cap", configured: 8000, worker: "1024", want: 512},
		{name: "uses worker cap when configured unset", configured: 0, worker: "2Gi", want: 1024},
		{name: "keeps configured without worker limit", configured: 2048, worker: "", want: 2048},
		{name: "handles mib suffix", configured: 2048, worker: "1024Mi", want: 512},
		{name: "preserves minimum for tiny limits", configured: 2048, worker: "128", want: 128},
		{name: "does not exceed very small worker limit", configured: 2048, worker: "64", want: 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveGeeseMemoryLimitMB(tt.configured, tt.worker)
			if got != tt.want {
				t.Fatalf("effectiveGeeseMemoryLimitMB(%d, %q) = %d, want %d", tt.configured, tt.worker, got, tt.want)
			}
		})
	}
}
