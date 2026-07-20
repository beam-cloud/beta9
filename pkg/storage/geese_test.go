package storage

import (
	"reflect"
	"testing"
)

type testVolumeReporter struct {
	workspaceID string
	hash        string
	sourcePath  string
	sizeBytes   int64
}

func (r *testVolumeReporter) ReportVolumeContent(workspaceID, hash, sourcePath string, sizeBytes int64) {
	r.workspaceID = workspaceID
	r.hash = hash
	r.sourcePath = sourcePath
	r.sizeBytes = sizeBytes
}

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

func TestHandleGeeseContentEventReportsStoredContent(t *testing.T) {
	reporter := &testVolumeReporter{}
	storage := &GeeseStorage{}
	storage.SetVolumeContentReporter("workspace-id", reporter)
	const modelSizeBytes = int64(1536) << 30

	storage.handleGeeseContentEvent(map[string]interface{}{
		"content_hash": "hash",
		"inode":        "/volumes/workspace/files/data.bin",
		"size_bytes":   uint64(modelSizeBytes),
	})

	if reporter.workspaceID != "workspace-id" {
		t.Fatalf("workspaceID = %q", reporter.workspaceID)
	}
	if reporter.hash != "hash" {
		t.Fatalf("hash = %q", reporter.hash)
	}
	if reporter.sourcePath != "/volumes/workspace/files/data.bin" {
		t.Fatalf("sourcePath = %q", reporter.sourcePath)
	}
	if reporter.sizeBytes != modelSizeBytes {
		t.Fatalf("sizeBytes = %d", reporter.sizeBytes)
	}
}

func TestWithBoundedMountOption(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		want    []string
	}{
		{name: "adds absent option", options: []string{"allow_other"}, want: []string{"allow_other", "max_read=1048576"}},
		{name: "clamps larger option", options: []string{"allow_other,max_read=2097152"}, want: []string{"allow_other,max_read=1048576"}},
		{name: "preserves smaller option", options: []string{"max_read=262144"}, want: []string{"max_read=262144"}},
		{name: "replaces invalid option", options: []string{"max_read=invalid"}, want: []string{"max_read=1048576"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := append([]string(nil), tt.options...)
			got := withBoundedMountOption(tt.options, "max_read", defaultGeeseFSSpliceReadBytes)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("withBoundedMountOption(%v) = %v, want %v", tt.options, got, tt.want)
			}
			if !reflect.DeepEqual(tt.options, original) {
				t.Fatalf("input options mutated from %v to %v", original, tt.options)
			}
		})
	}
}
