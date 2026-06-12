package worker

import (
	"testing"

	"github.com/beam-cloud/clip/pkg/clip"
)

func TestFormatImageIndexProgressWithPercent(t *testing.T) {
	progress := clip.OCIIndexProgress{
		LayerIndex:               2,
		TotalLayers:              5,
		BytesProcessed:           240 * 1024 * 1024,
		CompressedBytesProcessed: 50 * 1024 * 1024,
		CompressedBytesTotal:     100 * 1024 * 1024,
	}

	expected := "Indexing layer 2/5... 50% (50.0 MiB/100.0 MiB read, 240.0 MiB indexed)"
	if got := formatImageIndexProgress(progress); got != expected {
		t.Fatalf("unexpected progress message\nwant: %s\n got: %s", expected, got)
	}
}

func TestShouldLogImageIndexProgressBucketsByPercent(t *testing.T) {
	buckets := map[string]int64{}
	progress := clip.OCIIndexProgress{
		LayerDigest:              "sha256:layer",
		CompressedBytesProcessed: 9 * 1024 * 1024,
		CompressedBytesTotal:     100 * 1024 * 1024,
	}

	if shouldLogImageIndexProgress(progress, buckets) {
		t.Fatal("expected sub-10% progress to stay quiet")
	}

	progress.CompressedBytesProcessed = 10 * 1024 * 1024
	if !shouldLogImageIndexProgress(progress, buckets) {
		t.Fatal("expected first 10% bucket to log")
	}

	progress.CompressedBytesProcessed = 15 * 1024 * 1024
	if shouldLogImageIndexProgress(progress, buckets) {
		t.Fatal("expected same 10% bucket to stay quiet")
	}

	progress.CompressedBytesProcessed = 20 * 1024 * 1024
	if !shouldLogImageIndexProgress(progress, buckets) {
		t.Fatal("expected next 10% bucket to log")
	}
}

func TestShouldLogImageIndexProgressFallsBackToIndexedBytes(t *testing.T) {
	buckets := map[string]int64{}
	progress := clip.OCIIndexProgress{
		LayerDigest:    "sha256:layer",
		BytesProcessed: imageIndexProgressBucketBytes - 1,
	}

	if shouldLogImageIndexProgress(progress, buckets) {
		t.Fatal("expected progress below byte bucket to stay quiet")
	}

	progress.BytesProcessed = imageIndexProgressBucketBytes
	if !shouldLogImageIndexProgress(progress, buckets) {
		t.Fatal("expected first byte bucket to log")
	}
}
