package disk

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A flatten writes its output as it goes; growth is its progress, and an
// output that stops growing must leave a watchdog nothing to reset on.
func TestReportFileGrowth(t *testing.T) {
	shorten(t, &fileGrowthPoll, 5*time.Millisecond)
	path := filepath.Join(t.TempDir(), "flat")
	var reports atomic.Int64
	stop := reportFileGrowth(WithProgress(context.Background(), func() { reports.Add(1) }), path)
	defer stop()

	require.NoError(t, os.WriteFile(path, []byte("a"), 0o600))
	require.Eventually(t, func() bool { return reports.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, reports.Load(), "an output that stopped growing must not report progress")

	require.NoError(t, os.WriteFile(path, []byte("ab"), 0o600))
	require.Eventually(t, func() bool { return reports.Load() == 2 }, time.Second, time.Millisecond)
}

// A scan reports progress as it reads, so a watchdog can tell a long scan
// from a stalled one, and stops reading once its context is canceled.
func TestScanLayerReportsProgressAndStopsWhenCanceled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layer")
	data := make([]byte, 3*LayerChunkSize)
	for i := range data {
		data[i] = byte(i % 251)
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))

	var reports atomic.Int64
	_, err := ScanLayer(WithProgress(context.Background(), func() { reports.Add(1) }), path, chunkKey)
	require.NoError(t, err)
	require.Positive(t, reports.Load())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ScanLayer(ctx, path, chunkKey)
	require.ErrorIs(t, err, context.Canceled)
}
