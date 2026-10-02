package disk

import (
	"context"
	"os"
	"time"
)

type progressKey struct{}

// WithProgress returns a context under which long disk operations (scanning a
// layer, flattening or committing a chain) call report whenever their work
// measurably advances, so a caller can tell slow work from a stalled one.
func WithProgress(ctx context.Context, report func()) context.Context {
	return context.WithValue(ctx, progressKey{}, report)
}

func reportProgress(ctx context.Context) {
	if report, ok := ctx.Value(progressKey{}).(func()); ok {
		report()
	}
}

// fileGrowthPoll is how often a file being written is checked; tests shorten it.
var fileGrowthPoll = time.Second

// reportFileGrowth reports progress each time the file at path grows, until
// the returned stop function is called.
func reportFileGrowth(ctx context.Context, path string) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(fileGrowthPoll)
		defer ticker.Stop()
		var size int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if info, err := os.Stat(path); err == nil && info.Size() > size {
					size = info.Size()
					reportProgress(ctx)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
