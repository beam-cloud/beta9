package worker

import (
	"context"
	"os"
	"sync"
	"time"
)

var checkpointCacheIO checkpointIOActivity

// Start optional cache copies after foreground preparation, memory capture
// and final disk-state syncs have stayed quiet for a configured interval.
// The existing eight archive-cache admissions bound files retained while idle.
type checkpointIOActivity struct {
	mu      sync.Mutex
	active  int
	last    time.Time
	changed chan struct{}
}

func checkpointCacheQuietPeriod() time.Duration {
	d, err := time.ParseDuration(os.Getenv("BETA9_CHECKPOINT_CACHE_QUIET"))
	if err != nil || d < 0 || d > 2*time.Second {
		return 0
	}
	return d
}

func beginCheckpointCriticalIO() func() {
	if checkpointCacheQuietPeriod() == 0 {
		return func() {}
	}
	return checkpointCacheIO.begin()
}

func waitForCheckpointCacheQuiet(ctx context.Context) error {
	quiet := checkpointCacheQuietPeriod()
	if quiet == 0 {
		return ctx.Err()
	}
	return checkpointCacheIO.wait(ctx, quiet)
}

func (a *checkpointIOActivity) notifyLocked() {
	if a.changed != nil {
		close(a.changed)
	}
	a.changed = make(chan struct{})
}

func (a *checkpointIOActivity) begin() func() {
	a.mu.Lock()
	a.active++
	a.notifyLocked()
	a.mu.Unlock()
	return sync.OnceFunc(func() {
		a.mu.Lock()
		a.active--
		a.last = time.Now()
		a.notifyLocked()
		a.mu.Unlock()
	})
}

func (a *checkpointIOActivity) wait(ctx context.Context, quiet time.Duration) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		a.mu.Lock()
		active, changed := a.active, a.changed
		delay := quiet - time.Since(a.last)
		a.mu.Unlock()
		if active == 0 && delay <= 0 {
			return nil
		}
		if active > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}
