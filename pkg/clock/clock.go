// Package clock is the source of time for code that must be testable without
// sleeping: the scheduler's capacity path and the provisioner. Production
// code takes a Clock and uses Real; tests and the scheduler simulator use
// Fake and move time themselves.
package clock

import (
	"context"
	"time"
)

type Clock interface {
	Now() time.Time
	// AfterFunc calls f once, d from now.
	AfterFunc(d time.Duration, f func()) Timer
	// Every calls tick once per interval until ctx is done. It returns at
	// once; ticks never overlap.
	Every(ctx context.Context, interval time.Duration, tick func())
}

type Timer interface {
	// Stop prevents the timer from firing. It reports whether it did.
	Stop() bool
}

// OrReal returns c, or the real clock when c is nil.
func OrReal(c Clock) Clock {
	if c == nil {
		return Real()
	}
	return c
}

// Real is the wall clock.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}

func (realClock) Every(ctx context.Context, interval time.Duration, tick func()) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// A tick that was already due when ctx was cancelled must
				// not run.
				if ctx.Err() != nil {
					return
				}
				tick()
			}
		}
	}()
}
