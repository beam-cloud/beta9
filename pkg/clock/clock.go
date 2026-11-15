// Package clock is the source of time for code that must be testable without
// sleeping: the scheduler's capacity path and the provisioner. Production
// code takes a Clock and uses Real; tests and the scheduler simulator use
// Fake and move time themselves.
package clock

import (
	"context"
	"sort"
	"sync"
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

// Fake is a clock that only moves when Advance is called. Timers and ticks
// that come due run on the goroutine that called Advance, in time order, so
// a test that advances the clock sees every loop body run deterministically.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64
	timers []*fakeTimer
	onStep []func(time.Duration)
}

type fakeTimer struct {
	clock    *Fake
	at       time.Time
	seq      uint64
	fn       func()
	interval time.Duration // zero for one-shot timers
	ctx      context.Context
	stopped  bool
}

func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) AfterFunc(d time.Duration, fn func()) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addLocked(&fakeTimer{at: f.now.Add(d), fn: fn})
}

func (f *Fake) Every(ctx context.Context, interval time.Duration, tick func()) {
	if interval <= 0 {
		panic("clock: non-positive interval")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addLocked(&fakeTimer{at: f.now.Add(interval), fn: tick, interval: interval, ctx: ctx})
}

// OnStep registers fn to run each time the clock moves, with the distance
// moved, before any timer due at the new time fires. The simulator uses it to
// keep Redis key expiry in step with the clock.
func (f *Fake) OnStep(fn func(time.Duration)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onStep = append(f.onStep, fn)
}

// Advance moves the clock forward by d, running everything that comes due on
// the way. A timer added by a callback runs in the same call if it is due.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.mu.Unlock()

	for {
		timer := f.popDue(target)
		if timer == nil {
			break
		}
		f.moveTo(timer.at)
		if timer.ctx != nil && timer.ctx.Err() != nil {
			continue
		}
		timer.fn()
		if timer.interval > 0 {
			f.mu.Lock()
			if !timer.stopped {
				timer.at = timer.at.Add(timer.interval)
				f.addLocked(timer)
			}
			f.mu.Unlock()
		}
	}
	f.moveTo(target)
}

func (f *Fake) moveTo(t time.Time) {
	f.mu.Lock()
	step := t.Sub(f.now)
	if step <= 0 {
		f.mu.Unlock()
		return
	}
	f.now = t
	hooks := append([]func(time.Duration){}, f.onStep...)
	f.mu.Unlock()
	for _, hook := range hooks {
		hook(step)
	}
}

// popDue removes and returns the earliest live timer due at or before target.
func (f *Fake) popDue(target time.Time) *fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.timers) > 0 {
		timer := f.timers[0]
		if timer.at.After(target) {
			return nil
		}
		f.timers = f.timers[1:]
		if timer.stopped {
			continue
		}
		return timer
	}
	return nil
}

func (f *Fake) addLocked(timer *fakeTimer) *fakeTimer {
	f.seq++
	timer.clock = f
	timer.seq = f.seq
	index := sort.Search(len(f.timers), func(i int) bool {
		other := f.timers[i]
		if !other.at.Equal(timer.at) {
			return other.at.After(timer.at)
		}
		return other.seq > timer.seq
	})
	f.timers = append(f.timers, nil)
	copy(f.timers[index+1:], f.timers[index:])
	f.timers[index] = timer
	return timer
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped {
		return false
	}
	t.stopped = true
	for i, other := range t.clock.timers {
		if other == t {
			t.clock.timers = append(t.clock.timers[:i], t.clock.timers[i+1:]...)
			return true
		}
	}
	return false
}
