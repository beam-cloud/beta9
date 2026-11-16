package common

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Clock is the source of time for code that must be testable without
// sleeping: the scheduler's capacity path and the provisioner. Production
// code takes a Clock and runs on the wall clock; tests and the scheduler
// simulator use a FakeClock and move time themselves.
type Clock interface {
	Now() time.Time
	// AfterFunc calls f once, d from now.
	AfterFunc(d time.Duration, f func()) ClockTimer
	// Every calls tick once per interval until ctx is done. It returns at
	// once; ticks never overlap.
	Every(ctx context.Context, interval time.Duration, tick func())
}

// ClockTimer is a call that AfterFunc has yet to make.
type ClockTimer interface {
	// Stop prevents the timer from firing. It reports whether it did.
	Stop() bool
}

// ClockOrReal returns c, or the wall clock when c is nil.
func ClockOrReal(c Clock) Clock {
	if c == nil {
		return realClock{}
	}
	return c
}

// realClock is the wall clock.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) ClockTimer {
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

// FakeClock is a clock that only moves when Advance is called. Timers and
// ticks that come due run on the goroutine that called Advance, in time
// order, so a test that advances the clock sees every loop body run
// deterministically.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64
	timers []*fakeClockTimer
	onStep []func(time.Duration)
}

type fakeClockTimer struct {
	clock    *FakeClock
	at       time.Time
	seq      uint64
	fn       func()
	interval time.Duration // zero for one-shot timers
	ctx      context.Context
	stopped  bool
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *FakeClock) AfterFunc(d time.Duration, fn func()) ClockTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.addLocked(&fakeClockTimer{at: f.now.Add(d), fn: fn})
}

func (f *FakeClock) Every(ctx context.Context, interval time.Duration, tick func()) {
	if interval <= 0 {
		panic("clock: non-positive interval")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addLocked(&fakeClockTimer{at: f.now.Add(interval), fn: tick, interval: interval, ctx: ctx})
}

// OnStep registers fn to run each time the clock moves, with the distance
// moved, before any timer due at the new time fires. The simulator uses it to
// keep Redis key expiry in step with the clock.
func (f *FakeClock) OnStep(fn func(time.Duration)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onStep = append(f.onStep, fn)
}

// Advance moves the clock forward by d, running everything that comes due on
// the way. A timer added by a callback runs in the same call if it is due.
func (f *FakeClock) Advance(d time.Duration) {
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

func (f *FakeClock) moveTo(t time.Time) {
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
func (f *FakeClock) popDue(target time.Time) *fakeClockTimer {
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

func (f *FakeClock) addLocked(timer *fakeClockTimer) *fakeClockTimer {
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

func (t *fakeClockTimer) Stop() bool {
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
