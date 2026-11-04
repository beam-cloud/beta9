package runtime

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// A bounded rendezvous keeps the first guests in a burst from competing with
// host preparation for the rest. Disabled unless the staging experiment opts in.
type microVMBootBatcher struct {
	mu          sync.Mutex
	window      time.Duration
	quietWindow time.Duration
	current     *microVMBootGroup
}

type microVMBootGroup struct {
	ready           chan struct{}
	pending         int
	closed          bool
	deadline        *time.Timer
	quiet           *time.Timer
	quietGeneration uint64
	members         int
	created         time.Time
}

func newMicroVMBootBatcher() *microVMBootBatcher {
	window, err := time.ParseDuration(os.Getenv("BETA9_MICROVM_BOOT_BATCH_WINDOW"))
	if err != nil || window <= 0 || window > 500*time.Millisecond {
		return nil
	}
	quiet, err := time.ParseDuration(os.Getenv("BETA9_MICROVM_BOOT_BATCH_QUIET"))
	if err != nil || quiet < time.Millisecond || quiet > 200*time.Millisecond {
		quiet = 5 * time.Millisecond
	}
	return &microVMBootBatcher{window: window, quietWindow: quiet}
}

type microVMBootPreparationKey struct{}
type microVMBootPreparation struct {
	owner     *microVMBootBatcher
	wait      func(context.Context) error
	finish    func()
	claimed   atomic.Bool
	handedOff atomic.Bool
}

func (b *microVMBootBatcher) beginPreparation(ctx context.Context) (context.Context, func()) {
	if b == nil {
		return ctx, func() {}
	}
	wait, finish := b.join()
	preparation := &microVMBootPreparation{owner: b, wait: wait, finish: finish}
	return context.WithValue(ctx, microVMBootPreparationKey{}, preparation), func() {
		if !preparation.handedOff.Load() {
			finish()
		}
	}
}

// HandoffMicroVMBootPreparation transfers the rendezvous to the independently
// cancellable runtime goroutine. Only this value crosses contexts. The caller
// invokes finish even when spawn fails before entering Runtime.Run.
func HandoffMicroVMBootPreparation(ctx context.Context) (func(context.Context) context.Context, func()) {
	preparation, ok := ctx.Value(microVMBootPreparationKey{}).(*microVMBootPreparation)
	if !ok {
		return nil, nil
	}
	preparation.handedOff.Store(true)
	return func(ctx context.Context) context.Context {
		return context.WithValue(ctx, microVMBootPreparationKey{}, preparation)
	}, preparation.finish
}

func (b *microVMBootBatcher) joinForContext(ctx context.Context) (func(context.Context) error, func()) {
	if preparation, ok := ctx.Value(microVMBootPreparationKey{}).(*microVMBootPreparation); ok && preparation.owner == b && preparation.claimed.CompareAndSwap(false, true) {
		return preparation.wait, preparation.finish
	}
	return b.join()
}

func (b *microVMBootBatcher) join() (wait func(context.Context) error, finish func()) {
	if b == nil {
		return func(context.Context) error { return nil }, func() {}
	}
	b.mu.Lock()
	g := b.current
	if g == nil {
		g = &microVMBootGroup{ready: make(chan struct{}), created: time.Now()}
		b.current = g
		g.deadline = time.AfterFunc(b.window, func() { b.mu.Lock(); defer b.mu.Unlock(); b.open(g) })
	}
	g.pending++
	g.members++
	g.quietGeneration++
	if g.quiet != nil {
		g.quiet.Stop()
	}
	b.mu.Unlock()
	finish = sync.OnceFunc(func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		g.pending--
		if g.pending == 0 && !g.closed {
			g.quietGeneration++
			generation := g.quietGeneration
			quiet := b.quietWindow
			if quiet <= 0 {
				quiet = 5 * time.Millisecond
			}
			g.quiet = time.AfterFunc(min(quiet, b.window), func() {
				b.mu.Lock()
				defer b.mu.Unlock()
				if g.pending == 0 && g.quietGeneration == generation {
					b.open(g)
				}
			})
		}
	})
	wait = func(ctx context.Context) error {
		finish()
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.ready:
			return nil
		}
	}
	return wait, finish
}

// Called with b.mu held, including timer callbacks.
func (b *microVMBootBatcher) open(g *microVMBootGroup) {
	if g.closed {
		return
	}
	g.closed = true
	g.deadline.Stop()
	if g.quiet != nil {
		g.quiet.Stop()
	}
	close(g.ready)
	if b.current == g {
		b.current = nil
	}
	log.Info().Int("batch_size", g.members).Int("preparations_pending", g.pending).
		Dur("batch_age", time.Since(g.created)).Msg("microvm boot preparation batch released")
}
