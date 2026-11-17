package market

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	// freshFor is how many sampling intervals what a provider last answered
	// stands for what it sells now.
	freshFor = 3

	// How long an answer is given again without being worked out anew. The
	// record changes once an interval.
	seriesCachedFor = 30 * time.Second
	nowCachedFor    = 10 * time.Second
	cachedAtMost    = 256
)

// ErrNoHistory is the answer to a question about the past where no history
// is kept.
var ErrNoHistory = errors.New("the market's history is not kept here: S2 is not configured")

// ErrAdminOnly is the answer to a question that names providers from someone
// who may not know them.
var ErrAdminOnly = errors.New("which provider sells what is for cluster admins only")

// RequestError is what is wrong with a question.
type RequestError struct{ What string }

func (e *RequestError) Error() string { return e.What }

func badRequest(format string, args ...any) error {
	return &RequestError{What: fmt.Sprintf(format, args...)}
}

type ReaderOptions struct {
	Providers *providers.Registry
	Redis     *common.RedisClient
	// Log holds the history. Nil means none is kept.
	Log    Log
	Config types.ProviderMarketConfig
	// Sells reports whether tenants can reserve machines of a provider. Only
	// those are shown to them. Nil means every provider.
	Sells func(provider string) bool
	// Bill is what a tenant pays for what costs the provider's price. Nil
	// means the same.
	Bill func(micros int64) int64
	// Clock is the reader's source of time. Nil means the wall clock.
	Clock common.Clock
}

// Reader answers questions from the market's record: what is for sale now,
// and what it sold for over time.
//
// It answers two kinds of asker. A tenant sees what it would pay, by GPU,
// part of the world and how the machine is sold, and never who sells it. A
// cluster admin also sees every provider's own listings and prices.
type Reader struct {
	providers *providers.Registry
	shared    *Shared
	log       Log
	streams   Streams
	interval  time.Duration
	sells     func(string) bool
	bill      func(int64) int64
	clock     common.Clock

	mu     sync.Mutex
	cached map[string]cachedAnswer
}

type cachedAnswer struct {
	answer any
	until  time.Time
}

func NewReader(opts ReaderOptions) *Reader {
	sells, bill := opts.Sells, opts.Bill
	if sells == nil {
		sells = func(string) bool { return true }
	}
	if bill == nil {
		bill = func(micros int64) int64 { return micros }
	}
	return &Reader{
		providers: opts.Providers,
		shared:    NewShared(opts.Redis),
		log:       opts.Log,
		streams:   StreamsOf(opts.Config),
		interval:  opts.Config.IntervalOrDefault(),
		sells:     sells,
		bill:      bill,
		clock:     common.ClockOrReal(opts.Clock),
		cached:    map[string]cachedAnswer{},
	}
}

// HasHistory reports whether questions about the past can be answered.
func (r *Reader) HasHistory() bool { return r.log != nil }

// Offers returns what a provider sold when the sampler last asked it, and
// whether that is recent enough to stand for what it sells now.
func (r *Reader) Offers(ctx context.Context, provider string) ([]providers.Offer, bool) {
	latest, ok, err := r.shared.Latest(ctx, provider)
	if err != nil || !ok || r.clock.Now().Sub(latest.At) > freshFor*r.interval {
		return nil, false
	}
	return latest.Offers, true
}

// cache returns the answer kept under key, or works it out and keeps it.
func cache[T any](r *Reader, key string, keep time.Duration, work func() (T, error)) (T, error) {
	now := r.clock.Now()
	r.mu.Lock()
	kept, ok := r.cached[key]
	r.mu.Unlock()
	if ok && now.Before(kept.until) {
		if answer, ok := kept.answer.(T); ok {
			return answer, nil
		}
	}
	answer, err := work()
	if err != nil {
		return answer, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.cached) >= cachedAtMost {
		maps.DeleteFunc(r.cached, func(_ string, kept cachedAnswer) bool { return !now.Before(kept.until) })
		if len(r.cached) >= cachedAtMost {
			clear(r.cached)
		}
	}
	r.cached[key] = cachedAnswer{answer: answer, until: now.Add(keep)}
	return answer, nil
}
