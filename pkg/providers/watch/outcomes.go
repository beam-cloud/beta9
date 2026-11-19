package watch

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/rs/zerolog/log"
)

// What an outcome is of.
const (
	// OutcomeCreate: a provider was asked for a machine.
	OutcomeCreate = "create"
	// OutcomeActive: a machine that was bought came up.
	OutcomeActive = "active"
	// OutcomeEnd: a machine that was bought is given back, or was lost.
	OutcomeEnd = "end"
)

// How a purchase went.
const (
	ResultOK         = "ok"
	ResultOutOfStock = "out_of_stock"
	ResultQuota      = "quota"
	// ResultUnanswered: the provider gave no answer, and may have sold the
	// machine all the same.
	ResultUnanswered = "unanswered"
	ResultError      = "error"
)

// How a machine ended.
const (
	// ResultInterrupted: its provider took a spot machine back.
	ResultInterrupted = "interrupted"
	// ResultFailed: its provider says it failed.
	ResultFailed = "failed"
	// ResultGone: its provider no longer has it, and nobody here deleted it.
	ResultGone = "gone"
	// ResultNeverUp: it did not come up in the time it was given.
	ResultNeverUp = "never_up"
	// ResultLost: it came up, and then stopped working while its provider
	// still said it ran.
	ResultLost = "lost"
	// ResultReleased: it was given back because it was no longer wanted.
	ResultReleased = "released"
	// ResultExpired: the time it was bought for ran out.
	ResultExpired = "expired"
)

// Who bought a machine.
const (
	// SourcePool: a pool that buys machines as its work needs them.
	SourcePool = "pool"
	// SourceReservation: a tenant, from the compute page.
	SourceReservation = "reservation"
)

// Outcome is what became of one machine at one point of its life. For a
// provider that does not say what it has in stock, these are the only word
// on it: how often it sold what it listed, how long a machine took to come
// up, and how long a spot machine was kept.
type Outcome struct {
	At     time.Time `json:"-"`
	Event  string    `json:"event"`
	Result string    `json:"result"`
	Source string    `json:"source"`

	Provider string `json:"provider"`
	Cloud    string `json:"cloud,omitempty"`
	Region   string `json:"region,omitempty"`
	Geo      string `json:"geo,omitempty"`
	// Offer is the instance type, and Zone its zone where the provider
	// sells by zone.
	Offer string `json:"offer,omitempty"`
	Zone  string `json:"zone,omitempty"`
	Spot  bool   `json:"spot,omitempty"`
	GPU   string `json:"gpu,omitempty"`
	GPUs  uint32 `json:"gpus,omitempty"`
	// HourlyMicros is what the provider charged for the machine an hour.
	HourlyMicros int64 `json:"micros,omitempty"`

	// Took is how long the provider took to answer, for a purchase; how
	// long the machine took to come up, once it has; and how long the
	// machine was had, at its end.
	Took time.Duration `json:"-"`
	// Err is the start of what the provider said, when it refused.
	Err string `json:"err,omitempty"`
}

type outcomeWire struct {
	V  int   `json:"v"`
	TS int64 `json:"ts"`
	outcomeFields
	TookMS int64 `json:"took_ms,omitempty"`
}

// outcomeFields is Outcome without its methods, so that writing one does not
// call itself.
type outcomeFields Outcome

func (o Outcome) MarshalJSON() ([]byte, error) {
	return json.Marshal(outcomeWire{V: recordVersion, TS: o.At.UnixMilli(), outcomeFields: outcomeFields(o), TookMS: o.Took.Milliseconds()})
}

func (o *Outcome) UnmarshalJSON(data []byte) error {
	var in outcomeWire
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	*o = Outcome(in.outcomeFields)
	o.At = time.UnixMilli(in.TS).UTC()
	o.Took = time.Duration(in.TookMS) * time.Millisecond
	return nil
}

// OutcomeOf is the outcome of a machine bought, or to be bought, from an
// offer, with everything the offer says about it.
func OutcomeOf(offer providers.Offer) Outcome {
	listing := listingOf(offer)
	outcome := Outcome{
		Provider:     listing.Provider,
		Cloud:        listing.Cloud,
		Region:       listing.Region,
		Geo:          listing.Geo,
		Spot:         listing.Spot,
		GPU:          offer.GPU,
		GPUs:         offer.GPUCount,
		HourlyMicros: offer.HourlyCostMicros,
	}
	outcome.Zone, outcome.Offer = place(offer)
	return outcome
}

// Bought is the outcome of asking a provider for the machine, which
// answered with err after took.
func (o Outcome) Bought(err error, took time.Duration) Outcome {
	o.Event, o.Took = OutcomeCreate, took
	switch {
	case err == nil:
		o.Result = ResultOK
	case errors.Is(err, providers.ErrOutOfStock):
		o.Result = ResultOutOfStock
	case errors.Is(err, providers.ErrQuota):
		o.Result = ResultQuota
	case providers.Unanswered(err):
		o.Result = ResultUnanswered
	default:
		o.Result = ResultError
	}
	if err != nil {
		o.Err = shortError(err)
	}
	return o
}

// CameUp is the outcome of the machine coming up, took after it was bought.
func (o Outcome) CameUp(took time.Duration) Outcome {
	o.Event, o.Result, o.Took = OutcomeActive, ResultOK, took
	return o
}

// Ended is the outcome of the machine's end, had after it was bought.
func (o Outcome) Ended(result string, had time.Duration) Outcome {
	o.Event, o.Result, o.Took = OutcomeEnd, result, had
	return o
}

// outcomesHeld is how many outcomes wait to be written before more are
// dropped. A machine is bought far less often than this fills.
const outcomesHeld = 1024

// outcomesAtOnce is how many outcomes are written in one append.
const outcomesAtOnce = 100

// Recorder writes what became of the machines that were bought to the
// market's history. Recording never waits and never fails: a purchase is
// not held up for its record, and an outcome that cannot be written is
// dropped.
//
// A nil Recorder records nothing.
type Recorder struct {
	log     Log
	stream  Stream
	clock   common.Clock
	queue   chan Outcome
	dropped atomic.Int64
}

// NewRecorder returns a recorder that writes to log, or nil when there is no
// history to write to.
func NewRecorder(log Log, streams Streams, clock common.Clock) *Recorder {
	if log == nil {
		return nil
	}
	return &Recorder{log: log, stream: streams.Outcomes(), clock: common.ClockOrReal(clock), queue: make(chan Outcome, outcomesHeld)}
}

// Record takes note of an outcome, at the present time unless it says when.
func (r *Recorder) Record(outcome Outcome) {
	if r == nil {
		return
	}
	if outcome.At.IsZero() {
		outcome.At = r.clock.Now()
	}
	outcome.At = outcome.At.UTC()
	select {
	case r.queue <- outcome:
	default:
		if r.dropped.Add(1) == 1 {
			log.Warn().Msg("market: outcomes are recorded faster than they are written; some are dropped")
		}
	}
}

// Read returns the outcomes written from from until to, the oldest first.
func (r *Recorder) Read(ctx context.Context, from, to time.Time) ([]Outcome, error) {
	if r == nil {
		return nil, nil
	}
	records, err := r.log.Read(ctx, r.stream, from, to)
	outcomes := make([]Outcome, 0, len(records))
	for _, record := range records {
		var outcome Outcome
		if json.Unmarshal(record.Body, &outcome) == nil {
			outcomes = append(outcomes, outcome)
		}
	}
	return outcomes, err
}

// Start writes the outcomes that are recorded until ctx is done. It returns
// at once.
func (r *Recorder) Start(ctx context.Context) {
	if r == nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case first := <-r.queue:
				r.Flush(ctx, first)
			}
		}
	}()
}

// Flush writes the outcomes that wait, after any it is given.
func (r *Recorder) Flush(ctx context.Context, outcomes ...Outcome) {
	if r == nil {
		return
	}
	for {
		for len(outcomes) < outcomesAtOnce {
			select {
			case outcome := <-r.queue:
				outcomes = append(outcomes, outcome)
				continue
			default:
			}
			break
		}
		if len(outcomes) == 0 {
			return
		}
		records := make([]Record, 0, len(outcomes))
		for _, outcome := range outcomes {
			body, err := json.Marshal(outcome)
			if err != nil {
				continue
			}
			records = append(records, Record{At: outcome.At, Body: body})
		}
		if err := r.log.Append(ctx, r.stream, records...); err != nil {
			log.Warn().Err(err).Int("outcomes", len(records)).Msg("market: could not write what became of machines to the market's history")
		}
		outcomes = outcomes[:0]
	}
}
