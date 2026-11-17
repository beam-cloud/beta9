package market

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	// leaseFor is how long the sampler's lock is held without being
	// renewed. A gateway that stops is replaced within it.
	leaseFor = 30 * time.Second
	// standbyRetry is how often a gateway that does not sample asks for
	// the lock.
	standbyRetry = 10 * time.Second

	// askTimeout is the most a provider is given to say what it sells. One
	// that takes longer has not answered this time.
	askTimeout = 45 * time.Second
	// askGrace is how much longer a provider that does not heed its
	// deadline is waited for before the sample goes on without it.
	askGrace = 2 * time.Second

	// keyframeEvery is how often a provider's offers are written in full
	// though they have not changed, so that a reader of its snapshots never
	// goes further back than this for them.
	keyframeEvery = time.Hour

	// writeConcurrency is how many streams are written to at once.
	writeConcurrency = 8
	// rollUpAtMost is how many hours of a GPU's samples are put together
	// at once, when the sampler has been away.
	rollUpAtMost = 24

	// shownError is how much of a provider's error a record keeps.
	shownError = 160
)

type Options struct {
	Providers *providers.Registry
	Redis     *common.RedisClient
	// Log keeps the history. Nil means none is kept: what the providers
	// sell now is still published to the gateways.
	Log    Log
	Config types.ProviderMarketConfig
	// Clock is the sampler's source of time. Nil means the wall clock.
	Clock common.Clock
	// ID names this gateway in what it writes. Empty means its hostname.
	ID string
}

// Sampler asks every provider what it sells, at an interval, and records
// what they answer. Every gateway runs one, and the one that holds the lock
// samples.
type Sampler struct {
	providers *providers.Registry
	shared    *Shared
	log       Log
	streams   Streams
	lock      *common.RedisLock
	clock     common.Clock
	id        string
	interval  time.Duration
	dryRun    bool
	// standby is how often a gateway that does not sample asks for the
	// lock. askFor is the most a provider is given to answer, and askGrace
	// how much longer one that does not heed that is waited for.
	standby  time.Duration
	askFor   time.Duration
	askGrace time.Duration

	// asking are the providers that have yet to answer the last time they
	// were asked. One that hangs is not asked again on top of itself.
	askingMu sync.Mutex
	asking   map[string]bool

	// What the gateway that samples remembers from one sample to the next.
	// A gateway that takes the sampling over starts without it, and so
	// writes every provider's offers in full.
	written       map[string]writtenSnapshot
	rolledThrough time.Time
}

// writtenSnapshot is the offers of a provider that were last written in
// full, and when.
type writtenSnapshot struct {
	hash string
	at   time.Time
}

func NewSampler(opts Options) *Sampler {
	id := opts.ID
	if id == "" {
		id, _ = os.Hostname()
	}
	return &Sampler{
		providers: opts.Providers,
		shared:    NewShared(opts.Redis),
		log:       opts.Log,
		streams:   StreamsOf(opts.Config),
		lock:      common.NewRedisLock(opts.Redis),
		clock:     common.ClockOrReal(opts.Clock),
		id:        id,
		interval:  opts.Config.IntervalOrDefault(),
		dryRun:    opts.Config.DryRun,
		standby:   standbyRetry,
		askFor:    askTimeout,
		askGrace:  askGrace,
		asking:    map[string]bool{},
		written:   map[string]writtenSnapshot{},
	}
}

// StreamsOf names the market's streams as the config keeps them.
func StreamsOf(config types.ProviderMarketConfig) Streams {
	return Streams{
		SnapshotsFor: config.SnapshotRetentionOrDefault(),
		SamplesFor:   config.SeriesRetentionOrDefault(),
	}
}

// Start samples for as long as this gateway holds the sampler's lock, and
// asks for the lock while it does not, until ctx is done. It returns at
// once.
func (s *Sampler) Start(ctx context.Context) {
	if len(s.providers.Names()) == 0 {
		return // nobody to ask
	}
	go s.run(ctx)
}

func (s *Sampler) run(ctx context.Context) {
	for {
		err := s.lock.WithLease(ctx, common.RedisKeys.MarketSamplerLock(), common.RedisLockOptions{TtlS: int(leaseFor.Seconds())}, s.lead)
		if err != nil && !common.IsRedisLockNotObtained(err) && ctx.Err() == nil {
			log.Warn().Err(err).Msg("market: this gateway stopped sampling what the providers sell; it or another takes over")
		}
		if !sleep(ctx, s.standby) {
			return
		}
	}
}

// lead samples at every interval of the wall clock until ctx is done, which
// is when the gateway stops or the lock is lost.
func (s *Sampler) lead(ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("sampling panicked: %v", recovered)
		}
	}()
	log.Info().Str("sampler", s.id).Dur("interval", s.interval).Bool("history", s.log != nil).Bool("dry_run", s.dryRun).
		Msg("market: this gateway samples what the providers sell")
	s.written = map[string]writtenSnapshot{}
	s.rolledThrough = time.Time{}
	for {
		now := s.clock.Now()
		next := now.Truncate(s.interval).Add(s.interval)
		if !sleep(ctx, next.Sub(now)) {
			return nil
		}
		s.Sample(ctx, next)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// answer is what a provider said when it was asked what it sells.
type answer struct {
	provider string
	offers   []providers.Offer
	err      error
	took     time.Duration
}

func (a answer) poll() Poll {
	poll := Poll{OK: a.err == nil, MS: a.took.Milliseconds(), N: len(a.offers)}
	if a.err != nil {
		poll.Err = shortError(a.err)
	}
	return poll
}

// shortError is a provider's error as a record keeps it: the status it
// answered with, that it did not answer in time, or the start of what went
// wrong.
func shortError(err error) string {
	var answered *providers.StatusError
	switch {
	case errors.As(err, &answered):
		return fmt.Sprintf("%d", answered.Status)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, errNotInTime):
		return "timeout"
	}
	message := err.Error()
	if len(message) > shownError {
		message = message[:shownError]
	}
	return message
}

var (
	errNotInTime  = errors.New("the provider did not answer in time")
	errStillAsked = errors.New("the provider has yet to answer the last time it was asked")
)

// Sample takes the sample of one time: it asks every provider what it
// sells, publishes what they answer to the gateways, and writes it to the
// history. A sample that another gateway took already is not taken again.
func (s *Sampler) Sample(ctx context.Context, at time.Time) {
	at = at.UTC()
	claimed, err := s.shared.ClaimTick(ctx, at, s.id)
	if err != nil {
		log.Warn().Err(err).Msg("market: could not mark a sample as taken; it is left out")
		return
	}
	if !claimed {
		return
	}

	answers := s.ask(ctx)
	polls := make(map[string]Poll, len(answers))
	for _, answer := range answers {
		polls[answer.provider] = answer.poll()
		if answer.err != nil {
			log.Warn().Err(answer.err).Str("provider", answer.provider).Msg("market: a provider did not say what it sells")
			continue
		}
		if err := s.shared.Publish(ctx, answer.provider, Latest{At: at, Offers: answer.offers}); err != nil {
			log.Warn().Err(err).Str("provider", answer.provider).Msg("market: could not publish what a provider sells")
		}
	}
	if err := s.shared.Polled(ctx, at, polls); err != nil {
		log.Warn().Err(err).Msg("market: could not publish how the providers answered")
	}

	samples := s.samplesOf(ctx, at, answers, polls)
	if s.log == nil && !s.dryRun {
		return
	}
	writes := s.records(at, answers, samples)
	if s.dryRun {
		s.report(answers, samples, writes)
		return
	}
	s.write(ctx, writes)
	s.rollUp(ctx, at, samples)
}

// ask asks every provider at once. A provider that does not answer in time
// is left behind: the sample is not held up for it.
func (s *Sampler) ask(ctx context.Context) []answer {
	names := s.providers.Names()
	answered := make(chan answer, len(names))
	got := map[string]answer{}
	waiting := 0
	for _, name := range names {
		provider, _ := s.providers.Get(name)
		if !s.begin(name) {
			got[name] = answer{provider: name, err: errStillAsked}
			continue
		}
		waiting++
		go func() {
			defer s.end(name)
			askCtx, cancel := context.WithTimeout(ctx, s.askFor)
			defer cancel()
			started := time.Now()
			offers, err := offersOf(askCtx, provider)
			answered <- answer{provider: name, offers: offers, err: err, took: time.Since(started)}
		}()
	}

	deadline := time.NewTimer(s.askFor + s.askGrace)
	defer deadline.Stop()
	for waiting > 0 {
		select {
		case a := <-answered:
			got[a.provider] = a
			waiting--
		case <-deadline.C:
			waiting = 0
		case <-ctx.Done():
			waiting = 0
		}
	}

	answers := make([]answer, 0, len(names))
	for _, name := range names {
		a, ok := got[name]
		if !ok {
			a = answer{provider: name, err: errNotInTime, took: s.askFor + s.askGrace}
		}
		answers = append(answers, a)
	}
	return answers
}

func (s *Sampler) begin(provider string) bool {
	s.askingMu.Lock()
	defer s.askingMu.Unlock()
	if s.asking[provider] {
		return false
	}
	s.asking[provider] = true
	return true
}

func (s *Sampler) end(provider string) {
	s.askingMu.Lock()
	defer s.askingMu.Unlock()
	delete(s.asking, provider)
}

// offersOf asks one provider for everything it sells. A provider's client
// that panics has failed to answer.
func offersOf(ctx context.Context, provider providers.Provider) (offers []providers.Offer, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			offers, err = nil, fmt.Errorf("listing offers panicked: %v", recovered)
		}
	}()
	return provider.Offers(ctx, providers.OfferFilter{})
}

// samplesOf puts what the providers answered together by GPU type: one
// sample for each type that is for sale, or was within seenFor.
//
// A provider lists only what it has. A listing that was for sale and is no
// longer listed, by a provider that answered, is written as a row with no
// offers, so that the record says the provider had none, and does not leave
// a reader to tell that from a sample that was not taken.
func (s *Sampler) samplesOf(ctx context.Context, at time.Time, answers []answer, polls map[string]Poll) []Sample {
	rows := map[string][]Row{}
	kinds := map[string]StockKind{}
	listed := map[string]bool{}
	var forSale []Seen
	for _, answer := range answers {
		if answer.err != nil {
			continue
		}
		kinds[answer.provider] = stockKindOf(answer.provider, answer.offers)
		for gpu, offers := range byGPU(answer.offers) {
			for _, row := range rowsOf(offers, kinds[answer.provider]) {
				rows[gpu] = append(rows[gpu], row)
				listed[seenMember(gpu, row.Listing)] = true
				forSale = append(forSale, Seen{GPU: gpu, Listing: row.Listing})
			}
		}
	}

	if err := s.shared.See(ctx, at, forSale); err != nil {
		log.Warn().Err(err).Msg("market: could not remember what is for sale")
	}
	seen, err := s.shared.Seen(ctx, at)
	if err != nil {
		log.Warn().Err(err).Msg("market: could not read what has been for sale; what is sold out is left out of this sample")
	}
	gpus := map[string]bool{}
	for gpu := range rows {
		gpus[gpu] = true
	}
	for _, was := range seen {
		gpus[was.GPU] = true
		if !polls[was.Provider].OK || listed[seenMember(was.GPU, was.Listing)] {
			continue
		}
		kind, ok := kinds[was.Provider]
		if !ok || kind == StockListed {
			// It listed nothing at all this time, or nothing with a count.
			kind = stockKindByName(was.Provider)
		}
		rows[was.GPU] = append(rows[was.GPU], Row{Listing: was.Listing, Kind: kind})
	}

	samples := make([]Sample, 0, len(gpus))
	for _, gpu := range slices.Sorted(maps.Keys(gpus)) {
		slices.SortFunc(rows[gpu], func(a, b Row) int { return compareListings(a.Listing, b.Listing) })
		samples = append(samples, Sample{At: at, GPU: gpu, Polls: polls, Rows: rows[gpu]})
	}
	return samples
}

// byGPU groups a provider's offers by the type of their GPU. Machines
// without one are in the provider's snapshots, and in no series.
func byGPU(offers []providers.Offer) map[string][]providers.Offer {
	grouped := map[string][]providers.Offer{}
	for _, offer := range offers {
		if offer.GPU == "" || offer.GPUCount == 0 || offer.HourlyCostMicros <= 0 {
			continue
		}
		grouped[offer.GPU] = append(grouped[offer.GPU], offer)
	}
	return grouped
}

// pendingWrite is a record on its way to a stream.
type pendingWrite struct {
	stream Stream
	record Record
	// provider is set for a provider's snapshot. full says that the
	// snapshot holds the provider's offers, which hash names.
	provider string
	full     bool
	hash     string
}

// records encodes what a sample writes: a snapshot for each provider and a
// series record for each GPU type.
func (s *Sampler) records(at time.Time, answers []answer, samples []Sample) []pendingWrite {
	writes := make([]pendingWrite, 0, len(answers)+len(samples))
	for _, answer := range answers {
		snapshot := Snapshot{At: at, Provider: answer.provider, Kind: SnapshotError, Poll: answer.poll(), Sampler: s.id}
		write := pendingWrite{stream: s.streams.Snapshots(answer.provider), provider: answer.provider}
		if answer.err == nil {
			encoded, err := encodeOffers(answer.offers)
			if err != nil {
				log.Warn().Err(err).Str("provider", answer.provider).Msg("market: could not encode what a provider sells")
				continue
			}
			snapshot.Hash = hashOf(encoded)
			last, written := s.written[answer.provider]
			if written && last.hash == snapshot.Hash && at.Sub(last.at) < keyframeEvery {
				snapshot.Kind, snapshot.Since = SnapshotSame, last.at
			} else {
				snapshot.Kind, snapshot.Offers = SnapshotFull, answer.offers
				write.full, write.hash = true, snapshot.Hash
			}
		}
		body, err := json.Marshal(snapshot)
		if err != nil {
			log.Warn().Err(err).Str("provider", answer.provider).Msg("market: could not encode a provider's snapshot")
			continue
		}
		write.record = Record{At: at, Body: body}
		writes = append(writes, write)
	}
	for _, sample := range samples {
		body, err := json.Marshal(sample)
		if err != nil {
			log.Warn().Err(err).Str("gpu", sample.GPU).Msg("market: could not encode a sample")
			continue
		}
		writes = append(writes, pendingWrite{stream: s.streams.Series(sample.GPU, TierMinute), record: Record{At: at, Body: body}})
	}
	return writes
}

// write appends the records, several streams at a time. A provider's offers
// that were written in full are remembered, so that the next sample of the
// same offers says only that they are the same.
func (s *Sampler) write(ctx context.Context, writes []pendingWrite) {
	var mu sync.Mutex
	var failed []error
	var group errgroup.Group
	group.SetLimit(writeConcurrency)
	for _, write := range writes {
		group.Go(func() error {
			err := s.log.Append(ctx, write.stream, write.record)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				failed = append(failed, fmt.Errorf("%s: %w", write.stream.Name, err))
			case write.full:
				s.written[write.provider] = writtenSnapshot{hash: write.hash, at: write.record.At}
			}
			return nil
		})
	}
	_ = group.Wait()
	if len(failed) > 0 {
		log.Warn().Err(failed[0]).Int("failed", len(failed)).Int("records", len(writes)).Msg("market: could not write all of a sample to its history")
	}
}

// report logs how much a sample would write, by provider and in all, and
// what that comes to in a day. It is what a dry run is for: the streams are
// sized from what the providers really list, before anything is written.
//
// A dry run writes every provider's offers in full every time, which is the
// most a sample can come to. A provider whose offers have not changed is
// written as a record of a few hundred bytes.
func (s *Sampler) report(answers []answer, samples []Sample, writes []pendingWrite) {
	snapshotBytes := map[string]int{}
	seriesBytes, seriesRecords, largest, total := 0, 0, 0, 0
	for _, write := range writes {
		size := len(write.record.Body)
		total += size
		largest = max(largest, size)
		if write.provider != "" {
			snapshotBytes[write.provider] = size
			continue
		}
		seriesBytes += size
		seriesRecords++
	}
	rows := map[string]int{}
	for _, sample := range samples {
		for _, row := range sample.Rows {
			rows[row.Provider]++
		}
	}
	for _, answer := range answers {
		log.Info().
			Str("provider", answer.provider).
			Bool("answered", answer.err == nil).
			Int64("took_ms", answer.took.Milliseconds()).
			Int("offers", len(answer.offers)).
			Int("series_rows", rows[answer.provider]).
			Int("snapshot_bytes", snapshotBytes[answer.provider]).
			Msg("market: dry run: what a provider's sample would write")
	}
	log.Info().
		Int("gpu_types", len(samples)).
		Int("series_records", seriesRecords).
		Int("series_bytes", seriesBytes).
		Int("records", len(writes)).
		Int("bytes", total).
		Int("largest_record_bytes", largest).
		Int64("bytes_a_day_at_most", int64(total)*int64(24*time.Hour/s.interval)).
		Msg("market: dry run: what a sample would write")
}

// rollUp puts the samples of every hour that has ended together, once, for
// every GPU type. It reads the samples back from their stream, so a gateway
// that takes the sampling over picks up where the last one stopped.
func (s *Sampler) rollUp(ctx context.Context, at time.Time, samples []Sample) {
	ended := at.Truncate(time.Hour)
	if !ended.After(s.rolledThrough) {
		return
	}
	done, err := s.shared.RolledUp(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("market: could not read which hours have been put together")
		return
	}
	gpus := map[string]bool{}
	for _, sample := range samples {
		gpus[sample.GPU] = true
	}
	for gpu := range done {
		gpus[gpu] = true
	}

	var failed error
	var mu sync.Mutex
	var group errgroup.Group
	group.SetLimit(writeConcurrency)
	for gpu := range gpus {
		group.Go(func() error {
			// A GPU type that is new starts with the hour that just ended.
			from := ended.Add(-time.Hour)
			if last, ok := done[gpu]; ok {
				from = last.Add(time.Hour)
			}
			if earliest := ended.Add(-rollUpAtMost * time.Hour); from.Before(earliest) {
				from = earliest
			}
			for hour := from; hour.Before(ended); hour = hour.Add(time.Hour) {
				if err := s.rollUpHour(ctx, gpu, hour); err != nil {
					mu.Lock()
					failed = err
					mu.Unlock()
					return nil
				}
			}
			return nil
		})
	}
	_ = group.Wait()
	if failed != nil {
		log.Warn().Err(failed).Msg("market: could not put an hour of samples together; it is tried again")
		return
	}
	s.rolledThrough = ended
}

func (s *Sampler) rollUpHour(ctx context.Context, gpu string, hour time.Time) error {
	samples, err := readSamples(ctx, s.log, s.streams.Series(gpu, TierMinute), gpu, hour, hour.Add(time.Hour))
	if err != nil {
		return fmt.Errorf("read the samples of %s: %w", gpu, err)
	}
	if len(samples) > 0 {
		body, err := json.Marshal(RollUp(gpu, hour, samples))
		if err != nil {
			return err
		}
		if err := s.log.Append(ctx, s.streams.Series(gpu, TierHour), Record{At: hour, Body: body}); err != nil {
			return fmt.Errorf("write the hour of %s: %w", gpu, err)
		}
	}
	return s.shared.SetRolledUp(ctx, gpu, hour)
}
