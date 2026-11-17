package market

import (
	"context"
	"encoding/json"
	"maps"
	"math"
	"slices"
	"time"
)

// heldAtMost is the longest a sampled price is taken to have stood when no
// sample follows it. A gap in the samples is not filled with the price
// before it.
const heldAtMost = 5 * time.Minute

// readSamples returns the samples of one GPU type that a stream holds for a
// time, the oldest first, with one sample for each time. A record that
// cannot be read, of a version to come, is left out.
func readSamples(ctx context.Context, log Log, stream Stream, gpu string, from, to time.Time) ([]Sample, error) {
	records, err := log.Read(ctx, stream, from, to)
	if err != nil {
		return nil, err
	}
	samples := make([]Sample, 0, len(records))
	for _, record := range records {
		var sample Sample
		if json.Unmarshal(record.Body, &sample) != nil || sample.GPU != gpu {
			continue
		}
		samples = append(samples, sample)
	}
	return oneOfEachTime(samples, func(s Sample) time.Time { return s.At }), nil
}

// readHours returns what the hours of a time came to for one GPU type, the
// oldest first.
func readHours(ctx context.Context, log Log, stream Stream, gpu string, from, to time.Time) ([]Hourly, error) {
	records, err := log.Read(ctx, stream, from, to)
	if err != nil {
		return nil, err
	}
	hours := make([]Hourly, 0, len(records))
	for _, record := range records {
		var hour Hourly
		if json.Unmarshal(record.Body, &hour) != nil || hour.GPU != gpu {
			continue
		}
		hours = append(hours, hour)
	}
	return oneOfEachTime(hours, func(h Hourly) time.Time { return h.At }), nil
}

// oneOfEachTime orders records by their time and keeps the first of each.
// Two gateways that both took themselves for the sampler can have written
// the same time twice.
func oneOfEachTime[T any](records []T, at func(T) time.Time) []T {
	slices.SortStableFunc(records, func(a, b T) int { return at(a).Compare(at(b)) })
	return slices.CompactFunc(records, func(a, b T) bool { return at(a).Equal(at(b)) })
}

// RollUp puts the samples of an hour together, which must be in order: for
// every listing, what its cheapest offer cost through the hour, and how much
// of the hour it was in stock.
func RollUp(gpu string, hour time.Time, samples []Sample) Hourly {
	type sums struct {
		row      HourRow
		listed   int
		weighted float64
		weight   float64
		counted  bool
	}
	end := hour.Add(time.Hour)
	listings := map[Listing]*sums{}
	asked := map[string]*PollHour{}
	took := map[string]int64{}

	for i, sample := range samples {
		// A price stands until the next sample.
		until := end
		if i+1 < len(samples) {
			until = samples[i+1].At
		}
		held := min(until.Sub(sample.At), heldAtMost).Seconds()
		if held <= 0 {
			held = 1
		}

		for provider, poll := range sample.Polls {
			tally, ok := asked[provider]
			if !ok {
				tally = &PollHour{}
				asked[provider] = tally
			}
			tally.Polls++
			if poll.OK {
				tally.Answered++
				took[provider] += poll.MS
			}
		}

		for _, row := range sample.Rows {
			sum, ok := listings[row.Listing]
			if !ok {
				sum = &sums{row: HourRow{Listing: row.Listing, MinStock: math.MaxUint32}}
				listings[row.Listing] = sum
			}
			sum.row.Kind = row.Kind
			if row.Kind == StockCounted {
				sum.counted = true
				sum.row.MinStock = min(sum.row.MinStock, row.Stock)
				sum.row.MaxStock = max(sum.row.MaxStock, row.Stock)
			}
			if !row.InStock() {
				continue
			}
			if sum.listed == 0 {
				sum.row.Open, sum.row.Min, sum.row.Max = row.Min, row.Min, row.Min
			}
			sum.listed++
			sum.row.Close = row.Min
			sum.row.Min = min(sum.row.Min, row.Min)
			sum.row.Max = max(sum.row.Max, row.Min)
			sum.row.Offer = row.Offer
			sum.weighted += float64(row.Min) * held
			sum.weight += held
		}
	}

	out := Hourly{At: hour, GPU: gpu, Polls: make(map[string]PollHour, len(asked)), Rows: make([]HourRow, 0, len(listings))}
	for provider, tally := range asked {
		if tally.Answered > 0 {
			tally.MS = took[provider] / int64(tally.Answered)
		}
		out.Polls[provider] = *tally
	}
	for _, listing := range slices.SortedFunc(maps.Keys(listings), compareListings) {
		sum := listings[listing]
		row := sum.row
		row.Answers = out.Polls[listing.Provider].Answered
		if !sum.counted {
			row.MinStock = 0
		}
		if sum.weight > 0 {
			row.Mean = int64(math.Round(sum.weighted / sum.weight))
		}
		if row.Answers > 0 {
			// A listing that was first seen in the middle of the hour is in
			// fewer samples than its provider answered.
			row.InStock = math.Round(float64(sum.listed)/float64(max(row.Answers, sum.listed))*1000) / 1000
			if sum.listed < row.Answers && sum.counted {
				row.MinStock = 0
			}
		}
		out.Rows = append(out.Rows, row)
	}
	return out
}
