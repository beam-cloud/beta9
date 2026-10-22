package repository

import (
	"cmp"
	"context"
	"encoding/json"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	historySkipTTL        = 10 * time.Minute
	historySkipMaxEntries = 4096
	historySkipMaxRanges  = 16
)

// historySkips remembers, per stream and history query filters, sequence
// ranges in which no record passes the filters. Records never change once
// appended, so later reads with the same filters jump over these ranges
// instead of reading them again. Without this, a client polling from far
// behind the tail re-reads everything up to it on every poll when its filters
// match little or nothing there.
type historySkips struct {
	mu      sync.Mutex
	entries map[string]*historySkipEntry
}

type historySkipEntry struct {
	ranges seqRanges
	// probe is a record read along with the ranges. A stream deleted and
	// recreated under the same name reuses sequence numbers, so the ranges
	// only hold while the stream still has this record.
	probe  s2.StreamPosition
	usedAt time.Time
}

// seqRange is the half-open sequence range [from, to).
type seqRange struct{ from, to uint64 }

// seqRanges is sorted, disjoint and not adjacent.
type seqRanges []seqRange

// historyScan is one forward read of a stream. It jumps over the ranges known
// when it started and remembers each stretch it finds in which no record
// passes the filters.
type historyScan struct {
	skips  *historySkips
	key    string
	ranges seqRanges
	probe  s2.StreamPosition
	run    seqRange
	inRun  bool
}

// historySkipKey identifies the stream and filters ranges are found for. The
// time window, start position and limit only decide where a read starts and
// stops, so they are left out.
func historySkipKey(streamName s2.StreamName, query types.EventQuery) string {
	query.Limit = 0
	query.StartTime, query.EndTime = nil, nil
	query.SeqNum, query.Timestamp, query.TailOffset, query.Until = nil, nil, nil, nil
	query.WaitSeconds, query.Clamp = nil, nil
	filters, _ := json.Marshal(query)
	return string(streamName) + "\x00" + string(filters)
}

// startHistoryScan starts a forward read of streamName for query, dropping the
// known ranges if the stream no longer has the record they were found with.
func (r *S2EventRepository) startHistoryScan(ctx context.Context, streamName s2.StreamName, query types.EventQuery) (*historyScan, error) {
	scan := r.historySkips.scan(historySkipKey(streamName, query))
	if len(scan.ranges) == 0 {
		return scan, nil
	}
	held, err := r.streamHolds(ctx, streamName, scan.probe)
	if err != nil {
		return nil, err
	}
	if !held {
		r.historySkips.forget(scan.key)
		scan.ranges = nil
	}
	return scan, nil
}

// streamHolds reports whether the stream still has the record at position.
func (r *S2EventRepository) streamHolds(ctx context.Context, streamName s2.StreamName, position s2.StreamPosition) (bool, error) {
	count := uint64(1)
	batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{SeqNum: &position.SeqNum, Count: &count})
	if isS2ReadEmpty(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(batch.Records) == 1 &&
		batch.Records[0].SeqNum == position.SeqNum &&
		batch.Records[0].Timestamp == position.Timestamp, nil
}

func (h *historySkips) scan(key string) *historyScan {
	h.mu.Lock()
	defer h.mu.Unlock()

	scan := &historyScan{skips: h, key: key}
	entry, ok := h.entries[key]
	if !ok {
		return scan
	}
	now := time.Now()
	if now.Sub(entry.usedAt) > historySkipTTL {
		delete(h.entries, key)
		return scan
	}
	entry.usedAt = now
	scan.ranges, scan.probe = slices.Clone(entry.ranges), entry.probe
	return scan
}

// add records that no record in r passes the filters for key, found while the
// stream had probe.
func (h *historySkips) add(key string, r seqRange, probe s2.StreamPosition) {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	entry, ok := h.entries[key]
	if !ok {
		if h.entries == nil {
			h.entries = map[string]*historySkipEntry{}
		}
		if len(h.entries) >= historySkipMaxEntries {
			h.evictLocked(now)
		}
		entry = &historySkipEntry{}
		h.entries[key] = entry
	}
	entry.ranges = entry.ranges.with(r)
	entry.probe, entry.usedAt = probe, now
}

func (h *historySkips) forget(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.entries, key)
}

// evictLocked makes room for an entry: expired entries go first, otherwise
// the least recently used.
func (h *historySkips) evictLocked(now time.Time) {
	oldestKey, oldestUse := "", now
	for key, entry := range h.entries {
		if now.Sub(entry.usedAt) > historySkipTTL {
			delete(h.entries, key)
			continue
		}
		if !entry.usedAt.After(oldestUse) {
			oldestKey, oldestUse = key, entry.usedAt
		}
	}
	if len(h.entries) >= historySkipMaxEntries {
		delete(h.entries, oldestKey)
	}
}

// next returns where a read meant to start at seq can start, past the range
// covering seq if any, and how many records it can read before the next range.
func (rs seqRanges) next(seq uint64) (start, count uint64) {
	start = seq
	for _, r := range rs {
		if start < r.from {
			return start, r.from - start
		}
		start = max(start, r.to)
	}
	return start, math.MaxUint64 - start
}

// with returns the ranges merged with r. Past historySkipMaxRanges the
// shortest is dropped, since it saves the least reading.
func (rs seqRanges) with(r seqRange) seqRanges {
	merged := make(seqRanges, 0, len(rs)+1)
	for _, known := range rs {
		if known.to < r.from || r.to < known.from {
			merged = append(merged, known)
			continue
		}
		r = seqRange{from: min(r.from, known.from), to: max(r.to, known.to)}
	}
	merged = append(merged, r)
	slices.SortFunc(merged, func(a, b seqRange) int { return cmp.Compare(a.from, b.from) })
	if len(merged) > historySkipMaxRanges {
		shortest := slices.MinFunc(merged, func(a, b seqRange) int { return cmp.Compare(a.to-a.from, b.to-b.from) })
		merged = slices.DeleteFunc(merged, func(r seqRange) bool { return r == shortest })
	}
	return merged
}

// resume returns where a read meant to start at seq should start and how many
// records it should read before the next known range. Records jumped over
// join the current stretch.
func (s *historyScan) resume(seq uint64) (start, count uint64) {
	start, count = s.ranges.next(seq)
	if start > seq {
		s.extend(seq, start)
	}
	return start, count
}

// observe records whether record passed the filters. A passing record ends
// the current stretch.
func (s *historyScan) observe(record s2.SequencedRecord, passed bool) {
	s.probe = s2.StreamPosition{SeqNum: record.SeqNum, Timestamp: record.Timestamp}
	if passed {
		s.flush()
		return
	}
	s.extend(record.SeqNum, record.SeqNum+1)
}

// flush remembers the current stretch.
func (s *historyScan) flush() {
	if s.inRun {
		s.skips.add(s.key, s.run, s.probe)
		s.inRun = false
	}
}

func (s *historyScan) extend(from, to uint64) {
	if !s.inRun {
		s.run, s.inRun = seqRange{from: from}, true
	}
	s.run.to = to
}
