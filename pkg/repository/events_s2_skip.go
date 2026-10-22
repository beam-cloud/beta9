package repository

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	s2HistorySkipTTL        = 10 * time.Minute
	s2HistorySkipMaxEntries = 4096
	s2HistorySkipMaxRanges  = 16
)

// s2HistorySkip lists sequence ranges of one stream in which no record
// passes a history query's filters. Records never change once appended, so a
// later read with the same filters can jump over these ranges instead of
// reading them again. Without this, a client polling from a start position
// far behind the tail re-reads everything up to the tail on every poll when
// its filters match little or nothing there.
type s2HistorySkip struct {
	ranges []s2SeqRange // sorted, disjoint and not adjacent
	usedAt time.Time
}

// s2SeqRange is the half-open sequence range [from, to).
type s2SeqRange struct {
	from, to uint64
}

// resume returns where a read that would start at seq should start instead,
// past any range covering seq, and the start of the next range, if any, so
// the read can stop where already-checked records begin.
func (s s2HistorySkip) resume(seq uint64) (start uint64, nextRange uint64, bounded bool) {
	for _, r := range s.ranges {
		if seq < r.from {
			return seq, r.from, true
		}
		if seq < r.to {
			seq = r.to
		}
	}
	return seq, 0, false
}

func (s s2HistorySkip) end() uint64 {
	if len(s.ranges) == 0 {
		return 0
	}
	return s.ranges[len(s.ranges)-1].to
}

// historySkipKey identifies the stream and filters skip ranges were found
// for. The time window, start position and limit only decide where a read
// starts and stops, so they are left out.
func historySkipKey(streamName s2.StreamName, query types.EventQuery) string {
	query.Limit = 0
	query.StartTime, query.EndTime = nil, nil
	query.SeqNum, query.Timestamp, query.TailOffset, query.Until = nil, nil, nil, nil
	query.WaitSeconds, query.Clamp = nil, nil
	filters, _ := json.Marshal(query)
	return string(streamName) + "\x00" + string(filters)
}

func (r *S2EventRepository) historySkip(key string) s2HistorySkip {
	r.historySkipsMu.Lock()
	defer r.historySkipsMu.Unlock()

	skip, ok := r.historySkips[key]
	if !ok {
		return s2HistorySkip{}
	}
	now := time.Now()
	if now.Sub(skip.usedAt) > s2HistorySkipTTL {
		delete(r.historySkips, key)
		return s2HistorySkip{}
	}
	skip.usedAt = now
	r.historySkips[key] = skip
	return s2HistorySkip{ranges: slices.Clone(skip.ranges)}
}

// rememberHistorySkip records that no record in [from, to) passes the
// filters for key. When more than s2HistorySkipMaxRanges ranges are known,
// the shortest is dropped, since it saves the least reading.
func (r *S2EventRepository) rememberHistorySkip(key string, from, to uint64) {
	if to <= from {
		return
	}

	r.historySkipsMu.Lock()
	defer r.historySkipsMu.Unlock()

	if r.historySkips == nil {
		r.historySkips = map[string]s2HistorySkip{}
	}
	now := time.Now()
	skip, ok := r.historySkips[key]
	if !ok && len(r.historySkips) >= s2HistorySkipMaxEntries {
		r.evictHistorySkipsLocked(now)
	}

	ranges := make([]s2SeqRange, 0, len(skip.ranges)+1)
	for _, known := range skip.ranges {
		if known.to < from || to < known.from {
			ranges = append(ranges, known)
			continue
		}
		from, to = min(from, known.from), max(to, known.to)
	}
	ranges = append(ranges, s2SeqRange{from: from, to: to})
	slices.SortFunc(ranges, func(a, b s2SeqRange) int { return cmp.Compare(a.from, b.from) })
	if len(ranges) > s2HistorySkipMaxRanges {
		shortest := 0
		for i, candidate := range ranges {
			if candidate.to-candidate.from < ranges[shortest].to-ranges[shortest].from {
				shortest = i
			}
		}
		ranges = slices.Delete(ranges, shortest, shortest+1)
	}
	r.historySkips[key] = s2HistorySkip{ranges: ranges, usedAt: now}
}

func (r *S2EventRepository) forgetHistorySkip(key string) {
	r.historySkipsMu.Lock()
	defer r.historySkipsMu.Unlock()
	delete(r.historySkips, key)
}

func (r *S2EventRepository) evictHistorySkipsLocked(now time.Time) {
	oldestKey, oldestUse := "", now
	for key, skip := range r.historySkips {
		if now.Sub(skip.usedAt) > s2HistorySkipTTL {
			delete(r.historySkips, key)
			continue
		}
		if !skip.usedAt.After(oldestUse) {
			oldestKey, oldestUse = key, skip.usedAt
		}
	}
	if len(r.historySkips) >= s2HistorySkipMaxEntries {
		delete(r.historySkips, oldestKey)
	}
}
