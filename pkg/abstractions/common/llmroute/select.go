package llmroute

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	affinityImbalanceThreshold       = 2
	spreadScoreMax             int64 = 128
	connectionWeight                 = 1000
	activeStreamWeight               = 1000
	tokenPressureWeight              = 100
	sessionAffinityBonus             = 900
	exactPrefixBonus                 = 700
	prefixBlockBonus                 = 250
)

// Candidate is a ready replica that could serve the request.
type Candidate struct {
	ID          string
	Connections int64         // in-flight proxied connections
	Pressure    Pressure      // router-tracked stream/token load
	Engine      EngineMetrics // latest engine metrics snapshot
	ContextLen  int64         // model context length used to normalize token pressure
	Payload     any           // opaque caller data returned with the selection
}

// Affinity describes which replicas previously served this session / prompt.
type Affinity struct {
	ExactID        string         // replica that last served the exact affinity key
	ExactIsSession bool           // ExactID came from a session key, not a prompt hash
	PrefixMatches  map[string]int // prefix-block hits per replica id
}

// Selection is the outcome of Select.
type Selection struct {
	Candidate Candidate
	Reason    string
}

type scored struct {
	Candidate
	score         int64
	queueDepth    int64
	reason        string
	prefixMatches int
}

// Selector picks replicas; the zero value is ready to use.
type Selector struct {
	Counter atomic.Uint64
}

// Select chooses the best candidate for info, applying affinity bonuses when
// load is balanced and falling back to power-of-two-choices otherwise.
func (s *Selector) Select(candidates []Candidate, affinity Affinity, info *RequestInfo) (Selection, bool) {
	if len(candidates) == 0 {
		return Selection{}, false
	}
	scoredCandidates := make([]scored, 0, len(candidates))
	for _, c := range candidates {
		scoredCandidates = append(scoredCandidates, scoreCandidate(c, affinity, info))
	}

	var selected scored
	switch {
	case len(scoredCandidates) == 1:
		selected = applyAffinity(scoredCandidates[0], affinity)
	case balanced(scoredCandidates):
		for i := range scoredCandidates {
			scoredCandidates[i] = applyAffinity(scoredCandidates[i], affinity)
		}
		selected = slices.MinFunc(scoredCandidates, compareScored)
	default:
		selected = s.powerOfTwo(scoredCandidates, info)
		selected.reason = "power_of_two_load"
		if affinity.ExactID != "" || slices.ContainsFunc(scoredCandidates, func(c scored) bool { return c.prefixMatches > 0 }) {
			selected.reason = "load_imbalance"
		}
	}

	if info != nil {
		info.RouteReason = selected.reason
		info.PrefixCacheMatches = selected.prefixMatches
	}
	return Selection{Candidate: selected.Candidate, Reason: selected.reason}, true
}

func compareScored(a, b scored) int {
	return cmp.Or(cmp.Compare(a.score, b.score), strings.Compare(a.ID, b.ID))
}

func scoreCandidate(c Candidate, affinity Affinity, info *RequestInfo) scored {
	contextLen := c.ContextLen
	if contextLen <= 0 {
		contextLen = defaultContextLen
	}
	load := c.Connections*connectionWeight +
		c.Pressure.ActiveStreams*activeStreamWeight +
		(c.Pressure.TokenPressure*tokenPressureWeight)/contextLen +
		c.Engine.score()
	return scored{
		Candidate:     c,
		score:         load + spreadScore(c.ID, info),
		queueDepth:    c.Connections + c.Pressure.ActiveStreams + c.Engine.RunningRequests + c.Engine.WaitingRequests,
		reason:        "least_pressure",
		prefixMatches: affinity.PrefixMatches[c.ID],
	}
}

func applyAffinity(c scored, affinity Affinity) scored {
	exact := affinity.ExactID != "" && affinity.ExactID == c.ID
	switch {
	case exact && affinity.ExactIsSession:
		c.score -= sessionAffinityBonus
		c.reason = "session_affinity"
	case exact:
		c.score -= exactPrefixBonus
		c.reason = "prefix_affinity"
	case c.prefixMatches > 0:
		c.score -= int64(c.prefixMatches) * prefixBlockBonus
		c.reason = "prefix_block_affinity"
	}
	return c
}

// balanced reports whether queue depths are close enough for affinity to override load.
func balanced(candidates []scored) bool {
	lo, hi := candidates[0].queueDepth, candidates[0].queueDepth
	for _, c := range candidates[1:] {
		lo, hi = min(lo, c.queueDepth), max(hi, c.queueDepth)
	}
	return hi-lo <= affinityImbalanceThreshold
}

// powerOfTwo deterministically samples two candidates and returns the less loaded one.
func (s *Selector) powerOfTwo(candidates []scored, info *RequestInfo) scored {
	key := ""
	if info != nil {
		key = strings.Join([]string{info.Model, info.Path, info.AffinityKey, info.RequestID}, "\n")
	}
	sum := sha256.Sum256([]byte(key + "\n" + strconv.FormatUint(s.Counter.Add(1), 10)))
	n := len(candidates)
	left := int(binary.BigEndian.Uint64(sum[:8]) % uint64(n))
	right := int(binary.BigEndian.Uint64(sum[8:16]) % uint64(n-1))
	if right >= left {
		right++
	}
	if compareScored(candidates[right], candidates[left]) < 0 {
		return candidates[right]
	}
	return candidates[left]
}

// spreadScore is a small deterministic jitter so equal-load replicas are spread by key.
func spreadScore(replicaID string, info *RequestInfo) int64 {
	if replicaID == "" || info == nil {
		return 0
	}
	key := cmp.Or(info.AffinityKey, info.PrefixHash, info.Model+":"+info.Path)
	hash := sha256.Sum256([]byte(key + "\n" + replicaID))
	return int64(binary.BigEndian.Uint16(hash[:2])) % spreadScoreMax
}
