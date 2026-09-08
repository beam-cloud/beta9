package llmroute

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	// ID uniquely identifies the replica (container id).
	ID string
	// Connections is the number of in-flight proxied connections.
	Connections int64
	// Pressure is the router-tracked stream/token load for the replica.
	Pressure Pressure
	// Engine is the latest engine metrics snapshot for the replica.
	Engine EngineMetrics
	// ContextLen is the model context length used to normalize token pressure.
	ContextLen int64
	// Payload is opaque caller data returned with the selection.
	Payload any
}

// Affinity describes which replicas previously served this session / prompt.
type Affinity struct {
	// ExactID is the replica that last served the exact affinity key.
	ExactID string
	// ExactIsSession is true when ExactID came from a session key rather than a
	// prompt prefix hash.
	ExactIsSession bool
	// PrefixMatches counts prefix-block hits per replica id.
	PrefixMatches map[string]int
}

// Selection is the outcome of Select.
type Selection struct {
	Candidate     Candidate
	Score         int64
	Reason        string
	PrefixMatches int
}

type scored struct {
	Candidate
	score         int64
	queueDepth    int64
	reason        string
	prefixMatches int
}

// Selector picks replicas. The zero value is ready to use; Counter provides
// deterministic power-of-two sampling across calls.
type Selector struct {
	Counter atomic.Uint64
}

// Select chooses the best candidate for info, applying affinity bonuses when
// load is balanced and falling back to power-of-two-choices otherwise.
// It returns false when there are no candidates.
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
		sort.SliceStable(scoredCandidates, func(i, j int) bool {
			if scoredCandidates[i].score == scoredCandidates[j].score {
				return scoredCandidates[i].ID < scoredCandidates[j].ID
			}
			return scoredCandidates[i].score < scoredCandidates[j].score
		})
		selected = scoredCandidates[0]
	default:
		reason := "power_of_two_load"
		if hasAffinitySignal(scoredCandidates, affinity) {
			reason = "load_imbalance"
		}
		selected = s.powerOfTwo(scoredCandidates, info, reason)
	}

	if info != nil {
		info.RouteReason = selected.reason
		info.RouteScore = selected.score
		info.CandidateCount = len(candidates)
		info.PrefixCacheMatches = selected.prefixMatches
	}
	return Selection{
		Candidate:     selected.Candidate,
		Score:         selected.score,
		Reason:        selected.reason,
		PrefixMatches: selected.prefixMatches,
	}, true
}

func scoreCandidate(c Candidate, affinity Affinity, info *RequestInfo) scored {
	contextLen := c.ContextLen
	if contextLen <= 0 {
		contextLen = DefaultContextLen
	}
	loadScore := c.Connections*connectionWeight +
		c.Pressure.ActiveStreams*activeStreamWeight +
		(c.Pressure.TokenPressure*tokenPressureWeight)/contextLen +
		c.Engine.Score()

	return scored{
		Candidate:     c,
		score:         loadScore + spreadScore(c.ID, info),
		queueDepth:    c.Connections + c.Pressure.ActiveStreams + c.Engine.RunningRequests + c.Engine.WaitingRequests,
		reason:        "least_pressure",
		prefixMatches: affinity.PrefixMatches[c.ID],
	}
}

func applyAffinity(candidate scored, affinity Affinity) scored {
	if affinity.ExactID != "" && affinity.ExactID == candidate.ID {
		if affinity.ExactIsSession {
			candidate.score -= sessionAffinityBonus
			candidate.reason = "session_affinity"
		} else {
			candidate.score -= exactPrefixBonus
			candidate.reason = "prefix_affinity"
		}
	} else if candidate.prefixMatches > 0 {
		candidate.score -= int64(candidate.prefixMatches) * prefixBlockBonus
		candidate.reason = "prefix_block_affinity"
	}
	return candidate
}

func balanced(candidates []scored) bool {
	if len(candidates) < 2 {
		return true
	}
	minDepth, maxDepth := candidates[0].queueDepth, candidates[0].queueDepth
	for _, candidate := range candidates[1:] {
		minDepth = min(minDepth, candidate.queueDepth)
		maxDepth = max(maxDepth, candidate.queueDepth)
	}
	return maxDepth-minDepth <= affinityImbalanceThreshold
}

func hasAffinitySignal(candidates []scored, affinity Affinity) bool {
	if affinity.ExactID != "" {
		return true
	}
	for _, candidate := range candidates {
		if candidate.prefixMatches > 0 {
			return true
		}
	}
	return false
}

func (s *Selector) powerOfTwo(candidates []scored, info *RequestInfo, reason string) scored {
	if len(candidates) == 1 {
		candidates[0].reason = reason
		return candidates[0]
	}

	left, right := s.powerOfTwoIndices(len(candidates), info)
	selected := candidates[left]
	other := candidates[right]
	if other.score < selected.score || (other.score == selected.score && other.ID < selected.ID) {
		selected = other
	}
	selected.reason = reason
	return selected
}

func (s *Selector) powerOfTwoIndices(count int, info *RequestInfo) (int, int) {
	if count <= 1 {
		return 0, 0
	}

	var counter uint64
	if s != nil {
		counter = s.Counter.Add(1)
	} else {
		counter = uint64(time.Now().UnixNano())
	}

	key := ""
	if info != nil {
		key = strings.Join([]string{info.Model, info.Path, info.AffinityKey, info.RequestID}, "\n")
	}
	sum := sha256.Sum256([]byte(key + "\n" + strconv.FormatUint(counter, 10)))
	left := int(binary.BigEndian.Uint64(sum[:8]) % uint64(count))
	right := int(binary.BigEndian.Uint64(sum[8:16]) % uint64(count-1))
	if right >= left {
		right++
	}
	return left, right
}

// spreadScore adds a small deterministic jitter so equal-load replicas are
// spread by affinity key rather than always picking the lowest id.
func spreadScore(replicaID string, info *RequestInfo) int64 {
	if replicaID == "" || info == nil {
		return 0
	}

	key := info.AffinityKey
	if key == "" {
		key = info.PrefixHash
	}
	if key == "" {
		key = info.Model + ":" + info.Path
	}

	hash := sha256.Sum256([]byte(key + "\n" + replicaID))
	return int64(binary.BigEndian.Uint16(hash[:2])) % spreadScoreMax
}
