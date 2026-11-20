package llmroute

import (
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"sort"

	"github.com/cespare/xxhash/v2"
	lru "github.com/hashicorp/golang-lru/v2"
)

// tableSize is how many (prefix, replica) pairs one Table remembers: a few
// megabytes, and thousands of live conversations.
const tableSize = 1 << 16

// minSlack is how many more requests than the least loaded replica one may
// have in flight, at the least, and still be sent the prompts it holds. An
// engine runs a few requests in one batch about as fast as one, and a prompt
// that leaves its replica is computed again from its first token. With a
// slack of one, six conversations at a time over two replicas had two in
// ten move midway, each to compute 17,000 tokens again.
const minSlack = 4

// Replica is one place a request could run.
type Replica struct {
	ID     string
	Load   int64 // requests in flight on it
	Max    int64 // requests its engine runs at once; 0 is not known
	Queued int64 // requests its engine last said it had waiting for a slot
}

// full reports whether the replica's engine runs all it can at once: it has
// as many requests as it has slots, or it says that some wait for one. An
// engine whose slots are not known is known to be full only by its queue.
func (r Replica) full() bool {
	return (r.Max > 0 && r.Load >= r.Max) || r.Queued > 0
}

// Table remembers which replicas served which prompt prefixes, most recent
// first. It approximates the engines' KV caches from what this gateway
// routed; what another gateway routed is covered by the prompt's home.
type Table struct {
	served *lru.Cache[uint64, struct{}]
}

func NewTable() *Table {
	served, _ := lru.New[uint64, struct{}](tableSize) // fails only for a non-positive size
	return &Table{served: served}
}

// Rank orders replicas for a prompt, best first, as indexes into replicas.
// The caller takes the first one it can reserve.
func (t *Table) Rank(p Prompt, replicas []Replica) []int {
	key := p.Key
	if key == 0 {
		key = rand.Uint64() // nothing to keep together: spread evenly
	}
	least := int64(math.MaxInt64)
	for _, r := range replicas {
		least = min(least, r.Load)
	}
	type choice struct {
		index  int
		spill  bool // beyond the load bound: used only when nothing within it has room
		load   int64
		depth  int    // prefixes of the prompt this replica served
		weight uint64 // rendezvous weight; the highest is the prompt's home
	}
	choices := make([]choice, len(replicas))
	for i, r := range replicas {
		id := xxhash.Sum64String(r.ID)
		slack := int64(0)
		if p.Key != 0 {
			slack = max(minSlack, r.Max/4)
		}
		choices[i] = choice{
			index:  i,
			spill:  r.Load > least+slack || r.full(),
			load:   r.Load,
			depth:  t.depth(p, id),
			weight: mix(key ^ id),
		}
	}
	slices.SortFunc(choices, func(a, b choice) int {
		switch {
		case a.spill != b.spill:
			if a.spill {
				return 1
			}
			return -1
		case a.spill && a.load != b.load:
			return cmp.Compare(a.load, b.load)
		case a.depth != b.depth:
			return cmp.Compare(b.depth, a.depth)
		}
		return cmp.Compare(b.weight, a.weight)
	})
	order := make([]int, len(choices))
	for i, c := range choices {
		order[i] = c.index
	}
	return order
}

// Served records that a replica computed the prompt and now caches it.
func (t *Table) Served(p Prompt, replicaID string) {
	id := xxhash.Sum64String(replicaID)
	// Longest first, so the cache forgets a prompt from its end and what
	// remains is still a run of leading prefixes.
	for _, prefix := range slices.Backward(p.Prefixes) {
		t.served.Add(prefix^id, struct{}{})
	}
}

// depth is how many leading prefixes of the prompt the replica served.
func (t *Table) depth(p Prompt, id uint64) int {
	return sort.Search(len(p.Prefixes), func(i int) bool { return !t.served.Contains(p.Prefixes[i] ^ id) })
}

// mix is the splitmix64 finalizer: every input bit changes every output bit.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ x>>31
}
