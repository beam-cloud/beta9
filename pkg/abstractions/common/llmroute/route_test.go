package llmroute

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func read(t *testing.T, body, cacheKey string) Prompt {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &payload))
	return Read(payload, cacheKey)
}

func chat(messages ...string) string {
	return `{"model":"m","messages":[` + strings.Join(messages, ",") + `]}`
}

func message(role, content string) string {
	return fmt.Sprintf(`{"role":%q,"content":%q}`, role, content)
}

func idle(ids ...string) []Replica {
	replicas := make([]Replica, len(ids))
	for i, id := range ids {
		replicas[i] = Replica{ID: id, Max: 8}
	}
	return replicas
}

func xxhashOf(id string) uint64 { return xxhash.Sum64String(id) }

func first(t *Table, p Prompt, replicas []Replica) string {
	return replicas[t.Rank(p, replicas)[0]].ID
}

func TestReadFollowsAConversation(t *testing.T) {
	system := message("system", "You are terse.")
	turn1 := read(t, chat(system, message("user", "What is a KV cache?")), "")
	turn2 := read(t, chat(system, message("user", "What is a KV cache?"), message("assistant", "Attention state."), message("user", "Why reuse it?")), "")

	require.Len(t, turn1.Prefixes, 1, "a conversation is told apart at its first user message")
	require.Len(t, turn2.Prefixes, 3, "and then tracked message by message")
	assert.Equal(t, turn1.Prefixes[0], turn2.Prefixes[0], "a later turn extends the earlier prompt")
	assert.Equal(t, turn1.Key, turn2.Key, "a conversation keeps its key as it grows")
	assert.Equal(t, turn1.Prefixes[0], turn1.Key)

	for name, other := range map[string]string{
		"another question":      chat(system, message("user", "Name a prime.")),
		"another system prompt": chat(message("system", "You are verbose."), message("user", "What is a KV cache?")),
		"tools":                 `{"tools":[{"type":"function","function":{"name":"search"}}],"messages":[` + system + "," + message("user", "What is a KV cache?") + `]}`,
	} {
		assert.NotEqual(t, turn1.Key, read(t, other, "").Key, name)
	}

	reordered := read(t, `{"messages":[{"content":"You are terse.","role":"system"},{"content":"What is a KV cache?","role":"user"}],"model":"m"}`, "")
	assert.Equal(t, turn1, reordered, "field order does not change a prompt")

	keyed, sameKey := read(t, chat(system, message("user", "a")), "thread-1"), read(t, chat(system, message("user", "b")), "thread-1")
	assert.Equal(t, keyed.Key, sameKey.Key, "the caller's cache key names the conversation")
	assert.NotEqual(t, keyed.Key, read(t, chat(system, message("user", "a")), "").Key)

	preamble := read(t, chat(system), "")
	require.Len(t, preamble.Prefixes, 1, "a chat with no user message is one prefix")
	assert.Equal(t, preamble.Prefixes[0], preamble.Key)
	assert.Zero(t, Read(map[string]any{"input": "embed me"}, ""), "a request without a prompt has nothing to keep together")
}

func TestReadTracksBlocksOfLongText(t *testing.T) {
	document := strings.Repeat("retrieved context. ", 1200) // about 23 KB
	shared := func(a, b Prompt) (n int) {
		for n < len(a.Prefixes) && n < len(b.Prefixes) && a.Prefixes[n] == b.Prefixes[n] {
			n++
		}
		return n
	}

	// Completions that open alike share a key and all but their last block.
	a := read(t, fmt.Sprintf(`{"prompt":%q}`, document+"First question?"), "")
	b := read(t, fmt.Sprintf(`{"prompt":%q}`, document+"Second question?"), "")
	require.Greater(t, len(a.Prefixes), 5)
	assert.Equal(t, len(a.Prefixes)-1, shared(a, b))
	assert.Equal(t, a.Key, b.Key)

	// A long message late in a chat is tracked by block too.
	opening := []string{message("system", "s"), message("user", "summarize what the tool returns")}
	short := read(t, chat(opening...), "")
	long := read(t, chat(append(opening, message("tool", document))...), "")
	assert.Len(t, short.Prefixes, 1)
	assert.Greater(t, len(long.Prefixes), 5)
	assert.Equal(t, 1, shared(short, long))

	huge := Read(map[string]any{"prompt": strings.Repeat("x", 4<<20)}, "")
	assert.Len(t, huge.Prefixes, maxPrefixes, "the work per request is bounded")
}

func TestRankSendsAPromptHome(t *testing.T) {
	replicas := idle("a", "b", "c", "d")
	homes := map[string]int{}
	moved := 0
	for i := range 4000 {
		p := Prompt{Key: mix(uint64(i + 1))}
		home := first(NewTable(), p, replicas)
		require.Equal(t, home, first(NewTable(), p, replicas), "every gateway computes the same home")
		homes[home]++
		// Losing a replica moves only the prompts that lived on it.
		after := first(NewTable(), p, replicas[:3])
		if home != "d" && after != home {
			moved++
		}
	}
	for id, count := range homes {
		assert.InDelta(t, 1000, count, 150, "replica %s takes an even share", id)
	}
	assert.Zero(t, moved)
}

func TestRankFollowsTheConversation(t *testing.T) {
	system := message("system", strings.Repeat("policy ", 100))
	turn1 := read(t, chat(system, message("user", "hello")), "")
	turn2 := read(t, chat(system, message("user", "hello"), message("assistant", "hi"), message("user", "more")), "")
	turn3 := read(t, chat(system, message("user", "hello"), message("assistant", "hi"), message("user", "more"), message("assistant", "ok"), message("user", "again")), "")
	replicas := idle("a", "b", "c")
	table := NewTable()
	home := first(table, turn1, replicas)
	var away string
	for _, r := range replicas {
		if r.ID != home {
			away = r.ID
			break
		}
	}

	// The conversation ran away from home (it was busy): the next turn follows the cache.
	table.Served(turn1, away)
	assert.Equal(t, away, first(table, turn2, replicas))
	// The replica that served the most turns holds the most of the prompt.
	table.Served(turn2, home)
	assert.Equal(t, home, first(table, turn3, replicas))
	assert.Equal(t, 1, table.depth(turn3, xxhashOf(away)))
	assert.Equal(t, 3, table.depth(turn3, xxhashOf(home)))

	// Conversations that only share a system prompt each keep their own home,
	// so gateways agree on it without having seen each other's traffic.
	for i := range 50 {
		other := read(t, chat(system, message("user", fmt.Sprint("unrelated ", i))), "")
		assert.Equal(t, first(NewTable(), other, replicas), first(table, other, replicas))
	}
}

func TestRankBoundsLoad(t *testing.T) {
	p := read(t, chat(message("user", "hello")), "")
	table := NewTable()
	table.Served(p, "a")
	load := func(a, b int64) []Replica {
		return []Replica{{ID: "a", Load: a, Max: 8}, {ID: "b", Load: b, Max: 8}}
	}
	assert.Equal(t, "a", first(table, p, load(4, 0)), "a cached prefix is worth a few requests in the same batch")
	assert.Equal(t, "b", first(table, p, load(5, 0)), "past the bound the prompt spills")
	assert.Equal(t, "a", first(table, p, load(7, 3)), "the bound follows the least loaded replica")
	assert.Equal(t, "b", first(table, p, load(8, 7)), "a full replica is the last resort")
	assert.Equal(t, []int{0, 1}, table.Rank(p, load(8, 8)), "among equally full replicas the cache still decides")

	large := []Replica{{ID: "a", Load: 16, Max: 64}, {ID: "b", Max: 64}}
	assert.Equal(t, "a", first(table, p, large), "a large engine is worth a quarter of its slots")
	large[0].Load = 17
	assert.Equal(t, "b", first(table, p, large))

	// An engine of four slots keeps its prompts until it is full.
	small := []Replica{{ID: "a", Load: 3, Max: 4}, {ID: "b", Max: 4}}
	assert.Equal(t, "a", first(table, p, small))
	small[0].Load = 4
	assert.Equal(t, "b", first(table, p, small))

	unbounded := []Replica{{ID: "a", Load: 4}, {ID: "b"}}
	assert.Equal(t, "a", first(table, p, unbounded), "an engine that says nothing of its slots is taken to batch a few requests")
	unbounded[0].Load = 5
	assert.Equal(t, "b", first(table, p, unbounded))
	// Such an engine is full once it says that requests wait for a slot.
	queued := []Replica{{ID: "a", Load: 3, Queued: 1}, {ID: "b", Load: 2}}
	assert.Equal(t, "b", first(table, p, queued))
	queued[1].Queued = 2
	assert.Equal(t, "b", first(table, p, queued), "among full replicas the least loaded")
	queued[1].Load = 3
	assert.Equal(t, "a", first(table, p, queued), "and among those the one that holds the prompt")

	// A request with nothing to keep together goes to the least loaded replica.
	for range 20 {
		assert.Equal(t, "b", first(table, Prompt{}, load(1, 0)))
	}
	seen := map[string]bool{}
	for range 200 {
		seen[first(table, Prompt{}, load(0, 0))] = true
	}
	assert.Len(t, seen, 2, "equal replicas share such requests")
}

func TestTableForgetsAPromptFromItsEnd(t *testing.T) {
	table := NewTable()
	old := Prompt{Key: 1, Prefixes: []uint64{11, 12, 13, 14}}
	table.Served(old, "a")
	id := xxhashOf("a")
	for i := range tableSize - 2 { // leaves room for two of the four prefixes
		table.served.Add(mix(uint64(i))|1<<63, struct{}{})
	}
	assert.Equal(t, 2, table.depth(old, id), "what remains is still a leading run")
}
