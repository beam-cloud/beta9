// Package llmroute sends an LLM request to the replica that already holds
// its prompt in the engine's KV cache.
//
// vLLM and SGLang skip prefill for the longest prompt prefix they have
// computed before, so the time to first token of a long prompt depends on
// which replica receives it. Three rules decide, in this order:
//
//  1. Prefer the replica known to have served the longest prefix of the
//     prompt: the earlier turns of the same conversation. A Table remembers
//     which replica served what.
//  2. Otherwise prefer the conversation's home: rendezvous hashing of its key
//     over the replicas. Every gateway computes the same home with no shared
//     state, and a replica that leaves moves only its own conversations.
//  3. Never load a replica more than a bounded amount above the least loaded
//     one. Past that bound the request spills to the next replica in the
//     same order, so a busy home sheds work instead of queueing it.
//
// The package also reads an engine's readiness and Prometheus metrics.
package llmroute

import (
	"encoding/json"

	"github.com/cespare/xxhash/v2"
)

const (
	// blockSize is how finely a prefix is tracked inside one long message or
	// completion prompt, in bytes of its JSON: about a thousand tokens.
	blockSize = 4096
	// maxPrefixes bounds the work per request. A longer prompt is tracked to
	// this depth only.
	maxPrefixes = 256
)

// Prompt is what routing needs to know about a request.
type Prompt struct {
	// Key names the conversation: the caller's cache key when it sent one,
	// otherwise Prefixes[0]. Zero means the request has nothing to keep
	// together.
	Key uint64
	// Prefixes hash the prompt at successive points, shortest first. A chat
	// starts at the end of its first user message, the shortest prefix that
	// tells conversations apart, and adds one per later message; a completion
	// starts at its first block. Long text adds one per block. Two prompts
	// agree on Prefixes[i] exactly when they are equal up to that point.
	Prefixes []uint64
}

// Read fingerprints an OpenAI chat or completion body. cacheKey is the name
// the caller gave the conversation, if any.
func Read(body map[string]any, cacheKey string) Prompt {
	h := &prefixHasher{digest: xxhash.New()}
	encoder := json.NewEncoder(h) // map keys are written sorted, so equal values hash equally
	if tools, ok := body["tools"]; ok {
		_ = encoder.Encode(tools) // chat templates render tools ahead of the messages
	}
	if messages, ok := body["messages"].([]any); ok {
		for _, message := range messages {
			_ = encoder.Encode(message)
			if fields, _ := message.(map[string]any); fields["role"] == "user" {
				h.tracking = true
			}
			if h.tracking {
				h.mark()
			}
		}
	} else if prompt, ok := body["prompt"]; ok {
		h.tracking = true
		_ = encoder.Encode(prompt)
	}
	h.mark() // the end of a completion, or a chat with no user message
	p := Prompt{Prefixes: h.prefixes}
	switch {
	case cacheKey != "":
		p.Key = xxhash.Sum64String(cacheKey)
	case len(p.Prefixes) > 0:
		p.Key = p.Prefixes[0]
	}
	return p
}

// prefixHasher hashes a prompt as it is written and keeps the running hash
// at every mark and, once tracking, after every full block.
type prefixHasher struct {
	digest   *xxhash.Digest
	prefixes []uint64
	pending  int // bytes written since the last prefix
	tracking bool
}

func (h *prefixHasher) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 && len(h.prefixes) < maxPrefixes {
		take := len(p)
		if h.tracking {
			take = min(take, blockSize-h.pending)
		}
		_, _ = h.digest.Write(p[:take])
		p = p[take:]
		if h.pending += take; h.tracking && h.pending == blockSize {
			h.mark()
		}
	}
	return n, nil
}

func (h *prefixHasher) mark() {
	if h.pending > 0 && len(h.prefixes) < maxPrefixes {
		h.prefixes = append(h.prefixes, h.digest.Sum64())
	}
	h.pending = 0
}
