package common

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	RequestIDHeader        = "x-beta9-request-id"
	RequestAckHeader       = "x-beta9-request-ack"
	requestReplayTTL       = 5 * time.Minute
	requestJournalLimit    = 8192
	requestJournalBytes    = 64 << 20
	requestJournalInFlight = 16
)

type requestResult struct {
	fingerprint [32]byte
	done        chan struct{}
	response    interface{}
	err         error
	expires     time.Time
	size        int
}

// RequestJournal belongs to the operation's owner, not the gateway connection.
// Acknowledgements release responses only after the SDK has received them.
type RequestJournal struct {
	mu       sync.Mutex
	entries  map[string]*requestResult
	bytes    int
	inFlight int
}

func (j *RequestJournal) Do(ctx context.Context, id, method string, request proto.Message, acknowledgements []string, run func(context.Context) (interface{}, error)) (interface{}, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, err
	}
	fingerprint := sha256.Sum256(append([]byte(method), encoded...))
	j.mu.Lock()
	if j.entries == nil {
		j.entries = make(map[string]*requestResult)
	}
	for _, ack := range acknowledgements {
		if entry := j.entries[ack]; entry != nil && !entry.expires.IsZero() {
			j.bytes -= entry.size
			delete(j.entries, ack)
		}
	}
	for key, entry := range j.entries {
		if !entry.expires.IsZero() && time.Now().After(entry.expires) {
			j.bytes -= entry.size
			delete(j.entries, key)
		}
	}
	entry, found := j.entries[id]
	if found && entry.fingerprint != fingerprint {
		j.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, "request ID reused for a different operation")
	}
	if !found {
		if len(j.entries) >= requestJournalLimit || j.bytes >= requestJournalBytes || j.inFlight >= requestJournalInFlight {
			j.mu.Unlock()
			return nil, status.Error(codes.ResourceExhausted, "too many unacknowledged sandbox operations")
		}
		entry = &requestResult{fingerprint: fingerprint, done: make(chan struct{})}
		j.entries[id] = entry
		j.inFlight++
		go func() {
			// A gateway disconnect must not cancel an operation already accepted.
			operationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			entry.response, entry.err = run(operationCtx)
			j.mu.Lock()
			if response, ok := entry.response.(proto.Message); ok {
				entry.size = proto.Size(response)
				j.bytes += entry.size
			}
			j.inFlight--
			entry.expires = time.Now().Add(requestReplayTTL)
			close(entry.done)
			j.mu.Unlock()
		}()
	}
	j.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-entry.done:
		return entry.response, entry.err
	}
}
