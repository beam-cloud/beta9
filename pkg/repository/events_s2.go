package repository

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	defaultS2EventStreamPrefix = "events"
	defaultS2EventReadLimit    = 10000
	s2EventHistoryReadLimit    = uint64(1000)
	s2StreamResumeDelay        = time.Second
	s2EventAggregateScanLimit  = 50000
)

// S2EventRepository reads and writes events and logs in one S2 basin.
type S2EventRepository struct {
	s2StreamLayout
	basin        *s2.BasinClient
	writer       *s2EventWriter
	stubCache    stubCacheContents
	historySkips historySkips
}

// ScopedS2EventRepository writes events with tokens scoped to stream
// prefixes, each stream through the first target whose prefix holds it.
type ScopedS2EventRepository struct {
	targets []scopedS2EventTarget
	writer  *s2EventWriter
}

type scopedS2EventTarget struct {
	name   string
	prefix string
	basin  *s2.BasinClient
}

// Share the HTTP/2-capable transport across event clients. The SDK default
// disables automatic HTTP/2 and retains only two idle connections, so fleet
// fanout continually opens new TLS connections instead of reusing them.
func newS2EventClient(token string) *s2.Client {
	return s2.New(token, &s2.ClientOptions{
		HTTPClient:     &http.Client{Transport: http.DefaultTransport, Timeout: s2EventWriteTimeout},
		RequestTimeout: s2EventWriteTimeout,
		RetryConfig: &s2.RetryConfig{
			MaxAttempts:       3,
			AppendRetryPolicy: s2.AppendRetryPolicyAll,
		},
	})
}

func NewS2EventRepository(config types.S2Config) (*S2EventRepository, error) {
	if config.ApiKey == "" {
		return nil, nil
	}
	if config.Basin == "" {
		return nil, fmt.Errorf("s2 basin is required when s2 api key is configured")
	}

	repo := &S2EventRepository{
		s2StreamLayout: newS2StreamLayout(config.StreamPrefix),
		basin:          newS2EventClient(config.ApiKey).Basin(config.Basin),
	}
	repo.writer = newS2EventWriter("s2", repo.s2StreamLayout, func(s2.StreamName) *s2.BasinClient {
		return repo.basin
	})
	return repo, nil
}

func (r *S2EventRepository) PushEvent(event cloudevents.Event) error {
	return r.writer.push(event)
}

func (r *S2EventRepository) PushEventSync(event cloudevents.Event) error {
	return r.writer.write(event)
}

func NewScopedS2EventRepository(config types.S2Config) (*ScopedS2EventRepository, error) {
	if config.LogApiKey == "" && config.EventApiKey == "" {
		return nil, nil
	}
	if config.Basin == "" {
		return nil, fmt.Errorf("s2 basin is required when scoped s2 api keys are configured")
	}

	repo := &ScopedS2EventRepository{}
	addTarget := func(name, token, prefix string) {
		token = strings.TrimSpace(token)
		prefix = strings.Trim(strings.TrimSpace(prefix), "/")
		if token == "" || prefix == "" {
			return
		}
		repo.targets = append(repo.targets, scopedS2EventTarget{
			name:   name,
			prefix: prefix,
			basin:  newS2EventClient(token).Basin(config.Basin),
		})
	}
	addTarget("logs", config.LogApiKey, config.LogStreamPrefix)
	addTarget("events", config.EventApiKey, config.EventStreamPrefix)
	if len(repo.targets) == 0 {
		return nil, nil
	}

	repo.writer = newS2EventWriter("scoped s2", newS2StreamLayout(config.StreamPrefix), repo.basinForStream)
	return repo, nil
}

func (r *ScopedS2EventRepository) PushEvent(event cloudevents.Event) error {
	return r.writer.push(event)
}

func (r *ScopedS2EventRepository) PushEventSync(event cloudevents.Event) error {
	return r.writer.write(event)
}

func (r *ScopedS2EventRepository) basinForStream(streamName s2.StreamName) *s2.BasinClient {
	if target := r.targetForStream(streamName); target != nil {
		return target.basin
	}
	return nil
}

func (r *ScopedS2EventRepository) targetForStream(streamName s2.StreamName) *scopedS2EventTarget {
	stream := strings.Trim(string(streamName), "/")
	for i := range r.targets {
		target := &r.targets[i]
		if stream == target.prefix || strings.HasPrefix(stream, target.prefix+"/") {
			return target
		}
	}
	return nil
}
