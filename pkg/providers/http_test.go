package providers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// flakyAPI answers each request with the next of its replies, and the last
// one from then on.
type flakyAPI struct {
	server *httptest.Server
	api    *api

	mu      sync.Mutex
	replies []reply
	seen    int
	waits   []time.Duration
}

type reply struct {
	status     int
	body       string
	retryAfter string
	// hangUp closes the connection without answering.
	hangUp bool
}

func newFlakyAPI(t *testing.T, replies ...reply) *flakyAPI {
	flaky := &flakyAPI{replies: replies}
	flaky.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flaky.mu.Lock()
		next := flaky.replies[min(flaky.seen, len(flaky.replies)-1)]
		flaky.seen++
		flaky.mu.Unlock()

		if next.hangUp {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		if next.retryAfter != "" {
			w.Header().Set("Retry-After", next.retryAfter)
		}
		w.WriteHeader(next.status)
		w.Write([]byte(next.body))
	}))
	t.Cleanup(flaky.server.Close)
	flaky.api = newAPI(flaky.server.URL+"/", withHeader("X-Key", "secret"))
	flaky.api.sleep = func(ctx context.Context, d time.Duration) error {
		flaky.waits = append(flaky.waits, d)
		return ctx.Err()
	}
	return flaky
}

func (f *flakyAPI) requests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen
}

var (
	ok          = reply{status: http.StatusOK, body: `{"id":"abc"}`}
	unavailable = reply{status: http.StatusServiceUnavailable, body: "try later"}
	hangUp      = reply{hangUp: true}
)

type answer struct {
	ID string `json:"id"`
}

func TestAPISendsJSONWithCredentialsAndDecodesTheAnswer(t *testing.T) {
	var got *http.Request
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Write([]byte(`{"id":"abc"}`))
	}))
	defer server.Close()
	api := newAPI(server.URL, withHeader("X-Key", "secret"))

	var out answer
	require.NoError(t, api.do(context.Background(), call{method: http.MethodPost, path: "/things?x=1", body: map[string]int{"n": 2}, out: &out}))

	require.Equal(t, "abc", out.ID)
	require.Equal(t, "/things?x=1", got.URL.RequestURI())
	require.Equal(t, "secret", got.Header.Get("X-Key"))
	require.Equal(t, "application/json", got.Header.Get("Content-Type"))
	require.Equal(t, "application/json", got.Header.Get("Accept"))
	require.JSONEq(t, `{"n":2}`, gotBody)
}

// A token endpoint takes a form, not JSON.
func TestAPISendsAFormWhenGivenOne(t *testing.T) {
	var contentType, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Write([]byte(`{"id":"abc"}`))
	}))
	defer server.Close()
	api := newAPI(server.URL, withoutCredentials())

	var out answer
	form := url.Values{"grant_type": {"urn:example:grant"}, "assertion": {"a.b c"}}
	require.NoError(t, api.do(context.Background(), call{method: http.MethodPost, form: form, out: &out}))

	require.Equal(t, "abc", out.ID)
	require.Equal(t, "application/x-www-form-urlencoded", contentType)
	require.Equal(t, "assertion=a.b+c&grant_type=urn%3Aexample%3Agrant", gotBody)
}

func TestAPIReadsAgainWhenTheProviderFailsOrDoesNotAnswer(t *testing.T) {
	for name, first := range map[string]reply{"failed": unavailable, "no answer": hangUp} {
		t.Run(name, func(t *testing.T) {
			flaky := newFlakyAPI(t, first, first, ok)

			var out answer
			require.NoError(t, flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things", out: &out}))
			require.Equal(t, "abc", out.ID)
			require.Equal(t, 3, flaky.requests())
			require.Equal(t, []time.Duration{500 * time.Millisecond, time.Second}, flaky.waits)
		})
	}
}

func TestAPIGivesUpAfterThreeAttempts(t *testing.T) {
	flaky := newFlakyAPI(t, unavailable)

	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
	require.Equal(t, http.StatusServiceUnavailable, statusOf(err))
	require.Equal(t, 3, flaky.requests())
}

func TestAPIDoesNotRepeatAPurchaseThatMayHaveGoneThrough(t *testing.T) {
	for name, first := range map[string]reply{"failed": unavailable, "no answer": hangUp} {
		t.Run(name, func(t *testing.T) {
			flaky := newFlakyAPI(t, first, ok)

			err := flaky.api.do(context.Background(), call{method: http.MethodPost, path: "/things", body: struct{}{}})
			require.Error(t, err)
			require.Equal(t, 1, flaky.requests())
			require.Empty(t, flaky.waits)
		})
	}
}

func TestAPIRepeatsAWriteThatIsSafeToRepeat(t *testing.T) {
	flaky := newFlakyAPI(t, hangUp, unavailable, ok)

	require.NoError(t, flaky.api.do(context.Background(), call{method: http.MethodDelete, path: "/things/1", repeatable: true}))
	require.Equal(t, 3, flaky.requests())
}

func TestAPIFetchesCredentialsAgainOnceWhenTheyAreTurnedDown(t *testing.T) {
	unauthorized := reply{status: http.StatusUnauthorized, body: "token expired"}
	purchase := call{method: http.MethodPost, path: "/things", body: struct{}{}}

	flaky := newFlakyAPI(t, unauthorized, ok)
	renewed := 0
	flaky.api.authorize.renew = func() { renewed++ }
	require.NoError(t, flaky.api.do(context.Background(), purchase))
	require.Equal(t, 2, flaky.requests())
	require.Equal(t, 1, renewed)
	require.Empty(t, flaky.waits)

	// New credentials that are turned down too are the wrong credentials.
	flaky = newFlakyAPI(t, unauthorized)
	renewed = 0
	flaky.api.authorize.renew = func() { renewed++ }
	err := flaky.api.do(context.Background(), purchase)
	require.Equal(t, http.StatusUnauthorized, statusOf(err))
	require.Equal(t, 2, flaky.requests())
	require.Equal(t, 1, renewed)
}

func TestAPIDoesNotRepeatACallTheProviderRefused(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusConflict} {
		flaky := newFlakyAPI(t, reply{status: status, body: "no"}, ok)

		err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
		require.Equal(t, status, statusOf(err))
		require.Equal(t, 1, flaky.requests())
	}
}

func TestAPIWaitsAsLongAsARateLimitAsks(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusTooManyRequests, retryAfter: "3"}, reply{status: http.StatusTooManyRequests}, ok)

	// A rate-limited call was turned away before it did anything, so even a
	// purchase is sent again.
	require.NoError(t, flaky.api.do(context.Background(), call{method: http.MethodPost, path: "/things", body: struct{}{}}))
	require.Equal(t, 3, flaky.requests())
	require.Equal(t, []time.Duration{3 * time.Second, time.Second}, flaky.waits)
}

func TestAPIDoesNotWaitOutALongRateLimit(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusTooManyRequests, retryAfter: "60"}, ok)

	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
	require.Equal(t, http.StatusTooManyRequests, statusOf(err))
	require.Equal(t, 1, flaky.requests())
	require.Empty(t, flaky.waits)
}

func TestAPIStopsWhenTheCallerGivesUp(t *testing.T) {
	flaky := newFlakyAPI(t, unavailable)
	ctx, cancel := context.WithCancel(context.Background())
	flaky.api.sleep = func(ctx context.Context, d time.Duration) error {
		cancel()
		return ctx.Err()
	}

	err := flaky.api.do(ctx, call{method: http.MethodGet, path: "/things"})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, flaky.requests())
}

func TestAPIGivesAProviderThirtySecondsToAnswer(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer server.Close()
	defer close(release)

	var deadline time.Duration
	api := newAPI(server.URL, withHeader("X-Key", "secret"))
	api.client = &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		until, has := req.Context().Deadline()
		require.True(t, has, "a call to a provider always has a deadline")
		deadline = time.Until(until)
		return nil, errors.New("not sent")
	})}

	require.Error(t, api.do(context.Background(), call{method: http.MethodPost, path: "/things"}))
	require.InDelta(t, requestTimeout.Seconds(), deadline.Seconds(), 1)
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// A refusal is kept whole, so that a client can read why. What is shown of
// it, in logs and to users, is its start.
func TestStatusErrorShowsTheStartOfALongAnswer(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusBadRequest, body: strings.Repeat("x", 5000) + "y"})

	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
	var status *StatusError
	require.ErrorAs(t, err, &status)
	require.Len(t, status.Body, 5001)
	require.Equal(t, shownErrorBytes, strings.Count(err.Error(), "x"))
	require.NotContains(t, err.Error(), "y")
	require.Equal(t, http.MethodGet, status.Method)
	require.Equal(t, "/things", status.Path)
}

func TestAPIReportsAnAnswerItCannotDecode(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusOK, body: "<html>"})

	var out answer
	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things", out: &out})
	require.ErrorContains(t, err, "GET /things: decode response")
	require.Equal(t, 1, flaky.requests(), "an answer that arrived is not asked for again")
}

// What is shown of a refusal goes into logs and into messages to users,
// which take text only. A cut can fall inside a character.
func TestStatusErrorShowsValidTextWhereTheCutSplitsACharacter(t *testing.T) {
	refusal := &StatusError{Method: http.MethodGet, Path: "/things", Status: http.StatusBadRequest, Body: "x" + strings.Repeat("é", shownErrorBytes)}
	shown := refusal.Error()
	require.True(t, utf8.ValidString(shown), shown)
	// One byte of x, then two for each é: the cut falls inside the 512th.
	require.Equal(t, shownErrorBytes/2-1, strings.Count(shown, "é"))

	// Nor is what a provider sent always text.
	garbled := &StatusError{Method: http.MethodGet, Path: "/things", Status: http.StatusBadGateway, Body: "bad \xff\xfe gateway"}
	require.Equal(t, "GET /things: status 502: bad  gateway", garbled.Error())
}
