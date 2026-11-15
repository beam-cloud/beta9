package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// A provider that has not answered a call in this long is treated as
	// having failed it.
	requestTimeout = 30 * time.Second

	maxAttempts = 3

	// The wait before a retry doubles from firstRetryWait. A provider that
	// asks for longer than maxRetryWait is not waited for: the call fails and
	// the caller comes back on its own schedule.
	firstRetryWait = 500 * time.Millisecond
	maxRetryWait   = 5 * time.Second

	maxResponseBytes = 32 << 20
	// A provider's refusal is kept whole up to maxErrorBodyBytes, so that a
	// client can read why, and shown up to shownErrorBytes.
	maxErrorBodyBytes = 64 << 10
	shownErrorBytes   = 1 << 10
)

// api calls one provider's JSON API. Every call has a timeout. A call is
// sent again when the provider says it is being rate limited, and when it
// failed in a way that leaves it safe to repeat.
type api struct {
	baseURL   string
	authorize authorizer
	client    *http.Client
	// sleep waits between two tries of a call. A test replaces it.
	sleep func(ctx context.Context, d time.Duration) error
}

// authorizer puts a provider's credentials on requests.
type authorizer struct {
	// add puts the credentials on a request. It fails when they have to be
	// fetched first, and cannot be.
	add func(ctx context.Context, req *http.Request) error
	// renew, set when the credentials can be fetched again, drops the ones
	// the provider just turned down.
	renew func()
}

// withHeader authorizes with a header that never changes.
func withHeader(name, value string) authorizer {
	return authorizer{add: func(_ context.Context, req *http.Request) error {
		req.Header.Set(name, value)
		return nil
	}}
}

// withoutCredentials authorizes nothing: for the call that fetches them.
func withoutCredentials() authorizer {
	return authorizer{add: func(context.Context, *http.Request) error { return nil }}
}

func newAPI(baseURL string, authorize authorizer) *api {
	return &api{baseURL: strings.TrimRight(baseURL, "/"), authorize: authorize, client: &http.Client{}, sleep: sleepContext}
}

type call struct {
	method string
	path   string
	body   any // sent as JSON when not nil
	// form is sent as a form when not nil, in place of body.
	form url.Values
	out  any // the JSON response is decoded into it when not nil

	// repeatable marks a call that does no harm when the provider receives
	// it twice. Reads always are. A call that buys something is not: when it
	// fails without an answer, the purchase may have gone through.
	repeatable bool
}

// errNoAnswer marks a call that ended without the provider's whole answer.
var errNoAnswer = errors.New("no answer from the provider")

// StatusError is a provider's answer with a status outside 2xx.
type StatusError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	shown := e.Body
	if len(shown) > shownErrorBytes {
		shown = shown[:shownErrorBytes]
	}
	// The cut can fall inside a character, and a provider can send what is
	// no text at all. This goes to logs and to users, which take text.
	shown = strings.ToValidUTF8(shown, "")
	return fmt.Sprintf("%s %s: status %d: %s", e.Method, e.Path, e.Status, shown)
}

// decode reads the body of a refusal into the shape the provider gives its
// errors, so that a client can look for a reason it knows. A body that is
// not JSON, such as a proxy's error page, is left unread and that is no
// failure: it names no reason this client knows, and the refusal is passed
// on as it is.
func (e *StatusError) decode(into any) {
	_ = json.Unmarshal([]byte(e.Body), into)
}

func (a *api) do(ctx context.Context, c call) error {
	var payload []byte
	switch {
	case c.form != nil:
		payload = []byte(c.form.Encode())
	case c.body != nil:
		var err error
		if payload, err = json.Marshal(c.body); err != nil {
			return err
		}
	}

	wait := firstRetryWait
	renewed := false
	for attempt := 1; ; attempt++ {
		retryAfter, err := a.send(ctx, c, payload)
		if err == nil {
			return nil
		}
		if statusOf(err) == http.StatusUnauthorized && a.authorize.renew != nil && !renewed {
			// The provider did nothing with the call. Fetched credentials
			// can stop being good before they said they would, so the call
			// goes once more, with new ones.
			a.authorize.renew()
			renewed = true
			attempt--
			continue
		}
		if attempt == maxAttempts || ctx.Err() != nil || !retryable(c, err) {
			return err
		}
		if retryAfter > maxRetryWait {
			return err
		}
		if err := a.sleep(ctx, max(wait, retryAfter)); err != nil {
			return err
		}
		wait *= 2
	}
}

// send makes one attempt. Next to an error it returns how long the provider
// asked the caller to wait, if it did.
func (a *api) send(ctx context.Context, c call, payload []byte) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, c.method, a.baseURL+c.path, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	switch {
	case c.form != nil:
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	case payload != nil:
		req.Header.Set("Content-Type", "application/json")
	}
	if err := a.authorize.add(ctx, req); err != nil {
		// Credentials that could not be fetched are not the provider's
		// answer to this call: the status a login ended with says nothing
		// about the call. The error is passed on as words, so that nothing
		// reads a status out of it, renews on it or sends the call again
		// on top of the login's own attempts.
		return 0, fmt.Errorf("%s %s: %v", c.method, c.path, err)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return 0, because(errNoAnswer, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return 0, because(errNoAnswer, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if len(data) > maxErrorBodyBytes {
			data = data[:maxErrorBodyBytes]
		}
		return retryAfter(resp.Header), &StatusError{Method: c.method, Path: c.path, Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	if c.out == nil || len(data) == 0 {
		return 0, nil
	}
	if err := json.Unmarshal(data, c.out); err != nil {
		return 0, fmt.Errorf("%s %s: decode response: %w", c.method, c.path, err)
	}
	return 0, nil
}

// retryable reports whether sending the call again can help and is safe.
func retryable(c call, err error) bool {
	status := statusOf(err)
	if status == http.StatusTooManyRequests {
		// A rate-limited call was turned away before it did anything.
		return true
	}
	if !c.repeatable && c.method != http.MethodGet {
		return false
	}
	// No answer, or the provider failed on its side.
	return errors.Is(err, errNoAnswer) || status >= 500
}

// retryAfter reads how many seconds the provider asked the caller to wait.
func retryAfter(header http.Header) time.Duration {
	seconds, err := strconv.Atoi(header.Get("Retry-After"))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// statusOf returns the HTTP status a provider answered with, or 0 when err is
// not such an answer.
func statusOf(err error) int {
	var status *StatusError
	if errors.As(err, &status) {
		return status.Status
	}
	return 0
}
