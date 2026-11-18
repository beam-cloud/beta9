// Package providers buys machines from clouds and GPU marketplaces.
//
// A Provider reports what the provider knows: what it sells, and the state of
// what was bought. Pools, selectors and billing belong to its callers.
package providers

import (
	"bytes"
	"cmp"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type Provider interface {
	// Name is the key the provider is configured under, such as "shadeform".
	Name() string

	// Offers lists what matches the filter and can be bought now.
	Offers(ctx context.Context, filter OfferFilter) ([]Offer, error)

	// Create buys one machine. It fails with ErrOutOfStock when the provider
	// has none left and ErrQuota when the account may not buy more. A create
	// that ends without the provider's answer, which Unanswered reports, may
	// have bought the machine all the same: List returns it, with its tags,
	// once the provider knows of it.
	Create(ctx context.Context, req CreateRequest) (Instance, error)

	// Get fails with ErrNotFound once the provider no longer has the instance.
	Get(ctx context.Context, id string) (Instance, error)

	// List returns the instances this control plane created that the
	// provider still has.
	List(ctx context.Context) ([]Instance, error)

	// Extend moves the time at which the provider deletes the instance by
	// itself to until. Providers without such a timer do nothing.
	Extend(ctx context.Context, id string, until time.Time) error

	// Delete succeeds when the instance is gone, including when it already
	// was.
	Delete(ctx context.Context, id string) error
}

// OfferFilter narrows Offers. The zero filter matches everything.
type OfferFilter struct {
	GPUs    []string // any of these GPU types
	Regions []string // any of these regions
}

func (f OfferFilter) Matches(offer Offer) bool {
	if len(f.GPUs) > 0 && !slices.Contains(f.GPUs, offer.GPU) {
		return false
	}
	return len(f.Regions) == 0 || slices.Contains(f.Regions, offer.Region)
}

// matching returns the offers the filter lets through, in the order they
// came in.
func (f OfferFilter) matching(offers []Offer) []Offer {
	matching := make([]Offer, 0, len(offers))
	for _, offer := range offers {
		if f.Matches(offer) {
			matching = append(matching, offer)
		}
	}
	return matching
}

// Offer is one kind of machine in one region.
type Offer struct {
	// ID is the provider's name for what is sold. With Cloud and Region it
	// tells Create what to buy.
	ID       string
	Provider string
	// Cloud owns the hardware. It differs from Provider when the provider
	// resells other clouds.
	Cloud        string
	InstanceType string
	Region       string

	GPU           string // Beam's name for the GPU type; empty without a GPU
	GPUCount      uint32
	CPUMillicores int64
	MemoryMB      int64
	StorageMB     int64

	HourlyCostMicros int64
	// Spot marks a machine the provider may take back at any time, sold for
	// less. Create buys the offer as it is listed: spot when Spot is set.
	Spot bool

	// Available is how many machines can be bought now, when the provider
	// counts them. Offers returns only what is in stock, so zero means the
	// provider gave no count, not that there are none.
	Available   uint32
	Reliability float64

	DisplayName       string
	Category          string
	RegionDisplayName string
	Latitude          float64
	Longitude         float64
}

type CreateRequest struct {
	Offer Offer

	// Name is a hostname: lowercase letters, digits and dashes, at most 63
	// characters. A provider with a tighter limit shortens it.
	Name string

	// StartupScript is shell that runs as root when the machine first
	// boots. A provider may run it again on a later boot, as GCP does.
	StartupScript string

	// Tags are stored with the instance and returned by Get and List.
	Tags map[string]string

	// Until is the time at which a provider that can delete an instance by
	// itself is told to delete this one. It is passed on as it is: a caller
	// that wants a margin adds it. Zero means no end.
	Until time.Time

	// MaxHourlyCostMicros is the most the buyer pays per hour for a spot
	// machine, where the provider can be held to it. AWS and Nebius take a
	// price limit and are given this one. GCP, Verda and Shadeform take
	// none: a spot machine there pays the provider's spot price whatever it
	// rises to, and this is not looked at. Nor is it on Vast, where the
	// price bid is the price paid: Vast is bid the host's price of the
	// moment, and rents nothing that has come to cost more than the offer
	// said. Zero means no limit.
	MaxHourlyCostMicros int64
}

// Instance is what a provider says about a machine that was bought.
type Instance struct {
	ID     string
	Status Status
	// StatusDetail is the provider's own wording, such as "Running startup
	// script".
	StatusDetail string

	PublicIP string
	SSHHost  string
	SSHPort  uint32

	Tags map[string]string
}

type Status string

const (
	StatusPending Status = "pending" // bought, not running yet
	StatusActive  Status = "active"  // running
	StatusFailed  Status = "failed"  // the provider gave up on it; it still exists
	StatusDeleted Status = "deleted" // being deleted, or gone

	// StatusInterrupted: the provider took a spot machine back. What is left
	// of it, a stopped machine or its disk, may exist and cost money until
	// it is deleted.
	StatusInterrupted Status = "interrupted"

	// StatusUnknown is a status the client has no mapping for. It says
	// nothing about the instance, least of all that it is running.
	StatusUnknown Status = "unknown"
)

// DollarsToMicros converts a price to the millionths of a dollar that offers
// are priced in.
func DollarsToMicros(amount float64) int64 {
	return int64(math.Round(amount * 1_000_000))
}

var (
	// ErrOutOfStock: the provider has none of the offer left.
	ErrOutOfStock = errors.New("out of stock")
	// ErrQuota: the account has reached a limit and may not buy more.
	ErrQuota = errors.New("quota exceeded")
	// ErrNotFound: the provider no longer has the instance.
	ErrNotFound = errors.New("instance not found")
	// ErrDisabled: the provider is listed under providers.disabled, and
	// sells nothing new. Nothing was bought.
	ErrDisabled = errors.New("provider is disabled for new purchases")
)

// LaunchGrace is how long after a purchase its provider may not yet know of
// the machine it sold. An instance that is not found before then is asked
// for again, not given up.
const LaunchGrace = 2 * time.Minute

// because wraps one of the errors above with the provider's own explanation.
func because(kind error, cause error) error {
	return fmt.Errorf("%w: %w", kind, cause)
}

// notFound is the error of a call that asked for one instance. A provider
// that answers 404 has no such instance, which is ErrNotFound.
func notFound(err error) error {
	if statusOf(err) == http.StatusNotFound {
		return because(ErrNotFound, err)
	}
	return err
}

// unlessGone is the error of a call that deleted something. A provider that
// answers 404 no longer has it, and what is gone needs no deleting.
func unlessGone(err error) error {
	if statusOf(err) == http.StatusNotFound {
		return nil
	}
	return err
}

// refusal names why a provider turned a create down. kind reads the reason
// from the provider's answer, whose body is decoded into the shape the
// provider gives its errors, and returns nil when the answer names none it
// knows. That error is passed on as it is, and so is one that is not the
// provider's answer.
func refusal[T any](err error, kind func(answer *StatusError, body T) error) error {
	var answer *StatusError
	if !errors.As(err, &answer) {
		return err
	}
	var body T
	answer.decode(&body)
	if reason := kind(answer, body); reason != nil {
		return because(reason, err)
	}
	return err
}

// OwnerTag marks an instance with the control plane that bought it. List
// returns only instances that carry this control plane's value, so two
// deployments that share a provider account do not see each other's machines.
const OwnerTag = "beam-owner"

// Tag keys and values stay inside the strictest provider's label rules, so
// every provider can store every tag: lowercase letters, digits and dashes,
// at most 63 of them, with a letter or a digit first and last. A key begins
// on a letter, which Google asks of a label's key and of no value.
var (
	tagKeyPattern   = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)
	tagValuePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// checkOwner refuses an owner that some provider could not store as a tag's
// value. An owner does not change, so it is asked once: by the registry,
// before it makes a client.
func checkOwner(owner string) error {
	if !tagValuePattern.MatchString(owner) {
		return fmt.Errorf("provider owner %q must be lowercase letters, digits and dashes, at most 63 characters", owner)
	}
	return nil
}

// ownedTags returns the tags to store with a new instance: the caller's, plus
// the owner.
func ownedTags(owner string, tags map[string]string) (map[string]string, error) {
	if _, reserved := tags[OwnerTag]; reserved {
		return nil, fmt.Errorf("tag %q is set by the provider client", OwnerTag)
	}
	for key, value := range tags {
		if !tagKeyPattern.MatchString(key) || !tagValuePattern.MatchString(value) {
			return nil, fmt.Errorf("tag %q=%q must be lowercase letters, digits and dashes, at most 63 characters, and its key must begin with a letter", key, value)
		}
	}
	owned := maps.Clone(tags)
	if owned == nil {
		owned = map[string]string{}
	}
	owned[OwnerTag] = owner
	return owned, nil
}

// callerTags drops the owner from an instance's stored tags, leaving what
// the caller passed to Create.
func callerTags(stored map[string]string) map[string]string {
	tags := maps.Clone(stored)
	delete(tags, OwnerTag)
	if len(tags) == 0 {
		return nil
	}
	return tags
}

// tagWords writes tags for a provider that keeps words with an instance,
// and no pairs: each tag as the word "key=value", in the order of the keys.
// Keys and values have neither spaces nor equals signs.
func tagWords(tags map[string]string) []string {
	words := make([]string, 0, len(tags))
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		words = append(words, key+"="+tags[key])
	}
	return words
}

// wordTags reads tags back from such words. A word that is not a tag, as
// one somebody else wrote, is left out.
func wordTags(words []string) map[string]string {
	tags := map[string]string{}
	for _, word := range words {
		if key, value, ok := strings.Cut(word, "="); ok {
			tags[key] = value
		}
	}
	return tags
}

// Registry holds the providers a control plane can buy from. It is built
// once, at startup.
type Registry struct {
	byName map[string]Provider
}

// NewRegistry builds a client for every provider the config has credentials
// for. Owner identifies this control plane on the instances they buy. A
// provider the config disables keeps its client and sells nothing new.
//
// It does not fail, so that a gateway starts whatever its config says of
// providers. What is wrong is logged as an error, once, here: an owner that
// some provider could not store, for which no client is made, and a provider
// whose credentials are incomplete or cannot be read, which is left out.
func NewRegistry(config types.ProviderConfig, owner string) *Registry {
	registry := RegistryOf()
	if err := checkOwner(owner); err != nil {
		log.Error().Err(err).Msg("no provider is used: this control plane's name cannot be put on the machines it buys")
		return registry
	}
	if config.Shadeform.ApiKey != "" {
		registry.add(NewShadeform(config.Shadeform, owner))
	}
	if config.Hetzner.ApiToken != "" {
		registry.add(NewHetzner(config.Hetzner, owner))
	}
	if config.Vast.ApiKey != "" {
		registry.add(NewVast(config.Vast, owner))
	}

	// The others sign in with more than one thing, or buy in places that
	// are named. One that has some of that is meant to be used, and its
	// client says what it lacks.
	if verda := config.Verda; verda.ClientID != "" || verda.ClientSecret != "" {
		client, err := NewVerda(verda, owner)
		registry.use(verdaName, client, err)
	}
	if nebius := config.Nebius; nebius.ServiceAccountID != "" || nebius.PublicKeyID != "" || nebius.PrivateKey != "" || len(nebius.Projects) > 0 {
		client, err := NewNebius(nebius, owner)
		registry.use(nebiusName, client, err)
	}
	// AWS may have its credentials from where the gateway runs, so a
	// region to buy in is enough.
	if aws := config.AWS; len(aws.Regions) > 0 || aws.AccessKeyID != "" || aws.SecretAccessKey != "" {
		client, err := NewAWS(aws, owner)
		registry.use(awsName, client, err)
	}
	if gcp := config.GCP; gcp.ServiceAccountKey != "" || gcp.Project != "" || len(gcp.Regions) > 0 {
		client, err := NewGCP(gcp, owner)
		registry.use(gcpName, client, err)
	}
	for _, written := range config.Disabled {
		registry.disable(written)
	}
	return registry
}

// disable stops the provider written under providers.disabled from selling.
// One that has no credentials sells nothing as it is.
func (r *Registry) disable(written string) {
	name := strings.ToLower(strings.TrimSpace(written))
	if provider, configured := r.byName[name]; configured && r.IsEnabled(name) {
		r.byName[name] = disabledProvider{Provider: provider}
	}
}

// disabledProvider is a provider that sells nothing new: it lists no offers
// and refuses every create, without a call to the provider. What was bought
// from it before is still read, kept for longer and deleted through its
// client. Leaving the client out would strand those machines.
type disabledProvider struct{ Provider }

func (p disabledProvider) Offers(context.Context, OfferFilter) ([]Offer, error) {
	return nil, nil
}

func (p disabledProvider) Create(context.Context, CreateRequest) (Instance, error) {
	return Instance{}, fmt.Errorf("%s: %w", p.Name(), ErrDisabled)
}

// IsEnabled reports whether the provider is configured and sells new
// machines. Get also returns a disabled provider, for what it sold before.
func (r *Registry) IsEnabled(name string) bool {
	provider, ok := r.Get(name)
	if !ok {
		return false
	}
	_, disabled := provider.(disabledProvider)
	return !disabled
}

// RegistryOf holds exactly the given providers.
func RegistryOf(providers ...Provider) *Registry {
	registry := &Registry{byName: map[string]Provider{}}
	for _, provider := range providers {
		registry.add(provider)
	}
	return registry
}

func (r *Registry) add(provider Provider) {
	r.byName[provider.Name()] = provider
}

// use adds the client of a provider that could be given one. For one that
// could not, it logs why, and the provider is left out.
func (r *Registry) use(name string, client Provider, err error) {
	if err != nil {
		log.Error().Err(err).Str("provider", name).Msg("provider is left out: its credentials are incomplete or cannot be read")
		return
	}
	r.add(client)
}

// Get returns the provider configured under name.
func (r *Registry) Get(name string) (Provider, bool) {
	if r == nil {
		return nil, false
	}
	provider, ok := r.byName[name]
	return provider, ok
}

// Names lists the providers that sell new machines, in a fixed order. It is
// what discovery and purchases go by.
func (r *Registry) Names() []string {
	return slices.DeleteFunc(r.ConfiguredNames(), func(name string) bool { return !r.IsEnabled(name) })
}

// ConfiguredNames lists every provider that has a client, in a fixed order,
// the disabled ones too. It is what looking after machines goes by: what a
// disabled provider sold before, and what it may have sold without saying,
// is still to be read and deleted.
func (r *Registry) ConfiguredNames() []string {
	if r == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(r.byName))
}

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
	// rawOut receives the response without JSON decoding. A provider whose
	// response is not JSON must opt in and validate it itself.
	rawOut *[]byte

	// repeatable marks a call that does no harm when the provider receives
	// it twice. Reads always are. A call that buys something is not: when it
	// fails without an answer, the purchase may have gone through.
	repeatable bool
}

// errNoAnswer marks a call that ended without the provider's whole answer.
var errNoAnswer = errors.New("no answer from the provider")

// Unanswered reports whether err leaves open what became of a call: the
// provider gave no answer, or answered that it failed on its side. What the
// call asked for may have been done all the same.
func Unanswered(err error) bool {
	return errors.Is(err, errNoAnswer) || statusOf(err) >= http.StatusInternalServerError
}

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

// payload is what the call sends: its form, or else its body as JSON.
func (c call) payload() ([]byte, error) {
	switch {
	case c.form != nil:
		return []byte(c.form.Encode()), nil
	case c.body != nil:
		return json.Marshal(c.body)
	}
	return nil, nil
}

func (a *api) do(ctx context.Context, c call) error {
	payload, err := c.payload()
	if err != nil {
		return err
	}

	wait := firstRetryWait
	renewed := false
	for attempt := 1; ; attempt++ {
		asked, err := a.send(ctx, c, payload)
		if statusOf(err) == http.StatusUnauthorized && a.authorize.renew != nil && !renewed {
			// The provider did nothing with the call. Fetched credentials
			// can stop being good before they said they would, so the call
			// goes once more, with new ones, and that is not a retry.
			a.authorize.renew()
			renewed = true
			asked, err = a.send(ctx, c, payload)
		}
		if err == nil {
			return nil
		}
		if attempt == maxAttempts || ctx.Err() != nil || !retryable(c, err) || asked > maxRetryWait {
			return err
		}
		if err := a.sleep(ctx, max(wait, asked)); err != nil {
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
	if c.rawOut != nil {
		*c.rawOut = data
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
	if statusOf(err) == http.StatusTooManyRequests {
		// A rate-limited call was turned away before it did anything.
		return true
	}
	if !c.repeatable && c.method != http.MethodGet {
		return false
	}
	return Unanswered(err)
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

// A token is replaced this long before it expires, so that a call sent with
// it arrives while it is still good.
const renewTokenBefore = 2 * time.Minute

// accessToken holds the short-lived token a provider trades its credentials
// for, and fetches the next one when it is about to expire.
type accessToken struct {
	// fetch trades the credentials for a token and says how long it lasts.
	fetch func(ctx context.Context) (token string, lifetime time.Duration, err error)
	now   func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time
}

func newAccessToken(now func() time.Time, fetch func(ctx context.Context) (string, time.Duration, error)) *accessToken {
	return &accessToken{fetch: fetch, now: now}
}

func (t *accessToken) get(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token != "" && t.now().Before(t.expires.Add(-renewTokenBefore)) {
		return t.token, nil
	}
	token, lifetime, err := t.fetch(ctx)
	if err != nil {
		return "", fmt.Errorf("get an access token: %w", err)
	}
	if token == "" {
		return "", fmt.Errorf("get an access token: the provider sent none")
	}
	t.token, t.expires = token, t.now().Add(lifetime)
	return token, nil
}

// forget drops the token, so that the next call fetches one.
func (t *accessToken) forget() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = ""
}

// bearer authorizes with the token as a bearer token.
func (t *accessToken) bearer() authorizer {
	return authorizer{
		add: func(ctx context.Context, req *http.Request) error {
			token, err := t.get(ctx)
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		},
		renew: t.forget,
	}
}

// grant asks a token endpoint for an access token, which OAuth has it answer
// with the token and the seconds it lasts. What is asked with, and whether
// as JSON or as a form, is the provider's own. Asking twice does no harm.
func grant(ctx context.Context, login *api, request call) (token string, lifetime time.Duration, err error) {
	var granted struct {
		AccessToken string  `json:"access_token"`
		ExpiresIn   float64 `json:"expires_in"` // seconds
	}
	request.method, request.out, request.repeatable = http.MethodPost, &granted, true
	err = login.do(ctx, request)
	return granted.AccessToken, time.Duration(granted.ExpiresIn * float64(time.Second)), err
}

// rsaKey reads a service account's private key from PEM, in either of the
// two forms such keys are handed out in. A key that went through an
// environment variable or a JSON file has its line breaks written as \n.
func rsaKey(text string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(strings.ReplaceAll(text, `\n`, "\n")))
	if block == nil {
		return nil, fmt.Errorf("the private key is not in PEM form")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("read the private key: %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("the private key is not an RSA key")
	}
	return key, nil
}

// signedJWT returns the claims as a JSON Web Token signed with the key,
// RS256. It is what a service account proves itself with when it asks for an
// access token.
func signedJWT(key *rsa.PrivateKey, keyID string, claims map[string]any) (string, error) {
	header := map[string]string{"alg": "RS256", "typ": "JWT"}
	if keyID != "" {
		header["kid"] = keyID
	}
	encode := func(part any) (string, error) {
		data, err := json.Marshal(part)
		return base64.RawURLEncoding.EncodeToString(data), err
	}
	head, err := encode(header)
	if err != nil {
		return "", err
	}
	body, err := encode(claims)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(head + "." + body))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return head + "." + body + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// dollars is a price a provider writes in dollars, as a number or as a
// string such as "1.50". It holds the price in micros. A price the provider
// left empty is zero.
type dollars int64

func (d *dollars) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "" || text == "null" {
		*d = 0
		return nil
	}
	amount, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return fmt.Errorf("price %s is not a number of dollars", data)
	}
	*d = dollars(DollarsToMicros(amount))
	return nil
}

// count is a whole number a provider writes as a number or as a string such
// as "8". One the provider left empty is zero.
type count int64

func (c *count) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "" || text == "null" {
		*c = 0
		return nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("%s is not a whole number", data)
	}
	*c = count(n)
	return nil
}

const (
	// defaultDiskGB is the size of a machine's disk where the config names
	// none.
	defaultDiskGB = 200
	// hoursPerMonth is the month that a disk's price is for.
	hoursPerMonth = 730
)

// hourlyDiskMicros is what a disk of gb gigabytes costs an hour, in micros,
// at a price for a gigabyte for a month. It is for the providers that have
// no API for the price of a disk, where the price is their list's.
func hourlyDiskMicros(gb, perGBMonth int64) int64 {
	return gb * perGBMonth / hoursPerMonth
}

// cached remembers an answer that changes rarely.
type cached[T any] struct {
	mu     sync.Mutex
	value  T
	loaded time.Time
}

// get returns the remembered answer, and loads it when there is none or it
// is older than maxAge.
func (c *cached[T]) get(now time.Time, maxAge time.Duration, load func() (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded.IsZero() && now.Sub(c.loaded) < maxAge {
		return c.value, nil
	}
	value, err := load()
	if err != nil {
		return value, err
	}
	c.value, c.loaded = value, now
	return value, nil
}

func (c *cached[T]) forget() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loaded = time.Time{}
}

// noDeletionTimer is part of the client of a provider that has no time at
// which it deletes an instance by itself, or none that can be moved. For
// such a provider Extend does nothing.
type noDeletionTimer struct{}

func (noDeletionTimer) Extend(ctx context.Context, id string, until time.Time) error { return nil }

// unknownStatus is what a status this client has no mapping for is read as.
// It is logged, so that the mapping gets written.
func unknownStatus(provider, status string) Status {
	log.Warn().Str("provider", provider).Str("status", status).Msg("provider reported a status this client does not know")
	return StatusUnknown
}

// statusDetail is the detail of a machine whose provider says no more of it
// than its status: the provider's own word for the status, kept for a
// machine that failed, was taken back or is in a status this client does
// not know. Those are the ones somebody has to look into.
func statusDetail(status Status, wording string) string {
	switch status {
	case StatusFailed, StatusInterrupted, StatusUnknown:
		return wording
	}
	return ""
}

// sshPort is where a machine's SSH server listens, unless its provider says
// otherwise.
const sshPort = 22

// reachableAt records a machine's public address, where its SSH server
// listens too. A machine that has no address yet is left without one.
func (i *Instance) reachableAt(address string) {
	if address != "" {
		i.PublicIP, i.SSHHost, i.SSHPort = address, address, sshPort
	}
}

// maxPages is how many pages of one list are read. No account has a list
// that long: a provider that sends more is sending pages without end, and a
// client that read on would never return.
const maxPages = 1000

// tooManyPages is the error of a list that went past maxPages.
func tooManyPages(list string) error {
	return fmt.Errorf("%s: the provider sent more than %d pages of a list that should have ended", list, maxPages)
}

// offersByRegion gathers what each of the regions sells that matches the
// filter. A region that cannot be read is logged and left out, so that the
// others can still be bought in. When none of them can be read, that is the
// error, and it says what each lacks.
func offersByRegion(provider string, regions []string, filter OfferFilter, read func(region string) ([]Offer, error)) ([]Offer, error) {
	var offers []Offer
	var unread []error
	asked := 0
	for _, region := range regions {
		if len(filter.Regions) > 0 && !slices.Contains(filter.Regions, region) {
			continue
		}
		asked++
		sold, err := read(region)
		if err != nil {
			log.Warn().Err(err).Str("provider", provider).Str("region", region).Msg("could not read what a region sells; its offers are left out")
			unread = append(unread, err)
			continue
		}
		offers = append(offers, filter.matching(sold)...)
	}
	if asked > 0 && len(unread) == asked {
		return nil, errors.Join(unread...)
	}
	return offers, nil
}

// sortOffers puts offers in one order: by region, GPU and number of GPUs,
// and among those the cheapest first. A buyer chooses by its own rules; the
// order is there so that a client that gathers its offers from several
// answers lists the same stock the same way twice.
func sortOffers(offers []Offer) {
	slices.SortFunc(offers, func(a, b Offer) int {
		return cmp.Or(
			cmp.Compare(a.Region, b.Region),
			cmp.Compare(a.GPU, b.GPU),
			cmp.Compare(a.GPUCount, b.GPUCount),
			cmp.Compare(a.HourlyCostMicros, b.HourlyCostMicros),
			cmp.Compare(a.ID, b.ID),
		)
	})
}

func containsAny(text string, words ...string) bool {
	return slices.ContainsFunc(words, func(word string) bool { return strings.Contains(text, word) })
}

// gpuName is Beam's name for a GPU as a provider's catalog names it, or the
// catalog's own name when Beam has none.
func gpuName(catalog string) types.GpuType {
	key := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToUpper(r)
		}
		return -1
	}, catalog)
	switch {
	case containsAny(key, "GB200", "GB300"):
		// Not the B200 or B300 its name contains. Beam has no name for it.
		return types.GpuType(strings.TrimSpace(catalog))
	case strings.Contains(key, "RTXPROSERVER6000"):
		// What EC2 calls the GPU of its g7e instances.
		return types.GPU_RTX_PRO6000
	}
	return types.NormalizeGPUType(catalog)
}

// bootScript wraps a create request's startup commands as the script a
// provider runs when the machine boots.
func bootScript(commands string) string {
	return fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\n%s\n", commands)
}

// Fake is a provider that exists only in memory. Tests and the scheduler
// simulator buy from it, and decide what it has in stock and what becomes of
// the machines it sold.
type Fake struct {
	name  string
	owner string

	mu      sync.Mutex
	offers  []Offer
	refusal error
	// allowed is how many more creates succeed before the refusal applies.
	allowed   int
	deleteErr error
	instances map[string]*fakeInstance
	sold      int
	creates   []CreateRequest
	deletes   []string
	// unanswered is how many more creates are carried out and then reported
	// as failed.
	unanswered int
}

type fakeInstance struct {
	Instance
	owner string
	until time.Time
	offer Offer
}

func NewFake(name, owner string) *Fake {
	return &Fake{name: name, owner: owner, instances: map[string]*fakeInstance{}}
}

func (f *Fake) Name() string { return f.name }

// SetOffers replaces what the fake sells. An offer with a count sells out.
func (f *Fake) SetOffers(offers ...Offer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offers = slices.Clone(offers)
	for i := range f.offers {
		f.offers[i].Provider = f.name
	}
}

// RefuseCreates makes every create fail with err, until it is called with nil.
func (f *Fake) RefuseCreates(err error) {
	f.RefuseCreatesAfter(0, err)
}

// RefuseCreatesAfter lets the next n creates through and makes the ones after
// them fail with err: a provider whose catalogue promised more than it had.
func (f *Fake) RefuseCreatesAfter(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refusal, f.allowed = err, n
}

// LoseAnswers makes the next n creates succeed at the fake and fail for the
// caller: a request that timed out after the provider had acted on it.
func (f *Fake) LoseAnswers(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unanswered = n
}

// FailDeletes makes every delete fail with err, until it is called with nil.
func (f *Fake) FailDeletes(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteErr = err
}

// Plant puts an instance at the fake that owner bought some other way, and
// returns its ID.
func (f *Fake) Plant(owner string, status Status, tags map[string]string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add(owner, status, tags).ID
}

// SetStatus changes what the fake reports about an instance.
func (f *Fake) SetStatus(id string, status Status, detail string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		instance.Status, instance.StatusDetail = status, detail
	}
}

// SetAddress gives an instance the public address the fake reports for it.
func (f *Fake) SetAddress(id, publicIP string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		instance.PublicIP = publicIP
	}
}

// Forget removes an instance as if the provider had lost it.
func (f *Fake) Forget(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.instances, id)
}

// Everything returns every instance at the fake, whoever bought it, in the
// order they were sold.
func (f *Fake) Everything() []Instance {
	f.mu.Lock()
	defer f.mu.Unlock()
	instances := make([]Instance, 0, len(f.instances))
	for _, instance := range f.instances {
		instances = append(instances, instance.Instance)
	}
	slices.SortFunc(instances, func(a, b Instance) int { return compareIDs(a.ID, b.ID) })
	return instances
}

// Sold returns the offer an instance was bought from. It is the zero offer
// for an instance that was planted.
func (f *Fake) Sold(id string) Offer {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		return instance.offer
	}
	return Offer{}
}

// Creates returns every create request the fake accepted.
func (f *Fake) Creates() []CreateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.creates)
}

// Deletes returns the ID of every delete the fake received, failed or not.
func (f *Fake) Deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.deletes)
}

// Until returns when the instance is set to delete itself.
func (f *Fake) Until(id string) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if instance, ok := f.instances[id]; ok {
		return instance.until
	}
	return time.Time{}
}

func (f *Fake) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return filter.matching(f.offers), nil
}

func (f *Fake) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := ownedTags(f.owner, req.Tags); err != nil {
		return Instance{}, err
	}
	if f.refusal != nil {
		if f.allowed == 0 {
			return Instance{}, f.refusal
		}
		f.allowed--
	}
	sold, err := f.sell(req.Offer)
	if err != nil {
		return Instance{}, err
	}
	instance := f.add(f.owner, StatusPending, req.Tags)
	instance.until = req.Until
	instance.offer = sold
	f.creates = append(f.creates, req)
	if f.unanswered > 0 {
		f.unanswered--
		return Instance{}, because(errNoAnswer, fmt.Errorf("%s: create %s: the connection was reset before the answer arrived", f.name, req.Name))
	}
	return instance.Instance, nil
}

// sell takes one machine out of the offer's stock and returns the offer as
// the fake holds it now, which may differ from what the buyer last saw.
func (f *Fake) sell(wanted Offer) (Offer, error) {
	for i, offer := range f.offers {
		if offer.ID != wanted.ID || offer.Region != wanted.Region || offer.Cloud != wanted.Cloud || offer.Spot != wanted.Spot {
			continue
		}
		switch offer.Available {
		case 0: // not counted: never sells out
		case 1:
			f.offers = slices.Delete(f.offers, i, i+1)
		default:
			f.offers[i].Available--
		}
		return offer, nil
	}
	return Offer{}, because(ErrOutOfStock, fmt.Errorf("%s has no %s in %s", f.name, wanted.ID, wanted.Region))
}

func (f *Fake) add(owner string, status Status, tags map[string]string) *fakeInstance {
	f.sold++
	instance := &fakeInstance{
		Instance: Instance{ID: fmt.Sprintf("%s-%d", f.name, f.sold), Status: status, Tags: maps.Clone(tags)},
		owner:    owner,
	}
	f.instances[instance.ID] = instance
	return instance
}

func (f *Fake) Get(ctx context.Context, id string) (Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	instance, ok := f.instances[id]
	if !ok {
		return Instance{}, because(ErrNotFound, fmt.Errorf("%s has no instance %s", f.name, id))
	}
	return instance.Instance, nil
}

func (f *Fake) List(ctx context.Context) ([]Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	instances := []Instance{}
	for _, instance := range f.instances {
		if instance.owner == f.owner {
			instances = append(instances, instance.Instance)
		}
	}
	slices.SortFunc(instances, func(a, b Instance) int { return compareIDs(a.ID, b.ID) })
	return instances, nil
}

func (f *Fake) Extend(ctx context.Context, id string, until time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	instance, ok := f.instances[id]
	if !ok {
		return fmt.Errorf("%s has no instance %s", f.name, id)
	}
	instance.until = until
	return nil
}

func (f *Fake) Delete(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, id)
	if f.deleteErr != nil {
		return f.deleteErr
	}
	delete(f.instances, id)
	return nil
}

// compareIDs orders the fake's IDs by the order they were sold in.
func compareIDs(a, b string) int {
	return cmp.Or(cmp.Compare(len(a), len(b)), cmp.Compare(a, b))
}
