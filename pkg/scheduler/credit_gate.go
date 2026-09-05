package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

const (
	// creditGateErrorBillingUnavailable is the error code stamped on a
	// decision that was produced locally because the billing service could
	// not be reached. It is never cached.
	creditGateErrorBillingUnavailable = "billing_unavailable"

	creditGateResponseLimit = 1 << 20
	creditGateEnforceLockS  = 120
)

// creditDecision is the billing service's answer to "may this workspace run
// serverless workloads right now?". It mirrors the JSON returned by the
// internal API's serverless access endpoint.
type creditDecision struct {
	OK             bool   `json:"ok"`
	ErrorCode      string `json:"error_code"`
	Message        string `json:"message"`
	AvailableCents int64  `json:"available_cents"`
	RequiredCents  int64  `json:"required_cents"`

	// CheckedAt is when the billing service produced the decision. Set by the
	// gate, not the service.
	CheckedAt time.Time `json:"checked_at"`
}

// Deny converts a negative decision into the error surfaced to callers.
func (d creditDecision) Deny(workspaceId string) error {
	if d.OK {
		return nil
	}
	return &types.InsufficientCreditsError{
		WorkspaceId: workspaceId,
		Code:        d.ErrorCode,
		Reason:      d.Message,
	}
}

// creditGateBackend fetches a fresh decision from billing.
type creditGateBackend interface {
	Check(ctx context.Context, workspaceId string) (creditDecision, error)
}

type httpCreditGateBackend struct {
	client   *http.Client
	endpoint string
	token    string
}

func (b *httpCreditGateBackend) Check(ctx context.Context, workspaceId string) (creditDecision, error) {
	body, err := json.Marshal(map[string]string{"workspace_id": workspaceId})
	if err != nil {
		return creditDecision{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.endpoint, bytes.NewReader(body))
	if err != nil {
		return creditDecision{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.token != "" {
		req.Header.Set("Authorization", "Bearer "+b.token)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return creditDecision{}, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, creditGateResponseLimit))
	if err != nil {
		return creditDecision{}, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return creditDecision{}, fmt.Errorf("credit gate request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}

	var decision creditDecision
	if err := json.Unmarshal(data, &decision); err != nil {
		return creditDecision{}, fmt.Errorf("credit gate returned malformed response: %w", err)
	}
	return decision, nil
}

// CreditGate decides whether a workspace has prepaid credit to run serverless
// workloads. Decisions come from the billing service and are cached in Redis
// so every gateway replica sees the same answer and billing is asked at most
// once per workspace per cache window.
//
// A nil *CreditGate is valid and allows everything.
type CreditGate struct {
	config  types.CreditGateConfig
	backend creditGateBackend
	rdb     *common.RedisClient
	lock    *common.RedisLock

	inflight singleflight.Group
	now      func() time.Time
}

// NewCreditGate returns nil when the gate is not enabled by config.
// fallbackToken is used when the gate has no token of its own.
func NewCreditGate(config types.CreditGateConfig, fallbackToken string, rdb *common.RedisClient) *CreditGate {
	if !config.Enabled() {
		return nil
	}

	token := strings.TrimSpace(config.AuthToken)
	if token == "" {
		token = strings.TrimSpace(fallbackToken)
	}

	return newCreditGate(config, &httpCreditGateBackend{
		client:   &http.Client{Timeout: config.TimeoutOrDefault()},
		endpoint: strings.TrimSpace(config.Endpoint),
		token:    token,
	}, rdb)
}

func newCreditGate(config types.CreditGateConfig, backend creditGateBackend, rdb *common.RedisClient) *CreditGate {
	gate := &CreditGate{
		config:  config,
		backend: backend,
		rdb:     rdb,
		now:     time.Now,
	}
	if rdb != nil {
		gate.lock = common.NewRedisLock(rdb)
	}
	return gate
}

func (g *CreditGate) Enabled() bool {
	return g != nil
}

// Check returns nil when the workspace may run and an
// *types.InsufficientCreditsError when it may not.
func (g *CreditGate) Check(ctx context.Context, workspaceId string) error {
	if g == nil || workspaceId == "" {
		return nil
	}

	decision, err := g.Decision(ctx, workspaceId)
	if err != nil {
		return err
	}
	return decision.Deny(workspaceId)
}

// Decision returns the current decision for a workspace, from cache when it
// is fresh, otherwise from billing. When billing is unreachable a stale
// cached decision is reused; with none available the configured fail-open
// policy decides. The error return is only non-nil when the gate fails
// closed, and is then an *types.InsufficientCreditsError.
func (g *CreditGate) Decision(ctx context.Context, workspaceId string) (creditDecision, error) {
	if g == nil {
		return creditDecision{OK: true}, nil
	}

	cached, hasCached := g.cached(ctx, workspaceId)
	if hasCached && g.fresh(cached) {
		return cached, nil
	}

	result, err, _ := g.inflight.Do(workspaceId, func() (any, error) {
		// Another caller may have refreshed while we waited on the flight.
		if cached, ok := g.cached(ctx, workspaceId); ok && g.fresh(cached) {
			return cached, nil
		}

		// Deliberately not derived from the caller's ctx: a decision is
		// shared by every waiter on this flight.
		fetchCtx, cancel := context.WithTimeout(context.Background(), g.config.TimeoutOrDefault())
		defer cancel()

		decision, err := g.backend.Check(fetchCtx, workspaceId)
		if err != nil {
			return g.unavailable(workspaceId, err)
		}

		decision.CheckedAt = g.now()
		g.store(ctx, workspaceId, decision)
		return decision, nil
	})
	if err != nil {
		return creditDecision{}, err
	}
	return result.(creditDecision), nil
}

// Invalidate drops the cached decision so the next check asks billing.
func (g *CreditGate) Invalidate(ctx context.Context, workspaceId string) {
	if g == nil || g.rdb == nil {
		return
	}
	if err := g.rdb.Del(ctx, common.RedisKeys.WorkspaceCreditGate(workspaceId)).Err(); err != nil {
		log.Warn().Err(err).Str("workspace_id", workspaceId).Msg("credit gate: failed to invalidate cached decision")
	}
}

func (g *CreditGate) unavailable(workspaceId string, cause error) (creditDecision, error) {
	if stale, ok := g.cached(context.Background(), workspaceId); ok {
		log.Warn().Err(cause).Str("workspace_id", workspaceId).Bool("stale_ok", stale.OK).
			Msg("credit gate: billing unreachable, reusing stale decision")
		return stale, nil
	}

	if g.config.FailOpenOrDefault() {
		log.Warn().Err(cause).Str("workspace_id", workspaceId).
			Msg("credit gate: billing unreachable with no cached decision, failing open")
		return creditDecision{OK: true, ErrorCode: creditGateErrorBillingUnavailable, CheckedAt: g.now()}, nil
	}

	log.Error().Err(cause).Str("workspace_id", workspaceId).
		Msg("credit gate: billing unreachable with no cached decision, failing closed")
	return creditDecision{}, &types.InsufficientCreditsError{
		WorkspaceId: workspaceId,
		Code:        creditGateErrorBillingUnavailable,
		Reason:      "unable to verify credit balance; try again shortly",
	}
}

// deniedDecisionTTL bounds how long a denial is served from cache. A stale
// approval costs us at most CacheTTL of compute; a stale denial costs a
// customer who has just paid a "no credits" refusal, which is the moment they
// decide whether to stay. Denials are therefore re-checked almost immediately;
// the cached copy still serves as the stale fallback when billing is down.
const deniedDecisionTTL = 2 * time.Second

func (g *CreditGate) fresh(decision creditDecision) bool {
	age := g.now().Sub(decision.CheckedAt)
	if !decision.OK {
		return age < deniedDecisionTTL
	}
	return age < g.config.CacheTTLOrDefault()
}

func (g *CreditGate) cached(ctx context.Context, workspaceId string) (creditDecision, bool) {
	if g.rdb == nil {
		return creditDecision{}, false
	}

	data, err := g.rdb.Get(ctx, common.RedisKeys.WorkspaceCreditGate(workspaceId)).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Warn().Err(err).Str("workspace_id", workspaceId).Msg("credit gate: failed to read cached decision")
		}
		return creditDecision{}, false
	}

	var decision creditDecision
	if err := json.Unmarshal(data, &decision); err != nil || decision.CheckedAt.IsZero() {
		return creditDecision{}, false
	}
	return decision, true
}

func (g *CreditGate) store(ctx context.Context, workspaceId string, decision creditDecision) {
	if g.rdb == nil {
		return
	}

	data, err := json.Marshal(decision)
	if err != nil {
		return
	}
	// The entry lives for the stale window; freshness is judged from CheckedAt.
	if err := g.rdb.Set(ctx, common.RedisKeys.WorkspaceCreditGate(workspaceId), data, g.config.StaleTTLOrDefault()).Err(); err != nil {
		log.Warn().Err(err).Str("workspace_id", workspaceId).Msg("credit gate: failed to cache decision")
	}
}

// withEnforcementLease runs fn while holding the cluster-wide enforcement
// lock so only one gateway replica sweeps at a time. Returns false when the
// lock is held elsewhere.
func (g *CreditGate) withEnforcementLease(ctx context.Context, fn func(context.Context) error) (bool, error) {
	if g == nil {
		return false, nil
	}
	if g.lock == nil {
		return true, fn(ctx)
	}

	err := g.lock.WithLease(ctx, common.RedisKeys.WorkspaceCreditGateEnforceLock(), common.RedisLockOptions{TtlS: creditGateEnforceLockS}, fn)
	if err != nil && common.IsRedisLockNotObtained(err) {
		return false, nil
	}
	return true, err
}
