package managedendpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// billing is the one accounting path. A Charge is opened when work is
// accepted with the caller, app version and price snapshotted; settling it
// prices what the app reported and journals it; accounting applies the
// journal to the usage counters, the credit gate and the billing meter, then
// closes the journal entry. Synchronous requests settle inline; queued tasks
// settle from the task record, here or on read. Anything left pending after
// a crash is finished by the next flush.

const (
	chargeTTL       = time.Hour // journal retention after accounting
	billingLockKey  = "managed_endpoint:meter"
	billingLockTTL  = 30 * time.Second
	billingInterval = 5 * time.Second
	pendingBatch    = 100
	taskLostAfter   = 48 * time.Hour // an open charge whose task record vanished
)

type billing struct {
	s    *Service
	lock *common.RedisLock
}

func newBilling(s *Service) *billing { return &billing{s: s, lock: common.NewRedisLock(s.rdb)} }

// newCharge snapshots the caller and the published price of one accepted request.
func newCharge(id string, app *types.ManagedEndpoint, caller *types.Workspace, tokenID string, route types.EndpointRoute, now time.Time) *types.Charge {
	return &types.Charge{
		ID: id, Status: types.ChargeOpen, WorkspaceID: caller.ExternalId, TokenID: tokenID,
		AppID: app.Spec.ID, Version: app.Version, StubType: string(app.StubType), Route: route, Pricing: app.Pricing, AcceptedAt: now.UTC(),
	}
}

// attribute records the serving replica on the charge, including the
// provider's share when it ran on a contributed machine.
func (b *billing) attribute(c *types.Charge, replica *types.EndpointReplica) {
	if replica == nil {
		return
	}
	c.ReplicaID, c.ContainerID, c.MachineID, c.GPU, c.ConfigRevision = replica.ID, replica.ContainerID, replica.MachineID, replica.GPU, replica.Config.AckedRevision
	c.ProviderWorkspaceID, c.ProviderShareMicroUSD = "", 0
	if replica.ProviderWorkspaceID != "" && c.Cost.MicroUSD > 0 {
		c.ProviderWorkspaceID = replica.ProviderWorkspaceID
		c.ProviderShareMicroUSD = int64(float64(c.Cost.MicroUSD) * b.s.config.ProviderRevenueShare)
	}
}

// open journals a charge for work that will finish later (a queued task).
func (b *billing) open(ctx context.Context, c *types.Charge) error {
	_, err := b.s.repo.SaveCharge(ctx, c)
	return err
}

// finish journals a finished charge and applies it. The journal write is the
// commit point: once it succeeds the work is accepted for billing and a
// failure applying counters is retried by flush, never surfaced as a client
// error that would invite a retry of already-billed work. A charge that was
// already final (a duplicate completion) is left exactly as it was.
func (b *billing) finish(ctx context.Context, c *types.Charge) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	written, err := b.s.repo.SaveCharge(ctx, c)
	if err != nil {
		return fmt.Errorf("journal charge: %w", err)
	}
	if !written {
		return nil
	}
	b.s.emit(types.EventEndpointRoute, types.EventEndpointSchema{
		EndpointID: c.AppID, Action: "route." + string(c.Status), WorkspaceID: c.WorkspaceID, Version: c.Version, ReplicaID: c.ReplicaID, ContainerID: c.ContainerID, GPU: c.GPU,
		Revision: c.ConfigRevision, Message: c.Error, Data: map[string]any{"charge": c},
	})
	if err := b.s.repo.RecordRouteSample(ctx, c); err != nil {
		log.Debug().Err(err).Msg("managed endpoints: record route sample")
	}
	if err := b.account(ctx, c); err != nil {
		log.Warn().Err(err).Str("charge_id", c.ID).Msg("managed endpoints: charge journaled; accounting will retry")
	}
	return nil
}

// account applies a settled charge to the caller's spend, the provider's
// earnings and the credit gate, then closes the journal entry. Every step is
// idempotent on the charge id, so a retry after a partial failure charges once.
func (b *billing) account(ctx context.Context, c *types.Charge) error {
	if c.Status == types.ChargeOpen {
		return nil
	}
	if c.Status == types.ChargeSettled {
		if err := b.s.repo.AddUsage(ctx, types.UsageSpend, c.WorkspaceID, c.AppID, c.ID, c.SettledAt, c.Usage()); err != nil {
			return fmt.Errorf("record spend: %w", err)
		}
		if c.Cost.MicroUSD > 0 && b.s.scheduler != nil {
			if gate := b.s.scheduler.CreditGate(); gate != nil {
				gate.Invalidate(ctx, c.WorkspaceID)
			}
		}
		if c.ProviderWorkspaceID != "" {
			earned := types.Usage{Work: c.Work, Cost: types.Cost{MicroUSD: c.ProviderShareMicroUSD}}
			if err := b.s.repo.AddUsage(ctx, types.UsageEarned, c.ProviderWorkspaceID, c.AppID, c.ID, c.SettledAt, earned); err != nil {
				return fmt.Errorf("record provider earnings: %w", err)
			}
		}
	}
	return b.s.repo.CompleteAccounting(ctx, c.ID, chargeTTL)
}

// settleTask finishes an open task charge from the task record: a completed
// task is one flat-priced request, any other terminal state is unbilled.
// Submission, polling, failed attempts and retries never move money.
func (b *billing) settleTask(ctx context.Context, c *types.Charge) (*types.Charge, error) {
	if c.Status != types.ChargeOpen {
		return c, nil
	}
	task, err := b.s.backend.GetTaskWithRelated(ctx, c.ID)
	now := time.Now()
	switch {
	case err != nil:
		return c, err
	case task == nil || task.ExternalId != c.ID:
		if now.Sub(c.AcceptedAt) < taskLostAfter {
			return c, nil
		}
		c.Void("task record not found", now)
	case task.Status == types.TaskStatusComplete:
		if err := c.Settle(types.Work{}, now); err != nil {
			return c, err
		}
		c.StatusCode = 200
	case task.Status.IsCompleted():
		c.Void("task "+string(task.Status), now)
	default:
		return c, b.s.repo.DeferAccounting(ctx, c.ID, now.Add(repository.TaskPollInterval))
	}
	c.DurationMs = now.Sub(c.AcceptedAt).Milliseconds()
	if task != nil && task.ContainerId != "" {
		replica, _ := b.s.repo.GetReplicaByContainer(ctx, task.ContainerId)
		b.attribute(c, replica)
	}
	return c, b.finish(ctx, c)
}

// tokenUsage reads the OpenAI usage object from a response body.
func tokenUsage(body []byte) (w types.Work, ok bool) {
	var env struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			Details          *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Usage == nil {
		return types.Work{}, false
	}
	w = types.Work{PromptTokens: env.Usage.PromptTokens, CompletionTokens: env.Usage.CompletionTokens}
	if env.Usage.Details != nil {
		w.CachedTokens = env.Usage.Details.CachedTokens
	}
	return w, w.Valid()
}

// costUSD renders micro-dollars as the float OpenRouter puts in usage.cost.
func costUSD(microUSD int64) float64 { return float64(microUSD) / 1_000_000 }
