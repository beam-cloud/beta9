package providers

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNebiusPolicyBecomesActiveAfterInitialBackoffSchedule(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new policy"
		if existing {
			name = "existing policy"
		}
		t.Run(name, func(t *testing.T) {
			// Reaching ACTIVE requires a read after the old five waits totaling
			// thirty seconds. A matching existing policy also has its first
			// read in findPolicy before readiness polling starts.
			pendingReads := 6
			if existing {
				pendingReads++
			}
			states := slices.Repeat([]string{"STATE_CREATING"}, pendingReads)
			api := &nebiusPolicyService{existing: existing, states: append(states, "STATE_ACTIVE")}
			client := api.client(t)
			var waits []time.Duration
			client.api.sleep = func(ctx context.Context, d time.Duration) error {
				waits = append(waits, d)
				return ctx.Err()
			}

			instance, err := nebiusPolicyBuy(client, context.Background())
			require.NoError(t, err)
			require.Equal(t, "computeinstance-test", instance.ID)
			require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second}, waits)
			api.mu.Lock()
			defer api.mu.Unlock()
			require.Equal(t, pendingReads+1, api.policyReads)
			if existing {
				require.Zero(t, api.policyCreates, "reuse the exact unfinished policy instead of creating another")
			} else {
				require.Equal(t, 1, api.policyCreates)
			}
			require.Equal(t, 1, api.computeAttempts)
			require.Equal(t, 1, api.purchases)
			require.Equal(t, []string{nebiusPolicyTestID}, api.requestedPolicies)
			require.Less(t, slices.Index(api.events, "billing:STATE_ACTIVE"), slices.Index(api.events, "compute:create"))
		})
	}
}

func TestNebiusPolicyReadinessHasFiniteWaitBudget(t *testing.T) {
	api := &nebiusPolicyService{states: []string{"STATE_CREATING"}}
	client := api.client(t)
	var waited time.Duration
	var waits int
	client.api.sleep = func(ctx context.Context, d time.Duration) error {
		waited += d
		waits++
		require.Positive(t, d)
		require.LessOrEqual(t, d, 15*time.Second)
		if waits > 20 {
			return errors.New("readiness polling exceeded its finite wait budget")
		}
		return ctx.Err()
	}

	_, err := nebiusPolicyBuy(client, context.Background())
	require.ErrorContains(t, err, "is not ready")
	require.Equal(t, 2*time.Minute, waited)
	require.Equal(t, 11, waits)
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Equal(t, 12, api.policyReads)
	require.Equal(t, 1, api.policyCreates)
	require.Zero(t, api.computeAttempts)
	require.Zero(t, api.purchases)
}

func TestNebiusPolicyReadinessHonorsParentDeadline(t *testing.T) {
	api := &nebiusPolicyService{existing: true, states: []string{"STATE_CREATING"}}
	client := api.client(t)
	client.api.sleep = sleepContext
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := client.readyPolicy(ctx, "project-test", nebiusPolicyTestID, "gpu-h200-sxm", 3_980_000)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Zero(t, api.policyCreates)
	require.Zero(t, api.computeAttempts)
}

func TestNebiusPolicyReadinessHonorsParentCancellation(t *testing.T) {
	api := &nebiusPolicyService{states: []string{"STATE_CREATING"}}
	client := api.client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.api.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	_, err := nebiusPolicyBuy(client, ctx)
	require.ErrorIs(t, err, context.Canceled)
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Equal(t, 1, api.policyCreates)
	require.Equal(t, 1, api.policyReads)
	require.Zero(t, api.computeAttempts)
}
