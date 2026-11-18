package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// Nebius's billing Create returns an unfinished operation, not a usable
// policy. These states and the compute validation were recorded against the
// real APIs. Keep billing readiness separate from Compute's visibility so a
// test cannot accidentally grant the VM access before the policy is ready.
type nebiusPolicyService struct {
	mu sync.Mutex

	states             []string
	existing           bool
	policyCreates      int
	policyReads        int
	computeAttempts    int
	purchases          int
	propagationMisses  int
	computeStatus      int
	computeRefusal     any
	policyPrice        string
	events             []string
	requestedPolicies  []string
	policyReadStatuses []int
}

const nebiusPolicyTestID = "pricingpolicy-u00rtol7r7pulazcqik7v"

func (s *nebiusPolicyService) state() string {
	return s.states[min(max(s.policyReads-1, 0), len(s.states)-1)]
}

func nebiusPolicyMissing(id string) map[string]any {
	return map[string]any{
		"code": 3, "message": "spec.spot_pricing_policy.id is invalid",
		"details": []any{map[string]any{
			"@type":   "type.googleapis.com/nebius.common.v1.ServiceError",
			"service": "compute", "code": "BadRequest", "retryType": "NOTHING",
			"badRequest": map[string]any{"violations": []any{map[string]any{
				"field": "spec.spot_pricing_policy.id", "message": fmt.Sprintf("pricing policy not found by id %q", id),
			}}},
		}},
	}
}

func (s *nebiusPolicyService) client(t *testing.T) *Nebius {
	t.Helper()
	if len(s.states) == 0 {
		s.states = []string{"STATE_ACTIVE"}
	}
	if s.policyPrice == "" {
		s.policyPrice = "3.980"
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.Method + " " + r.URL.Path {
		case "GET /iam/v1/projects/project-test":
			writeJSON(w, http.StatusOK, map[string]any{
				"metadata": map[string]any{"parentId": "tenant-test"}, "spec": map[string]any{"region": "eu-north1"},
			})
		case "POST /billing/v1/pricing-policies":
			s.policyCreates++
			s.existing = true
			s.events = append(s.events, "billing:create")
			// The resource ID arrives before status/finishedAt. This is the
			// asynchronous success response that triggered the production bug.
			writeJSON(w, http.StatusOK, map[string]any{"id": "billingoperation-test", "resourceId": nebiusPolicyTestID})
		case "GET /billing/v1/pricing-policies":
			if !s.existing {
				writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
				return
			}
			s.policyReads++
			state := s.state()
			s.events = append(s.events, "billing:"+state)
			if s.policyReads <= len(s.policyReadStatuses) && s.policyReadStatuses[s.policyReads-1] != http.StatusOK {
				writeJSON(w, s.policyReadStatuses[s.policyReads-1], map[string]any{"code": 7, "message": "permission denied"})
				return
			}
			items := []any{}
			if state != "absent" {
				items = append(items, map[string]any{
					"metadata": map[string]any{"id": nebiusPolicyTestID, "parentId": "project-test"},
					"spec": map[string]any{
						"computeInstanceSpec": map[string]any{"v1": map[string]any{"platform": "gpu-h200-sxm"}},
						"pricing":             map[string]any{"maxPriceV1": map[string]any{"maxPrice": s.policyPrice}},
					},
					"status": map[string]any{"state": state, "schedulingState": "SCHEDULING_STATE_ALLOWED"},
				})
			}
			writeJSON(w, http.StatusOK, map[string]any{"items": items})
		case "POST /compute/v1/instances":
			s.computeAttempts++
			s.events = append(s.events, "compute:create")
			var body struct {
				Spec struct {
					Policy *struct {
						ID string `json:"id"`
					} `json:"spotPricingPolicy"`
					FollowsSpotPrice *struct{} `json:"followsSpotPrice"`
				} `json:"spec"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.Spec.Policy == nil || body.Spec.FollowsSpotPrice != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"code": 3, "message": "missing price cap"})
				return
			}
			s.requestedPolicies = append(s.requestedPolicies, body.Spec.Policy.ID)
			if s.computeRefusal != nil {
				writeJSON(w, s.computeStatus, s.computeRefusal)
				return
			}
			if s.state() != "STATE_ACTIVE" || s.computeAttempts <= s.propagationMisses {
				writeJSON(w, http.StatusBadRequest, nebiusPolicyMissing(body.Spec.Policy.ID))
				return
			}
			s.purchases++
			writeJSON(w, http.StatusOK, map[string]any{"id": "computeoperation-test", "resourceId": "computeinstance-test"})
		default:
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 5, "message": "not found"})
		}
	}))
	t.Cleanup(server.Close)
	client := &Nebius{
		api: newAPI(server.URL, withoutCredentials()), owner: testOwner, now: time.Now,
		config: types.NebiusProviderConfig{DiskGB: defaultDiskGB, Projects: []types.NebiusProjectConfig{{ID: "project-test", SubnetID: "subnet-test"}}},
	}
	client.api.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return client
}

func nebiusPolicyBuy(client *Nebius, ctx context.Context) (Instance, error) {
	return client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-policy-test", MaxHourlyCostMicros: 4_000_000})
}

func TestNebiusWaitsForAsyncPricingPolicyAndComputeVisibility(t *testing.T) {
	api := &nebiusPolicyService{states: []string{"absent", "STATE_CREATING", "STATE_UPDATING", "STATE_ACTIVE"}, propagationMisses: 2}
	instance, err := nebiusPolicyBuy(api.client(t), context.Background())
	api.mu.Lock()
	defer api.mu.Unlock()
	require.NoError(t, err)
	require.Equal(t, "computeinstance-test", instance.ID)
	require.Equal(t, StatusPending, instance.Status)
	require.Equal(t, 1, api.policyCreates)
	require.Equal(t, 3, api.computeAttempts)
	require.Equal(t, 1, api.purchases)
	require.Equal(t, []string{nebiusPolicyTestID, nebiusPolicyTestID, nebiusPolicyTestID}, api.requestedPolicies, "every retry retains the same price cap")
	require.Less(t, slices.Index(api.events, "billing:STATE_ACTIVE"), slices.Index(api.events, "compute:create"))
}

func TestNebiusWaitsForAnExistingUnfinishedPricingPolicy(t *testing.T) {
	api := &nebiusPolicyService{existing: true, states: []string{"STATE_CREATING", "STATE_UPDATING", "STATE_ACTIVE"}}
	_, err := nebiusPolicyBuy(api.client(t), context.Background())
	api.mu.Lock()
	defer api.mu.Unlock()
	require.NoError(t, err)
	require.Zero(t, api.policyCreates, "an unfinished matching policy must not create a duplicate")
	require.Equal(t, 1, api.computeAttempts)
	require.Equal(t, 1, api.purchases)
	require.Less(t, slices.Index(api.events, "billing:STATE_ACTIVE"), slices.Index(api.events, "compute:create"))
}

func TestNebiusDoesNotBuyBeforePricingPolicyReadiness(t *testing.T) {
	for _, state := range []string{"absent", "STATE_CREATING", "STATE_UPDATING", "STATE_DELETING", "STATE_UNKNOWN"} {
		t.Run(state, func(t *testing.T) {
			api := &nebiusPolicyService{states: []string{state}}
			_, err := nebiusPolicyBuy(api.client(t), context.Background())
			api.mu.Lock()
			defer api.mu.Unlock()
			require.Error(t, err)
			require.Zero(t, api.computeAttempts)
			require.Zero(t, api.purchases)
			require.LessOrEqual(t, api.policyReads, 6, "readiness polling must be bounded")
		})
	}
}

func TestNebiusDoesNotBuyWhenPolicyChangedOrCannotBeRead(t *testing.T) {
	for name, api := range map[string]*nebiusPolicyService{
		"price changed": {policyPrice: "4.000"},
		"read denied":   {policyReadStatuses: []int{http.StatusForbidden}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := nebiusPolicyBuy(api.client(t), context.Background())
			api.mu.Lock()
			defer api.mu.Unlock()
			require.Error(t, err)
			require.Zero(t, api.computeAttempts)
			require.Zero(t, api.purchases)
		})
	}
}

func TestNebiusPricingPolicyPropagationRetriesAreBoundedAndCancelable(t *testing.T) {
	api := &nebiusPolicyService{existing: true, propagationMisses: 100}
	_, err := nebiusPolicyBuy(api.client(t), context.Background())
	api.mu.Lock()
	require.Error(t, err)
	require.Equal(t, 5, api.computeAttempts)
	require.Zero(t, api.purchases)
	api.mu.Unlock()

	api = &nebiusPolicyService{existing: true, propagationMisses: 100}
	client := api.client(t)
	client.api.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	_, err = nebiusPolicyBuy(client, context.Background())
	api.mu.Lock()
	defer api.mu.Unlock()
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, api.computeAttempts)
}

func TestNebiusDoesNotRetryOtherCreateRefusals(t *testing.T) {
	wrongField := nebiusPolicyMissing(nebiusPolicyTestID)
	wrongField["details"].([]any)[0].(map[string]any)["badRequest"].(map[string]any)["violations"].([]any)[0].(map[string]any)["field"] = "spec.boot_disk"
	multiple := nebiusPolicyMissing(nebiusPolicyTestID)
	badRequest := multiple["details"].([]any)[0].(map[string]any)["badRequest"].(map[string]any)
	badRequest["violations"] = append(badRequest["violations"].([]any), map[string]any{"field": "metadata.name", "message": "invalid name"})
	for name, tc := range map[string]struct {
		status int
		body   any
	}{
		"different policy":  {http.StatusBadRequest, nebiusPolicyMissing("pricingpolicy-other")},
		"different field":   {http.StatusBadRequest, wrongField},
		"other violations":  {http.StatusBadRequest, multiple},
		"unstructured text": {http.StatusBadRequest, map[string]any{"code": 3, "message": "spec.spot_pricing_policy.id is invalid: pricing policy not found by id " + nebiusPolicyTestID}},
		"ambiguous failure": {http.StatusInternalServerError, nebiusPolicyMissing(nebiusPolicyTestID)},
		"permission denied": {http.StatusForbidden, map[string]any{"code": 7, "message": "permission denied"}},
		"quota exceeded":    {http.StatusBadRequest, map[string]any{"code": 8, "message": "quota exceeded", "details": []any{map[string]any{"quotaFailure": map[string]any{}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			api := &nebiusPolicyService{existing: true, computeStatus: tc.status, computeRefusal: tc.body}
			_, err := nebiusPolicyBuy(api.client(t), context.Background())
			api.mu.Lock()
			defer api.mu.Unlock()
			require.Error(t, err)
			require.Equal(t, 1, api.computeAttempts, "only the exact policy-not-found validation may retry")
			require.Zero(t, api.purchases)
			if tc.status == http.StatusInternalServerError {
				require.True(t, Unanswered(err), "ambiguous creates must remain available for reconciliation")
			}
		})
	}
}

func TestNebiusPolicyReadinessRespectsCancellation(t *testing.T) {
	api := &nebiusPolicyService{states: []string{"STATE_CREATING"}}
	client := api.client(t)
	client.api.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	_, err := nebiusPolicyBuy(client, context.Background())
	api.mu.Lock()
	defer api.mu.Unlock()
	require.True(t, errors.Is(err, context.Canceled))
	require.Zero(t, api.computeAttempts)
	require.Equal(t, 1, api.policyReads)
}
