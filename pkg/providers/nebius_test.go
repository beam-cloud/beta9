package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"gopkg.in/yaml.v2"
)

// What a machine runs when it starts.

// nebiusUserData is what the last create gave cloud-init.
func nebiusUserData(api *nebiusAPI) string {
	userData, _ := api.lastCreate()["spec"].(map[string]any)["cloudInitUserData"].(string)
	return userData
}

// nebiusStartupCommand reads user data as cloud-init does, and returns the
// one command it runs. Nebius hands the field to cloud-init as a cloud-config
// part, which has to be a YAML mapping. A bare shell script is a scalar, and
// the serial console of a real machine said of one: "Yaml load allows ...
// dict ... but got str instead".
func nebiusStartupCommand(t *testing.T, userData string) []string {
	t.Helper()
	require.True(t, strings.HasPrefix(userData, "#cloud-config\n"))
	var config struct {
		RunCmd [][]string `yaml:"runcmd"`
	}
	require.NoError(t, yaml.UnmarshalStrict([]byte(userData), &config))
	require.Len(t, config.RunCmd, 1, "install once in the final stage, rather than every boot or twice")
	require.Len(t, config.RunCmd[0], 3)
	require.Equal(t, []string{"/bin/bash", "-c"}, config.RunCmd[0][:2])
	return config.RunCmd[0]
}

func TestNebiusStartupIsExecutableCloudConfig(t *testing.T) {
	for name, commands := range map[string]string{
		"installer": "curl -fsSL 'https://gateway.example/install' | sh -s -- --join-token 'synthetic-token'",
		"quoting":   "printf '%s\\n' \"double: 'single' # comment \\ tab\t <>& UTF-8: λ\"\n",
		"heredoc":   "cat <<'CONFIG'\n#cloud-config\nruncmd:\n  - [echo, do-not-run-as-another-command]\nCONFIG\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			api := newNebiusAPI(t)
			_, err := api.client(testOwner).Create(context.Background(), CreateRequest{
				Offer: nebiusH200, Name: "beam-cloud-config-test", StartupScript: commands,
			})
			require.NoError(t, err)
			argv := nebiusStartupCommand(t, nebiusUserData(api))
			require.Equal(t, bootScript(commands), argv[2], "the complete shell program must survive the provider's YAML parsing unchanged")
		})
	}
}

func TestNebiusCloudConfigRunsTheOriginalCommandOnceAndKeepsPipefail(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	output := filepath.Join(t.TempDir(), "result")
	// boot buys a machine that starts with the commands, and runs what
	// cloud-init would on it.
	boot := func(name, commands string) ([]byte, error) {
		_, err := client.Create(context.Background(), CreateRequest{Offer: nebiusH200, Name: name, StartupScript: commands})
		require.NoError(t, err)
		argv := nebiusStartupCommand(t, nebiusUserData(api))
		command := exec.Command(argv[0], argv[1:]...)
		command.Env = append(os.Environ(), "SMOKE_OUTPUT="+output)
		return command.CombinedOutput()
	}
	written := func() string {
		contents, err := os.ReadFile(output)
		require.NoError(t, err)
		return string(contents)
	}
	const wrote = "single ' quote: # \\\n"

	log, err := boot("beam-cloud-config-test", `printf '%s\n' "single ' quote: # \\" >> "$SMOKE_OUTPUT"`+"\n")
	require.NoError(t, err, "%s", log)
	require.Equal(t, wrote, written(), "the one runcmd entry must execute the preserved shell program exactly once")

	log, err = boot("beam-cloud-config-failure-test", "false | true\nprintf unexpected >> \"$SMOKE_OUTPUT\"")
	require.Error(t, err, "%s", log)
	require.Equal(t, wrote, written(), "a failed bootstrap pipeline must stop before later commands")
}

// How far a machine is.

func TestNebiusGetAndListFollowAnAsynchronousFirstStart(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-startup-status-test"})
	require.NoError(t, err)
	for _, step := range []struct {
		state       string
		reconciling bool
		want        Status
	}{
		{nebiusStopped, true, StatusPending},
		{nebiusStarting, true, StatusPending},
		{nebiusRunning, false, StatusActive},
		{nebiusStopped, false, StatusInterrupted},
	} {
		api.mu.Lock()
		status := api.instances[api.find(created.ID)]["status"].(map[string]any)
		status["state"], status["reconciling"] = step.state, step.reconciling
		api.mu.Unlock()
		got, err := client.Get(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, step.want, got.Status, "Get: %s, reconciling=%v", step.state, step.reconciling)
		listed, err := client.List(ctx)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, step.want, listed[0].Status, "List: %s, reconciling=%v", step.state, step.reconciling)
	}
}

func TestNebiusStoppedDuringAsyncCreateIsPending(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       string
		reconciling bool
		want        Status
	}{
		{"preparing the first start", nebiusStopped, true, StatusPending},
		{"completed spot stop", nebiusStopped, false, StatusInterrupted},
		{"preemption stopping", nebiusStopping, true, StatusInterrupted},
		{"running during reconciliation", nebiusRunning, true, StatusActive},
		{"failed during reconciliation", nebiusError, true, StatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{
				"spec":   map[string]any{"preemptible": map[string]any{}},
				"status": map[string]any{"state": test.state, "reconciling": test.reconciling},
			})
			require.NoError(t, err)
			var instance nebiusInstance
			require.NoError(t, json.Unmarshal(body, &instance))
			require.Equal(t, test.want, instance.status())
		})
	}
}

// A pricing policy that is not ready at once.

const (
	nebiusPolicyTestID       = "pricingpolicy-u00rtol7r7pulazcqik7v"
	nebiusPolicyTestInstance = "computeinstance-test"
	nebiusPolicyTestSubnet   = "vpcsubnet-e00policytest"
	// What the price limit of these tests leaves for the one GPU of the VM
	// once its disk is paid for.
	nebiusPolicyTestPerGPU int64 = 3_980_000

	// nebiusPolicyUnlisted has the stand-in leave the policy out of the
	// list, as billing does at first with one it is making.
	nebiusPolicyUnlisted nebiusPolicyState = "absent"
	// nebiusPolicyUnheardOf is a state that Nebius does not write.
	nebiusPolicyUnheardOf nebiusPolicyState = "STATE_UNKNOWN"

	// nebiusComputeAsked is the event of Compute being asked for a VM.
	nebiusComputeAsked = "compute:create"
)

// nebiusH200Platform is the platform of the VM these tests buy, which its
// pricing policy is for.
var nebiusH200Platform, _, _ = strings.Cut(nebiusH200.ID, "/")

// nebiusBillingSaid is the event of billing listing the policy in a state.
func nebiusBillingSaid(state nebiusPolicyState) string { return "billing:" + string(state) }

// What Compute answered a create with while it had not heard of the pricing
// policy that billing had just called active, as the real API wrote it. It
// is spelled out here, and not put together from what the client looks for,
// so that the client is held to what Nebius sent.
const (
	nebiusUnseenMessage   = "spec.spot_pricing_policy.id is invalid"
	nebiusUnseenField     = "spec.spot_pricing_policy.id"
	nebiusUnseenViolation = "pricing policy not found by id %q"
)

// nebiusViolation is one thing Compute finds wrong with a create.
type nebiusViolation struct{ field, message string }

// nebiusUnseen is the violation of naming a pricing policy that Compute has
// not heard of.
func nebiusUnseen(policy string) nebiusViolation {
	return nebiusViolation{nebiusUnseenField, fmt.Sprintf(nebiusUnseenViolation, policy)}
}

// nebiusInvalid is how Compute turns down a create for what is wrong with
// it, before it makes anything.
func nebiusInvalid(violations ...nebiusViolation) *nebiusReply {
	listed := make([]any, len(violations))
	for i, violation := range violations {
		listed[i] = map[string]any{"field": violation.field, "message": violation.message}
	}
	return &nebiusReply{
		status: http.StatusBadRequest, code: int(codes.InvalidArgument), message: nebiusUnseenMessage,
		detail: map[string]any{"code": "BadRequest", "retryType": "NOTHING", "badRequest": map[string]any{"violations": listed}},
	}
}

// nebiusPolicyAPI stands in for Nebius where a pricing policy takes time.
// Billing answers a create of one with an operation that is not over, and
// says how far the policy is each time it is listed. Compute hears of an
// active policy later still. The two are scripted apart, so that no test has
// Compute sell a VM under a policy that billing has not finished. nebiusAPI,
// which the other tests use, has every policy active at once.
type nebiusPolicyAPI struct {
	mu sync.Mutex

	// states are what billing says of the policy each time it is listed,
	// and the last of them from then on. Active when none is given.
	states []nebiusPolicyState
	// existing has the policy made before the test buys anything.
	existing bool
	// perGPU is the limit the listed policy has, when it is not the one
	// that was asked for.
	perGPU int64
	// readStatuses answer the lists of the policy in order, where one is
	// not http.StatusOK.
	readStatuses []int
	// unseen is how many creates Compute turns down for a policy it has
	// not heard of, once billing calls the policy active.
	unseen int
	// refusal, when set, answers every create.
	refusal *nebiusReply

	seen nebiusPolicyCalls
}

// nebiusPolicyCalls is what the client asked of the stand-in.
type nebiusPolicyCalls struct {
	policyCreates, policyReads, computeAttempts, purchases int
	// requestedPolicies are the policies the creates named, in order.
	requestedPolicies []string
	// events are what billing said of the policy and when Compute was asked
	// for a VM, in order.
	events []string
}

// activeBeforeCompute reports whether billing had called the policy active
// by the time Compute was first asked for a VM.
func (c nebiusPolicyCalls) activeBeforeCompute() bool {
	active := slices.Index(c.events, nebiusBillingSaid(nebiusPolicyActive))
	return active >= 0 && active < slices.Index(c.events, nebiusComputeAsked)
}

// calls returns what the client has asked for so far.
func (s *nebiusPolicyAPI) calls() nebiusPolicyCalls {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := s.seen
	calls.requestedPolicies = slices.Clone(calls.requestedPolicies)
	calls.events = slices.Clone(calls.events)
	return calls
}

// state is what billing said of the policy when it was last listed.
func (s *nebiusPolicyAPI) state() nebiusPolicyState {
	return s.states[min(max(s.seen.policyReads-1, 0), len(s.states)-1)]
}

// client starts the stand-in and returns a client of it that does not wait
// between two looks at a policy.
func (s *nebiusPolicyAPI) client(t *testing.T) *Nebius {
	t.Helper()
	if len(s.states) == 0 {
		s.states = []nebiusPolicyState{nebiusPolicyActive}
	}
	if s.perGPU == 0 {
		s.perGPU = nebiusPolicyTestPerGPU
	}
	mux := http.NewServeMux()
	mux.HandleFunc(http.MethodGet+" /iam/v1/projects/"+nebiusNorth, s.project)
	mux.HandleFunc(http.MethodPost+" "+nebiusPoliciesPath, s.makePolicy)
	mux.HandleFunc(http.MethodGet+" "+nebiusPoliciesPath, s.listPolicies)
	mux.HandleFunc(http.MethodPost+" "+nebiusInstancesPath, s.create)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client := &Nebius{
		api: newAPI(server.URL, withoutCredentials()), owner: testOwner, now: time.Now,
		config: types.NebiusProviderConfig{DiskGB: defaultDiskGB, Projects: []types.NebiusProjectConfig{{ID: nebiusNorth, SubnetID: nebiusPolicyTestSubnet}}},
	}
	client.api.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return client
}

func (s *nebiusPolicyAPI) project(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"metadata": map[string]any{"parentId": nebiusTestTenant}, "spec": map[string]any{"region": nebiusRegions[nebiusNorth]},
	})
}

// makePolicy answers as billing does: with the ID of the policy, before
// the operation that makes it has a status or is finished. Taking that
// answer for a policy that was ready is what lost purchases.
func (s *nebiusPolicyAPI) makePolicy(w http.ResponseWriter, _ *http.Request) {
	s.seen.policyCreates++
	s.existing = true
	writeJSON(w, http.StatusOK, map[string]any{"id": "billingoperation-test", "resourceId": nebiusPolicyTestID})
}

func (s *nebiusPolicyAPI) listPolicies(w http.ResponseWriter, _ *http.Request) {
	if !s.existing {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
		return
	}
	s.seen.policyReads++
	state := s.state()
	s.seen.events = append(s.seen.events, nebiusBillingSaid(state))
	if read := s.seen.policyReads; read <= len(s.readStatuses) && s.readStatuses[read-1] != http.StatusOK {
		writeJSON(w, s.readStatuses[read-1], (nebiusReply{code: int(codes.PermissionDenied), message: "permission denied"}).body())
		return
	}
	items := []any{}
	if state != nebiusPolicyUnlisted {
		items = append(items, map[string]any{
			"metadata": map[string]any{"id": nebiusPolicyTestID, "parentId": nebiusNorth},
			"spec": map[string]any{
				"computeInstanceSpec": map[string]any{"v1": map[string]any{"platform": nebiusH200Platform}},
				"pricing":             map[string]any{"maxPriceV1": map[string]any{"maxPrice": fmt.Sprintf("%.3f", float64(s.perGPU)/1e6)}},
			},
			"status": map[string]any{"state": state, "schedulingState": "SCHEDULING_STATE_ALLOWED"},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *nebiusPolicyAPI) create(w http.ResponseWriter, r *http.Request) {
	s.seen.computeAttempts++
	s.seen.events = append(s.seen.events, nebiusComputeAsked)
	var body struct {
		Spec struct {
			Policy *struct {
				ID string `json:"id"`
			} `json:"spotPricingPolicy"`
			FollowsSpotPrice *struct{} `json:"followsSpotPrice"`
		} `json:"spec"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil || body.Spec.Policy == nil || body.Spec.FollowsSpotPrice != nil {
		writeJSON(w, http.StatusBadRequest, (nebiusReply{code: int(codes.InvalidArgument), message: "missing price cap"}).body())
		return
	}
	s.seen.requestedPolicies = append(s.seen.requestedPolicies, body.Spec.Policy.ID)
	refusal := s.refusal
	if refusal == nil && (s.state() != nebiusPolicyActive || s.seen.computeAttempts <= s.unseen) {
		refusal = nebiusInvalid(nebiusUnseen(body.Spec.Policy.ID))
	}
	if refusal != nil {
		writeJSON(w, refusal.status, refusal.body())
		return
	}
	s.seen.purchases++
	writeJSON(w, http.StatusOK, map[string]any{"id": "computeoperation-test", "resourceId": nebiusPolicyTestInstance})
}

// nebiusBuyLimited buys the VM these tests buy, at a price limit that takes
// a pricing policy.
func nebiusBuyLimited(ctx context.Context, client *Nebius) (Instance, error) {
	return client.Create(ctx, CreateRequest{
		Offer: nebiusH200, Name: "beam-h200-policy-test", MaxHourlyCostMicros: nebiusPolicyTestPerGPU + nebiusTestDisk,
	})
}

// nebiusPolicyWindow is every wait of a readiness window in which the policy
// never turns active: the client's waits, for as long as it waits at most.
func nebiusPolicyWindow() []time.Duration {
	var waits []time.Duration
	for left := nebiusPolicyReadyTimeout; left > 0; left -= waits[len(waits)-1] {
		waits = append(waits, min(nebiusPolicyWait(len(waits)), left))
	}
	return waits
}

// The words are Nebius's, as its API writes them. They are spelled out here
// and nowhere else in these tests, so that a state the client reads under
// another name than billing gives it is caught.
func TestNebiusReadsHowFarBillingIsWithAPricingPolicy(t *testing.T) {
	for word, want := range map[string]struct {
		state      nebiusPolicyState
		unfinished bool
	}{
		"STATE_UNSPECIFIED": {nebiusPolicyUnspecified, true},
		"STATE_CREATING":    {nebiusPolicyCreating, true},
		"STATE_UPDATING":    {nebiusPolicyUpdating, true},
		"STATE_ACTIVE":      {nebiusPolicyActive, false},
		"STATE_DELETING":    {nebiusPolicyDeleting, false},
		// Billing lists a policy before it has written how far it is.
		"": {"", true},
		// One that Nebius adds later is not waited for: nothing says it
		// will turn active.
		string(nebiusPolicyUnheardOf): {nebiusPolicyUnheardOf, false},
	} {
		t.Run(word, func(t *testing.T) {
			listed, err := json.Marshal(map[string]any{"status": map[string]string{"state": word}})
			require.NoError(t, err)
			var policy nebiusPolicy
			require.NoError(t, json.Unmarshal(listed, &policy))
			require.Equal(t, want.state, policy.Status.State)
			require.Equal(t, want.unfinished, policy.Status.State.unfinished())
		})
	}
}

func TestNebiusPolicyWaitDoublesFromTheFirstToTheLongest(t *testing.T) {
	require.Equal(t, nebiusPolicyFirstWait, nebiusPolicyWait(0))
	attempt := 1
	for ; nebiusPolicyWait(attempt-1) < nebiusPolicyLongestWait; attempt++ {
		require.Equal(t, min(2*nebiusPolicyWait(attempt-1), nebiusPolicyLongestWait), nebiusPolicyWait(attempt))
	}
	// From there on it is the longest, however often the policy is looked at.
	require.Equal(t, nebiusPolicyLongestWait, nebiusPolicyWait(attempt))
	require.Equal(t, nebiusPolicyLongestWait, nebiusPolicyWait(math.MaxInt))
}

// Compute hears of the policy on the last try the client makes.
func TestNebiusWaitsForAsyncPricingPolicyAndComputeVisibility(t *testing.T) {
	api := &nebiusPolicyAPI{
		states: []nebiusPolicyState{nebiusPolicyUnlisted, nebiusPolicyCreating, nebiusPolicyUpdating, nebiusPolicyActive},
		unseen: nebiusPolicyUnseenRetries,
	}
	instance, err := nebiusBuyLimited(context.Background(), api.client(t))
	require.NoError(t, err)
	require.Equal(t, nebiusPolicyTestInstance, instance.ID)
	require.Equal(t, StatusPending, instance.Status)
	calls := api.calls()
	require.Equal(t, 1, calls.policyCreates)
	require.Equal(t, nebiusPolicyUnseenRetries+1, calls.computeAttempts)
	require.Equal(t, 1, calls.purchases)
	require.Equal(t, slices.Repeat([]string{nebiusPolicyTestID}, nebiusPolicyUnseenRetries+1), calls.requestedPolicies, "every retry retains the same price cap")
	require.True(t, calls.activeBeforeCompute())
}

func TestNebiusWaitsForAnExistingUnfinishedPricingPolicy(t *testing.T) {
	api := &nebiusPolicyAPI{existing: true, states: []nebiusPolicyState{nebiusPolicyCreating, nebiusPolicyUpdating, nebiusPolicyActive}}
	_, err := nebiusBuyLimited(context.Background(), api.client(t))
	require.NoError(t, err)
	calls := api.calls()
	require.Zero(t, calls.policyCreates, "an unfinished matching policy must not create a duplicate")
	require.Equal(t, 1, calls.computeAttempts)
	require.Equal(t, 1, calls.purchases)
	require.True(t, calls.activeBeforeCompute())
}

func TestNebiusDoesNotBuyBeforePricingPolicyReadiness(t *testing.T) {
	for _, state := range []nebiusPolicyState{nebiusPolicyUnlisted, nebiusPolicyCreating, nebiusPolicyUpdating, nebiusPolicyDeleting, nebiusPolicyUnheardOf} {
		t.Run(string(state), func(t *testing.T) {
			api := &nebiusPolicyAPI{states: []nebiusPolicyState{state}}
			_, err := nebiusBuyLimited(context.Background(), api.client(t))
			require.Error(t, err)
			calls := api.calls()
			require.Zero(t, calls.computeAttempts)
			require.Zero(t, calls.purchases)
			require.LessOrEqual(t, calls.policyReads, len(nebiusPolicyWindow())+1, "readiness polling must be bounded")
		})
	}
}

func TestNebiusDoesNotBuyWhenPolicyChangedOrCannotBeRead(t *testing.T) {
	for name, api := range map[string]*nebiusPolicyAPI{
		"price changed": {perGPU: nebiusPolicyTestPerGPU + 20_000},
		"read denied":   {readStatuses: []int{http.StatusForbidden}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := nebiusBuyLimited(context.Background(), api.client(t))
			require.Error(t, err)
			calls := api.calls()
			require.Zero(t, calls.computeAttempts)
			require.Zero(t, calls.purchases)
		})
	}
}

// Compute has still not heard of the policy when the client has made its
// last try.
func TestNebiusPricingPolicyPropagationRetriesAreBoundedAndCancelable(t *testing.T) {
	api := &nebiusPolicyAPI{existing: true, unseen: nebiusPolicyUnseenRetries + 1}
	_, err := nebiusBuyLimited(context.Background(), api.client(t))
	require.Error(t, err)
	calls := api.calls()
	require.Equal(t, nebiusPolicyUnseenRetries+1, calls.computeAttempts)
	require.Zero(t, calls.purchases)

	api = &nebiusPolicyAPI{existing: true, unseen: nebiusPolicyUnseenRetries + 1}
	client := api.client(t)
	client.api.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	_, err = nebiusBuyLimited(context.Background(), client)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, api.calls().computeAttempts)
}

func TestNebiusDoesNotRetryOtherCreateRefusals(t *testing.T) {
	unseen := nebiusUnseen(nebiusPolicyTestID)
	ambiguous := nebiusInvalid(unseen)
	ambiguous.status = http.StatusInternalServerError
	for name, refusal := range map[string]*nebiusReply{
		"different policy": nebiusInvalid(nebiusUnseen("pricingpolicy-other")),
		"different field":  nebiusInvalid(nebiusViolation{"spec.boot_disk", unseen.message}),
		"other violations": nebiusInvalid(unseen, nebiusViolation{"metadata.name", "invalid name"}),
		"unstructured text": {
			status: http.StatusBadRequest, code: int(codes.InvalidArgument),
			message: nebiusUnseenMessage + ": pricing policy not found by id " + nebiusPolicyTestID,
		},
		"ambiguous failure": ambiguous,
		"permission denied": {status: http.StatusForbidden, code: int(codes.PermissionDenied), message: "permission denied"},
		"quota exceeded": {
			status: http.StatusBadRequest, code: int(codes.ResourceExhausted), message: "quota exceeded",
			detail: map[string]any{"quotaFailure": map[string]any{}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			api := &nebiusPolicyAPI{existing: true, refusal: refusal}
			_, err := nebiusBuyLimited(context.Background(), api.client(t))
			require.Error(t, err)
			calls := api.calls()
			require.Equal(t, 1, calls.computeAttempts, "only the exact policy-not-found validation may retry")
			require.Zero(t, calls.purchases)
			if refusal.status == http.StatusInternalServerError {
				require.True(t, Unanswered(err), "ambiguous creates must remain available for reconciliation")
			}
		})
	}
}

// The policy turns active one look after the waits have grown to their
// longest. A client that gave up where they stop growing lost the purchase.
func TestNebiusPolicyBecomesActiveAfterInitialBackoffSchedule(t *testing.T) {
	grown := 0
	for nebiusPolicyWait(grown) < nebiusPolicyLongestWait {
		grown++
	}
	want := make([]time.Duration, grown+2)
	for attempt := range want {
		want[attempt] = nebiusPolicyWait(attempt)
	}
	for name, existing := range map[string]bool{"new policy": false, "existing policy": true} {
		t.Run(name, func(t *testing.T) {
			// A policy that exists is also looked at once where it is
			// found, before it is waited for.
			pendingReads := len(want)
			if existing {
				pendingReads++
			}
			api := &nebiusPolicyAPI{existing: existing, states: append(slices.Repeat([]nebiusPolicyState{nebiusPolicyCreating}, pendingReads), nebiusPolicyActive)}
			client := api.client(t)
			var waits []time.Duration
			client.api.sleep = func(ctx context.Context, d time.Duration) error {
				waits = append(waits, d)
				return ctx.Err()
			}

			instance, err := nebiusBuyLimited(context.Background(), client)
			require.NoError(t, err)
			require.Equal(t, nebiusPolicyTestInstance, instance.ID)
			require.Equal(t, want, waits)
			calls := api.calls()
			require.Equal(t, pendingReads+1, calls.policyReads)
			if existing {
				require.Zero(t, calls.policyCreates, "reuse the exact unfinished policy instead of creating another")
			} else {
				require.Equal(t, 1, calls.policyCreates)
			}
			require.Equal(t, 1, calls.computeAttempts)
			require.Equal(t, 1, calls.purchases)
			require.Equal(t, []string{nebiusPolicyTestID}, calls.requestedPolicies)
			require.True(t, calls.activeBeforeCompute())
		})
	}
}

func TestNebiusPolicyReadinessHasFiniteWaitBudget(t *testing.T) {
	api := &nebiusPolicyAPI{states: []nebiusPolicyState{nebiusPolicyCreating}}
	client := api.client(t)
	var waits []time.Duration
	client.api.sleep = func(ctx context.Context, d time.Duration) error {
		waits = append(waits, d)
		// No more waits than the window has room for at its shortest.
		if len(waits) > int(nebiusPolicyReadyTimeout/nebiusPolicyFirstWait) {
			return fmt.Errorf("readiness polling exceeded its finite wait budget")
		}
		return ctx.Err()
	}

	_, err := nebiusBuyLimited(context.Background(), client)
	require.ErrorContains(t, err, "is not ready")
	require.Equal(t, nebiusPolicyWindow(), waits)
	waited := time.Duration(0)
	for _, wait := range waits {
		require.Positive(t, wait)
		require.LessOrEqual(t, wait, nebiusPolicyLongestWait)
		waited += wait
	}
	require.Equal(t, nebiusPolicyReadyTimeout, waited)
	calls := api.calls()
	require.Equal(t, len(waits)+1, calls.policyReads)
	require.Equal(t, 1, calls.policyCreates)
	require.Zero(t, calls.computeAttempts)
	require.Zero(t, calls.purchases)
}

func TestNebiusPolicyReadinessRespectsCancellation(t *testing.T) {
	api := &nebiusPolicyAPI{states: []nebiusPolicyState{nebiusPolicyCreating}}
	client := api.client(t)
	client.api.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	_, err := nebiusBuyLimited(context.Background(), client)
	require.ErrorIs(t, err, context.Canceled)
	calls := api.calls()
	require.Zero(t, calls.computeAttempts)
	require.Equal(t, 1, calls.policyReads)
}

func TestNebiusPolicyReadinessHonorsParentDeadline(t *testing.T) {
	api := &nebiusPolicyAPI{existing: true, states: []nebiusPolicyState{nebiusPolicyCreating}}
	client := api.client(t)
	client.api.sleep = sleepContext
	// Far sooner than the client's first wait is over.
	ctx, cancel := context.WithTimeout(context.Background(), nebiusPolicyFirstWait/32)
	defer cancel()

	_, err := client.readyPolicy(ctx, nebiusNorth, nebiusPolicyTestID, nebiusH200Platform, nebiusPolicyTestPerGPU)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	calls := api.calls()
	require.Zero(t, calls.policyCreates)
	require.Zero(t, calls.computeAttempts)
}

func TestNebiusPolicyReadinessHonorsParentCancellation(t *testing.T) {
	api := &nebiusPolicyAPI{states: []nebiusPolicyState{nebiusPolicyCreating}}
	client := api.client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.api.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	_, err := nebiusBuyLimited(ctx, client)
	require.ErrorIs(t, err, context.Canceled)
	calls := api.calls()
	require.Equal(t, 1, calls.policyCreates)
	require.Equal(t, 1, calls.policyReads)
	require.Zero(t, calls.computeAttempts)
}
