package managedendpoint

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type delayedChargeRepository struct {
	repository.ManagedEndpointRepository
	delay time.Duration
}

func (r *delayedChargeRepository) SaveCharge(ctx context.Context, c *types.Charge) (bool, error) {
	time.Sleep(r.delay)
	return r.ManagedEndpointRepository.SaveCharge(ctx, c)
}

func responseTiming(t *testing.T, header http.Header) map[string]float64 {
	t.Helper()
	values := map[string]float64{}
	for _, field := range header.Values("Server-Timing") {
		for _, metric := range strings.Split(field, ",") {
			name, duration, ok := strings.Cut(strings.TrimSpace(metric), ";dur=")
			require.True(t, ok, "invalid timing metric %q", metric)
			value, err := strconv.ParseFloat(duration, 64)
			require.NoError(t, err)
			require.False(t, math.IsNaN(value) || math.IsInf(value, 0))
			require.GreaterOrEqual(t, value, 0.0)
			require.NotContains(t, values, name)
			values[name] = value
		}
	}
	for _, name := range []string{"beam_prepare", "beam_upstream", "beam_settle", "beam_total"} {
		require.Contains(t, values, name)
	}
	// Three rounded milliseconds may differ from their rounded sum by 0.002.
	require.InDelta(t, values["beam_total"], values["beam_prepare"]+values["beam_upstream"]+values["beam_settle"], 0.003)
	return values
}

const timingRequest = `{"model":"acme/decision","state":"Refund requested","questions":{"decision":{"type":"noul","instructions":"Does the customer request a refund?"}}}`
const timingResponse = `{"model":"acme/decision","answers":{"decision":{"type":"noul","noul":0.8}},"usage":{"input_tokens":100,"output_tokens":0}}`

func TestBufferedResponseTimingSeparatesBodyReadAndSettlement(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       string
		wantStatus int
	}{
		{"success", timingResponse, http.StatusOK},
		{"missing usage", `{"answers":{"decision":{"type":"noul","noul":0.8}}}`, http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServiceForTest(t)
			s.repo = &delayedChargeRepository{ManagedEndpointRepository: s.repo, delay: 20 * time.Millisecond}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				time.Sleep(15 * time.Millisecond)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Server-Timing", "model;dur=7")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				// The upstream metric must include reading the body after headers arrive.
				time.Sleep(15 * time.Millisecond)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			app := seedApp(t, s, "acme/decision", types.EndpointKindDecision, types.Pricing{PromptTokens: "0.000001", CompletionTokens: "0"})
			replica := seedReplica(t, s, app)
			replica.Status, replica.Address = types.ReplicaStatusReady, strings.TrimPrefix(upstream.URL, "http://")
			require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
			rec := call(t, s, userInfo, http.MethodPost, "/v1/systemone", timingRequest)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			timing := responseTiming(t, rec.Result().Header)
			require.Equal(t, 7.0, timing["model"], "upstream metrics remain intact")
			require.GreaterOrEqual(t, timing["beam_upstream"], 29.5)
			require.GreaterOrEqual(t, timing["beam_settle"], 19.5)
			require.Greater(t, timing["beam_total"], timing["beam_upstream"])
			if tc.wantStatus == http.StatusOK {
				require.Equal(t, tc.body, rec.Body.String(), "System One's response stays verbatim")
			}
		})
	}
}

func TestBufferedResponseTimingIncludesEarlierAttemptInPrepare(t *testing.T) {
	s := newServiceForTest(t)
	var attempts atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempt := attempts.Add(1)
		time.Sleep(20 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"retry another replica"}`))
			return
		}
		_, _ = w.Write([]byte(timingResponse))
	}))
	defer upstream.Close()
	app := seedApp(t, s, "acme/decision", types.EndpointKindDecision, types.Pricing{Request: "0.001"})
	replica := seedReplica(t, s, app)
	replica.Status, replica.Address = types.ReplicaStatusReady, strings.TrimPrefix(upstream.URL, "http://")
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	replica.ID = "rep-2"
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	rec := call(t, s, userInfo, http.MethodPost, "/v1/systemone", timingRequest)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.EqualValues(t, 2, attempts.Load())
	timing := responseTiming(t, rec.Result().Header)
	require.GreaterOrEqual(t, timing["beam_prepare"], 19.5, "earlier attempts precede final dispatch")
	require.GreaterOrEqual(t, timing["beam_upstream"], 19.5)
}
