package managedendpoint

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func TestProxyJSONBillsReportedTokensAndPreservesRequestPricing(t *testing.T) {
	tokenPrice := types.Pricing{PromptTokens: "0.000000021", CompletionTokens: "0"}
	for _, tc := range []struct {
		name      string
		kind      types.EndpointKind
		pricing   types.Pricing
		usage     string
		wantWork  types.Work
		wantMicro int64
		wantError bool
		reported  bool
	}{
		{"custom tokens", types.EndpointKindCustom, tokenPrice, `{"input_tokens":1000,"output_tokens":0}`, types.Work{Requests: 1, PromptTokens: 1000}, 21, false, true},
		{"large valid custom count", types.EndpointKindCustom, tokenPrice, `{"input_tokens":9223372036855,"output_tokens":0}`, types.Work{Requests: 1, PromptTokens: 9_223_372_036_855}, 193_690_812_774, false, true},
		{"explicit zero", types.EndpointKindCustom, tokenPrice, `{"input_tokens":0,"output_tokens":0}`, types.Work{Requests: 1}, 0, false, true},
		{"sub micro rounded down", types.EndpointKindCustom, tokenPrice, `{"input_tokens":23,"output_tokens":0}`, types.Work{Requests: 1, PromptTokens: 23}, 0, false, true},
		{"sub micro rounded up", types.EndpointKindCustom, tokenPrice, `{"input_tokens":24,"output_tokens":0}`, types.Work{Requests: 1, PromptTokens: 24}, 1, false, true},
		{"openai", types.EndpointKindLLM, tokenPrice, `{"prompt_tokens":1000,"completion_tokens":2}`, types.Work{Requests: 1, PromptTokens: 1000, CompletionTokens: 2}, 21, false, true},
		{"embedding", types.EndpointKindEmbedding, tokenPrice, `{"prompt_tokens":1000}`, types.Work{Requests: 1, PromptTokens: 1000}, 21, false, true},
		{"request ignores tokens", types.EndpointKindCustom, types.Pricing{Request: "0.002"}, `{"input_tokens":1000,"output_tokens":0}`, types.Work{Requests: 1}, 2000, false, true},
		{"request needs no tokens", types.EndpointKindCustom, types.Pricing{Request: "0.002"}, `null`, types.Work{Requests: 1}, 2000, false, false},
		{"request ignores malformed tokens", types.EndpointKindCustom, types.Pricing{Request: "0.002"}, `{"input_tokens":null,"output_tokens":0}`, types.Work{Requests: 1}, 2000, false, false},
		{"unknown usage", types.EndpointKindCustom, tokenPrice, `{"total_tokens":1000}`, types.Work{}, 0, true, false},
		{"null counter", types.EndpointKindCustom, tokenPrice, `{"input_tokens":null,"output_tokens":0}`, types.Work{}, 0, true, false},
		{"mixed formats", types.EndpointKindCustom, tokenPrice, `{"input_tokens":1000,"output_tokens":0,"prompt_tokens":1000}`, types.Work{}, 0, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServiceForTest(t)
			app := seedApp(t, s, "acme/decision", tc.kind, tc.pricing)
			replica := seedReplica(t, s, app)
			recorder := httptest.NewRecorder()
			ctx := &auth.HttpAuthContext{Context: echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/invoke", nil), recorder), AuthInfo: userInfo}
			now := time.Now()
			rq := &routeRequest{
				ctx: ctx, auth: userInfo, requestID: "req-billing", startedAt: now,
				charge: &types.Charge{ID: "req-billing", WorkspaceID: "user-ws", AppID: app.Spec.ID, Pricing: tc.pricing, AcceptedAt: now},
			}
			body := `{"answers":{"a":{"type":"noul","noul":0.9}},"usage":` + tc.usage + `}`
			response := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}
			require.NoError(t, s.router.proxyJSON(context.Background(), rq, app, replica, response, "application/json"))
			c := charge(t, s, rq.requestID)
			require.Equal(t, tc.wantWork, c.Work)
			require.Equal(t, tc.wantMicro, c.Cost.MicroUSD)
			if tc.wantError {
				require.Equal(t, http.StatusBadGateway, recorder.Code)
				require.Contains(t, recorder.Body.String(), "missing_usage")
				require.Equal(t, types.ChargeVoid, c.Status)
				require.Equal(t, types.Usage{}, spend(t, s, "user-ws"))
			} else {
				require.Equal(t, http.StatusOK, recorder.Code)
				require.Equal(t, types.ChargeSettled, c.Status)
				require.Equal(t, c.Usage(), spend(t, s, "user-ws"))
				var served map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &served))
				require.JSONEq(t, `{"a":{"type":"noul","noul":0.9}}`, string(served["answers"]))
				var originalUsage, servedUsage map[string]json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(tc.usage), &originalUsage))
				require.NoError(t, json.Unmarshal(served["usage"], &servedUsage))
				if tc.reported {
					var cost float64
					require.NoError(t, json.Unmarshal(servedUsage["cost"], &cost))
					require.Equal(t, float64(c.Cost.MicroUSD)/1_000_000, cost)
					delete(servedUsage, "cost")
				}
				require.Equal(t, originalUsage, servedUsage, "preserve the original counter names and values")
			}
			metrics, err := s.repo.GetRouteMetrics(context.Background(), app.Spec.ID, replica.GPU, replica.ID, 0, time.Minute)
			require.NoError(t, err)
			require.EqualValues(t, 1, metrics.Requests)
			require.Equal(t, tc.wantWork.PromptTokens, metrics.PromptTokens)
			require.Equal(t, tc.wantMicro, metrics.CostMicroUSD)
		})
	}
}
