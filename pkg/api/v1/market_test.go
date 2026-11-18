package apiv1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var marketNoon = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

var (
	asTenant = &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkspace}, Workspace: &types.Workspace{Id: 1}}
	asAdmin  = &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeClusterAdmin}}
)

// newMarketGroup is the market of a cluster with two providers, sampled
// once: one sells an A10G on demand in New York, the other on spot in
// Virginia. Tenants pay a tenth more than the providers charge.
func newMarketGroup(t *testing.T, history bool) *MarketGroup {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })

	shadeform := providers.NewFake("shadeform", "test")
	shadeform.SetOffers(providers.Offer{ID: "A10G_1x", Cloud: "lambda", Region: "newyork-usa-1", GPU: "A10G", GPUCount: 1, HourlyCostMicros: 1_000_000})
	aws := providers.NewFake("aws", "test")
	aws.SetOffers(providers.Offer{ID: "us-east-1b/g5.xlarge", InstanceType: "g5.xlarge", Region: "us-east-1", GPU: "A10G", GPUCount: 1, HourlyCostMicros: 380_000, Spot: true})
	registry := providers.RegistryOf(shadeform, aws)

	var log watch.Log
	if history {
		log = watch.NewMemoryLog()
	}
	clock := common.NewFakeClock(marketNoon)
	watch.NewSampler(watch.Options{Providers: registry, Redis: rdb, Log: log, Clock: clock, ID: "gw-1"}).Sample(context.Background(), marketNoon)
	clock.Advance(time.Minute)

	reader := watch.NewReader(watch.ReaderOptions{
		Providers: registry, Redis: rdb, Log: log, Clock: clock,
		Bill: func(micros int64) int64 { return micros * 11 / 10 },
	})
	return NewMarketGroup(echo.New().Group("/api/v1/market"), reader)
}

// askMarket asks the market a question as someone, and returns the status
// of the answer and its body.
func askMarket(t *testing.T, handler echo.HandlerFunc, as *auth.AuthInfo, query string) (int, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/?"+query, nil), recorder)
	err := handler(&auth.HttpAuthContext{Context: ctx, AuthInfo: as})
	if err == nil {
		return recorder.Code, recorder.Body.String()
	}
	var refused *echo.HTTPError
	require.ErrorAs(t, err, &refused)
	body, _ := json.Marshal(refused.Message)
	return refused.Code, string(body)
}

func TestTheMarketTellsATenantWhatItWouldPayAndAnAdminWhoSells(t *testing.T) {
	group := newMarketGroup(t, true)

	status, body := askMarket(t, group.Now, asTenant, "")
	require.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{
		"admin": false,
		"as_of": "2026-11-17T12:00:00Z",
		"interval_seconds": 60,
		"history": true,
		"gpus": [{"gpu":"A10G","counts":[1],"geos":["us-east"],"pricing":["on_demand","spot"]}],
		"rows": [
			{"gpu":"A10G","gpus":1,"geo":"us-east","pricing":"on_demand","price":1100000,"in_stock":true,"stock":null},
			{"gpu":"A10G","gpus":1,"geo":"us-east","pricing":"spot","price":418000,"in_stock":null,"stock":null}
		]
	}`, body)

	status, body = askMarket(t, group.Now, asAdmin, "")
	require.Equal(t, http.StatusOK, status)
	var told struct {
		Admin bool `json:"admin"`
		Rows  []struct {
			Provider string `json:"provider"`
			Cost     int64  `json:"cost"`
			Price    int64  `json:"price"`
		} `json:"rows"`
		Providers []struct {
			Provider string `json:"provider"`
			OK       bool   `json:"ok"`
		} `json:"providers"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &told))
	assert.True(t, told.Admin)
	require.Len(t, told.Rows, 2)
	assert.Equal(t, "shadeform", told.Rows[0].Provider)
	assert.Equal(t, int64(1_000_000), told.Rows[0].Cost)
	assert.Equal(t, int64(1_100_000), told.Rows[0].Price)
	assert.Len(t, told.Providers, 2)
}

func TestTheMarketsHistoryIsAskedForInTheQuery(t *testing.T) {
	group := newMarketGroup(t, true)
	start := strconv.FormatInt(marketNoon.UnixMilli(), 10)

	status, body := askMarket(t, group.Series, asTenant, "gpu=A10G&start=2026-11-17T12:00:00Z")
	require.Equal(t, http.StatusOK, status, body)
	assert.JSONEq(t, `{
		"gpu": "A10G",
		"start": "2026-11-17T12:00:00Z",
		"end": "2026-11-17T12:01:00Z",
		"tier": "1m",
		"step_seconds": 60,
		"cols": ["ts","price","low","high","stock","in_stock","answered"],
		"series": [
			{"key":"spot","pricing":"spot","points":[[`+start+`,418000,418000,418000,null,null,1]]},
			{"key":"on_demand","pricing":"on_demand","points":[[`+start+`,1100000,1100000,1100000,null,1,1]]}
		]
	}`, body)

	keys := func(as *auth.AuthInfo, query string) []string {
		status, body := askMarket(t, group.Series, as, "gpu=A10G&start="+start+"&"+query)
		require.Equal(t, http.StatusOK, status, body)
		var answer struct {
			Series []struct {
				Key string `json:"key"`
			} `json:"series"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &answer))
		keys := []string{}
		for _, series := range answer.Series {
			keys = append(keys, series.Key)
		}
		return keys
	}
	assert.Equal(t, []string{"us-east|spot", "us-east|on_demand"}, keys(asTenant, "group=geo,pricing"))
	assert.Equal(t, []string{"us-east|spot", "us-east|on_demand"}, keys(asTenant, "group=geo&group=pricing"))
	assert.Equal(t, []string{"all"}, keys(asTenant, "group=none"))
	assert.Equal(t, []string{"spot"}, keys(asTenant, "pricing=SPOT"))
	assert.Equal(t, []string{}, keys(asTenant, "gpu_count=2,4"))
	assert.Equal(t, []string{}, keys(asTenant, "geo=eu-north,eu-west"))
	assert.Equal(t, []string{"aws", "shadeform"}, keys(asAdmin, "group=provider&raw=true"))
	assert.Equal(t, []string{"lambda"}, keys(asAdmin, "group=cloud&provider=shadeform"))
}

func TestAQuestionTheMarketCannotAnswerIsRefusedWithWhy(t *testing.T) {
	group := newMarketGroup(t, true)

	for query, want := range map[string]struct {
		as     *auth.AuthInfo
		status int
		why    string
	}{
		"":                              {asTenant, http.StatusBadRequest, "gpu is required"},
		"gpu=A10G&pricing=cheap":        {asTenant, http.StatusBadRequest, `pricing must be on_demand, spot or any, not \"cheap\"`},
		"gpu=A10G&gpu_count=two":        {asTenant, http.StatusBadRequest, `gpu_count must be whole numbers, not \"two\"`},
		"gpu=A10G&start=yesterday":      {asTenant, http.StatusBadRequest, `start must be a time as RFC 3339 or in milliseconds, not \"yesterday\"`},
		"gpu=A10G&raw=maybe":            {asAdmin, http.StatusBadRequest, `raw must be true or false, not \"maybe\"`},
		"gpu=A10G&group=provider":       {asTenant, http.StatusForbidden, "cluster admins only"},
		"gpu=A10G&provider=aws":         {asTenant, http.StatusForbidden, "cluster admins only"},
		"gpu=A10G&raw=true":             {asTenant, http.StatusForbidden, "cluster admins only"},
		"gpu=A10G&region=us-east-1":     {asTenant, http.StatusForbidden, "cluster admins only"},
		"gpu=A10G&group=provider&raw=1": {asAdmin, http.StatusOK, `"provider":"aws"`},
	} {
		status, body := askMarket(t, group.Series, want.as, query)
		assert.Equal(t, want.status, status, query)
		assert.Contains(t, body, want.why, query)
	}
}

func TestWithoutAHistoryTheMarketStillTellsWhatIsForSale(t *testing.T) {
	group := newMarketGroup(t, false)

	status, body := askMarket(t, group.Now, asTenant, "")
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, `"history":false`)
	assert.Contains(t, body, `"price":1100000`)

	status, body = askMarket(t, group.Series, asTenant, "gpu=A10G")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, body, "history is not kept")
}

func TestAClusterThatDoesNotSampleTheMarketSaysSo(t *testing.T) {
	group := NewMarketGroup(echo.New().Group("/api/v1/market"), nil)

	for _, handler := range []echo.HandlerFunc{group.Now, group.Series} {
		status, body := askMarket(t, handler, asTenant, "gpu=A10G")
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, body, "not sampled")
	}
}
