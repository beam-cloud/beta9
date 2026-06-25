package pod

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestInspectLLMRequestParsesOpenAIChatAndRestoresBody(t *testing.T) {
	body := `{"model":"zai-org/GLM-4.5-Air-FP8","user":"session-1","stream":true,"max_tokens":64,"messages":[{"role":"system","content":"You are terse."},{"role":"user","content":"Explain prefix caching for LLM serving."}]}`
	pb := &PodProxyBuffer{
		stubConfig: &types.StubConfigV1{
			ServingProtocol: "openai",
			LLM:             &types.LLMConfig{ModelID: "fallback-model"},
		},
	}
	ctx := newLLMEchoContext(http.MethodPost, "/v1/chat/completions", body)

	info, err := pb.inspectLLMRequest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || !info.Enabled {
		t.Fatal("expected LLM request info")
	}
	if info.Model != "zai-org/GLM-4.5-Air-FP8" {
		t.Fatalf("model = %q", info.Model)
	}
	if !info.Stream {
		t.Fatal("expected stream=true")
	}
	if info.OutputTokens != 64 {
		t.Fatalf("output tokens = %d, want 64", info.OutputTokens)
	}
	if info.SessionKey != "session-1" || info.AffinityKey == "" || info.PrefixHash == "" {
		t.Fatalf("bad affinity fields: %+v", info)
	}
	if info.SessionHash == "" {
		t.Fatal("expected hashed session key")
	}
	if len(info.PrefixBlocks) == 0 {
		t.Fatal("expected prefix block hashes")
	}
	restored, err := io.ReadAll(ctx.Request().Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(restored) != body {
		t.Fatal("request body was not restored after inspection")
	}
}

func TestInspectLLMRequestNormalizesSubdomainRewrittenPodPath(t *testing.T) {
	pb := &PodProxyBuffer{stubConfig: &types.StubConfigV1{ServingProtocol: "openai"}}
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/pod/public/stub-1/8000/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}]}`))
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	info, err := pb.inspectLLMRequest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || !info.Enabled {
		t.Fatal("expected LLM request info")
	}
	if info.Path != "/v1/chat/completions" {
		t.Fatalf("path = %q", info.Path)
	}
}

func TestLLMRequestUsesHeaderSession(t *testing.T) {
	pb := &PodProxyBuffer{stubConfig: &types.StubConfigV1{ServingProtocol: "openai"}}
	ctx := newLLMEchoContext(http.MethodPost, "/v1/completions", `{"prompt":"hello"}`)
	ctx.Request().Header.Set("X-Beam-LLM-Session", "thread-123")

	info, err := pb.inspectLLMRequest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.SessionKey != "thread-123" {
		t.Fatalf("session key = %q", info.SessionKey)
	}
}

func TestLLMPressureLifecycle(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	info := &llmRequestInfo{
		Enabled:       true,
		Model:         "model",
		AffinityKey:   "affinity",
		PromptTokens:  100,
		OutputTokens:  50,
		TokenPressure: 150,
	}

	tracker := pb.startLLMRequest(info, "container-a", "")
	total, err := sharedPodLLMPressure(context.Background(), rdb, "workspace", "stub")
	if err != nil {
		t.Fatal(err)
	}
	if total.ActiveStreams != 1 || total.TokenPressure != 150 {
		t.Fatalf("total pressure = %+v", total)
	}
	container, err := readPodLLMPressure(context.Background(), rdb, "workspace", "stub", "container-a")
	if err != nil {
		t.Fatal(err)
	}
	if container.ActiveStreams != 1 || container.TokenPressure != 150 {
		t.Fatalf("container pressure = %+v", container)
	}

	tracker.finish()
	total, err = sharedPodLLMPressure(context.Background(), rdb, "workspace", "stub")
	if err != nil {
		t.Fatal(err)
	}
	if total.ActiveStreams != 0 || total.TokenPressure != 0 {
		t.Fatalf("total pressure after finish = %+v", total)
	}
}

func TestReserveLLMContainerPrefersAffinity(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			connections:     0,
			addressMap:      map[int32]string{8000: "127.0.0.1:1"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:1"},
		},
		{
			id:              "container-b",
			connections:     0,
			addressMap:      map[int32]string{8000: "127.0.0.1:2"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:2"},
		},
	}
	if err := rdb.Set(context.Background(), Keys.podLLMAffinity("workspace", "stub", "affinity"), "container-b", llmAffinityTTL).Err(); err != nil {
		t.Fatal(err)
	}

	info := &llmRequestInfo{Enabled: true, AffinityKey: "affinity", PrefixHash: "affinity"}
	got, ok, _, _ := pb.reserveLLMContainerForPort(8000, info)
	if !ok {
		t.Fatal("expected reservation")
	}
	if got.id != "container-b" {
		t.Fatalf("container = %q, want container-b", got.id)
	}
	if info.RouteReason != "prefix_affinity" {
		t.Fatalf("route reason = %q", info.RouteReason)
	}
}

func TestReserveLLMContainerSpillsBusyAffinityTarget(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			addressMap:      map[int32]string{8000: "127.0.0.1:1"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:1"},
		},
		{
			id:              "container-b",
			addressMap:      map[int32]string{8000: "127.0.0.1:2"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:2"},
		},
	}
	if err := rdb.Set(context.Background(), Keys.podLLMAffinity("workspace", "stub", "session"), "container-a", llmAffinityTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(context.Background(), Keys.podLLMPressure("workspace", "stub", "container-a"), "active_streams", 1, "token_pressure", 512).Err(); err != nil {
		t.Fatal(err)
	}

	info := &llmRequestInfo{
		Enabled:      true,
		AffinityKey:  "session",
		SessionHash:  "session",
		PrefixHash:   "session",
		PromptTokens: 128,
		OutputTokens: 128,
	}
	got, ok, _, _ := pb.reserveLLMContainerForPort(8000, info)
	if !ok {
		t.Fatal("expected reservation")
	}
	if got.id != "container-b" {
		t.Fatalf("container = %q, want idle container-b", got.id)
	}
	if info.RouteReason != "least_pressure" {
		t.Fatalf("route reason = %q", info.RouteReason)
	}
}

func TestReserveLLMContainerIgnoresAffinityWhenLoadIsImbalanced(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			addressMap:      map[int32]string{8000: "127.0.0.1:1"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:1"},
		},
		{
			id:              "container-b",
			addressMap:      map[int32]string{8000: "127.0.0.1:2"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:2"},
		},
	}
	if err := rdb.Set(context.Background(), Keys.podLLMAffinity("workspace", "stub", "session"), "container-a", llmAffinityTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(context.Background(), Keys.podLLMPressure("workspace", "stub", "container-a"), "active_streams", 10, "token_pressure", 4096).Err(); err != nil {
		t.Fatal(err)
	}

	info := &llmRequestInfo{
		Enabled:      true,
		AffinityKey:  "session",
		SessionHash:  "session",
		PrefixHash:   "session",
		PromptTokens: 128,
		OutputTokens: 128,
	}
	got, ok, _, _ := pb.reserveLLMContainerForPort(8000, info)
	if !ok {
		t.Fatal("expected reservation")
	}
	if got.id != "container-b" {
		t.Fatalf("container = %q, want idle container-b", got.id)
	}
	if info.RouteReason != "load_imbalance" {
		t.Fatalf("route reason = %q", info.RouteReason)
	}
}

func TestReserveLLMContainerUsesPrefixSpreadTieBreak(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			connections:     0,
			addressMap:      map[int32]string{8000: "127.0.0.1:1"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:1"},
		},
		{
			id:              "container-b",
			connections:     0,
			addressMap:      map[int32]string{8000: "127.0.0.1:2"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:2"},
		},
	}

	info := &llmRequestInfo{Enabled: true, Model: "model", Path: "/v1/chat/completions"}
	for i := 0; i < 1000; i++ {
		info.AffinityKey = "prefix-spread-" + strconv.Itoa(i)
		info.PrefixHash = info.AffinityKey
		if llmSpreadScore("container-b", info) < llmSpreadScore("container-a", info) {
			break
		}
	}
	if llmSpreadScore("container-b", info) >= llmSpreadScore("container-a", info) {
		t.Fatal("test setup did not find a prefix that prefers container-b")
	}

	got, ok, _, _ := pb.reserveLLMContainerForPort(8000, info)
	if !ok {
		t.Fatal("expected reservation")
	}
	if got.id != "container-b" {
		t.Fatalf("container = %q, want container-b", got.id)
	}
	if info.RouteReason != "least_pressure" {
		t.Fatalf("route reason = %q", info.RouteReason)
	}
}

func TestReserveLLMContainerUsesPrefixBlockAffinity(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			connections:     0,
			addressMap:      map[int32]string{8000: "127.0.0.1:1"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:1"},
		},
		{
			id:              "container-b",
			connections:     0,
			addressMap:      map[int32]string{8000: "127.0.0.1:2"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:2"},
		},
	}

	sharedPrefix := strings.Repeat("shared system prompt and retrieval context ", 30)
	previous := &llmRequestInfo{Enabled: true, Model: "model", Path: "/v1/chat/completions"}
	setLLMAffinity(previous, sharedPrefix+"first question")
	pb.recordLLMAffinity(previous, "container-b")

	info := &llmRequestInfo{Enabled: true, Model: "model", Path: "/v1/chat/completions"}
	setLLMAffinity(info, sharedPrefix+"second question")
	got, ok, _, _ := pb.reserveLLMContainerForPort(8000, info)
	if !ok {
		t.Fatal("expected reservation")
	}
	if got.id != "container-b" {
		t.Fatalf("container = %q, want container-b", got.id)
	}
	if info.RouteReason != "prefix_block_affinity" {
		t.Fatalf("route reason = %q", info.RouteReason)
	}
	if info.PrefixCacheMatches == 0 {
		t.Fatal("expected prefix block match count")
	}
}

func TestReserveLLMContainerConnectionPressureBeatsSpread(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			connections:     0,
			addressMap:      map[int32]string{8000: "127.0.0.1:1"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:1"},
		},
		{
			id:              "container-b",
			connections:     1,
			addressMap:      map[int32]string{8000: "127.0.0.1:2"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:2"},
		},
	}

	info := &llmRequestInfo{Enabled: true, Model: "model", Path: "/v1/chat/completions"}
	for i := 0; i < 1000; i++ {
		info.AffinityKey = "prefix-pressure-" + strconv.Itoa(i)
		info.PrefixHash = info.AffinityKey
		if llmSpreadScore("container-b", info) < llmSpreadScore("container-a", info) {
			break
		}
	}
	if llmSpreadScore("container-b", info) >= llmSpreadScore("container-a", info) {
		t.Fatal("test setup did not find a prefix that prefers container-b")
	}

	got, ok, _, _ := pb.reserveLLMContainerForPort(8000, info)
	if !ok {
		t.Fatal("expected reservation")
	}
	if got.id != "container-a" {
		t.Fatalf("container = %q, want container-a", got.id)
	}
}

func TestLLMEngineMetricsFromPrometheusUsesVLLMDeltas(t *testing.T) {
	now := time.Unix(100, 0)
	previous := llmEngineMetricsSnapshot{
		GenerationTokensTotal:   100,
		PromptTokensTotal:       50,
		PrefixCacheHitsTotal:    25,
		PrefixCacheQueriesTotal: 50,
		TTFTSumSeconds:          1.0,
		TTFTCount:               4,
		TPOTSumSeconds:          0.4,
		TPOTCount:               10,
		UpdatedAtUnixMs:         now.Add(-5 * time.Second).UnixMilli(),
	}
	body := `
	# HELP vllm:num_requests_running running
	vllm:num_requests_running{model_name="qwen"} 2
	vllm:num_requests_waiting{model_name="qwen"} 1
	vllm:kv_cache_usage_perc{model_name="qwen"} 0.92
	vllm:prefix_cache_hits_total{model_name="qwen"} 85
	vllm:prefix_cache_queries_total{model_name="qwen"} 125
	vllm:generation_tokens_total{model_name="qwen"} 250
	vllm:prompt_tokens_total{model_name="qwen"} 110
	vllm:time_to_first_token_seconds_sum{model_name="qwen"} 1.6
vllm:time_to_first_token_seconds_count{model_name="qwen"} 6
vllm:time_per_output_token_seconds_sum{model_name="qwen"} 0.7
vllm:time_per_output_token_seconds_count{model_name="qwen"} 20
`

	got := llmEngineMetricsFromPrometheus([]byte(body), previous, now)

	if got.RunningRequests != 2 || got.WaitingRequests != 1 {
		t.Fatalf("bad scheduler gauges: %+v", got)
	}
	if got.GPUCacheUsageMilli != 920 || got.PrefixCacheHitMilli != 800 {
		t.Fatalf("bad cache metrics: %+v", got)
	}
	if got.TTFTMs != 300 || got.TPOTMs != 30 {
		t.Fatalf("bad histogram deltas: %+v", got)
	}
	if got.DecodeTokensPerSecond != 30 || got.PromptTokensPerSecond != 12 {
		t.Fatalf("bad token rates: %+v", got)
	}
}

func TestLLMEngineMetricsRoundTripRedis(t *testing.T) {
	rdb := newLLMTestRedis(t)
	want := llmEngineMetricsSnapshot{
		RunningRequests:         2,
		WaitingRequests:         1,
		TTFTMs:                  180,
		TPOTMs:                  25,
		DecodeTokensPerSecond:   220,
		GPUCacheUsageMilli:      700,
		PrefixCacheHitMilli:     850,
		GenerationTokensTotal:   1234,
		PrefixCacheHitsTotal:    700,
		PrefixCacheQueriesTotal: 1000,
		TTFTSumSeconds:          3.5,
		TTFTCount:               10,
		UpdatedAtUnixMs:         time.Now().UnixMilli(),
	}
	if err := writePodLLMEngineMetrics(context.Background(), rdb, "workspace", "stub", "container-a", want); err != nil {
		t.Fatal(err)
	}

	got, err := readPodLLMEngineMetrics(context.Background(), rdb, "workspace", "stub", "container-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.RunningRequests != want.RunningRequests ||
		got.WaitingRequests != want.WaitingRequests ||
		got.TTFTMs != want.TTFTMs ||
		got.DecodeTokensPerSecond != want.DecodeTokensPerSecond ||
		got.GPUCacheUsageMilli != want.GPUCacheUsageMilli ||
		got.GenerationTokensTotal != want.GenerationTokensTotal ||
		got.PrefixCacheHitsTotal != want.PrefixCacheHitsTotal ||
		got.PrefixCacheQueriesTotal != want.PrefixCacheQueriesTotal {
		t.Fatalf("round trip mismatch: got=%+v want=%+v", got, want)
	}
}

func TestReserveLLMContainerUsesEngineMetricsPressure(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			addressMap:      map[int32]string{8000: "127.0.0.1:1"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:1"},
		},
		{
			id:              "container-b",
			addressMap:      map[int32]string{8000: "127.0.0.1:2"},
			readyAddressMap: map[int32]string{8000: "127.0.0.1:2"},
		},
	}
	if err := writePodLLMEngineMetrics(context.Background(), rdb, "workspace", "stub", "container-a", llmEngineMetricsSnapshot{
		WaitingRequests: 3,
		TTFTMs:          700,
		UpdatedAtUnixMs: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := writePodLLMEngineMetrics(context.Background(), rdb, "workspace", "stub", "container-b", llmEngineMetricsSnapshot{
		DecodeTokensPerSecond: 300,
		PrefixCacheHitMilli:   700,
		UpdatedAtUnixMs:       time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}

	got, ok, _, _ := pb.reserveLLMContainerForPort(8000, &llmRequestInfo{Enabled: true, Model: "model", Path: "/v1/chat/completions"})
	if !ok {
		t.Fatal("expected reservation")
	}
	if got.id != "container-b" {
		t.Fatalf("container = %q, want container-b", got.id)
	}
}

func TestLLMRouteEventEmittedOnFinish(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	events := &captureLLMRouteEvents{ch: make(chan types.EventLLMRouteSchema, 1)}
	pb.eventRepo = events
	pb.workspace.ExternalId = "workspace-ext"
	pb.appID = "app-ext"

	info := &llmRequestInfo{
		Enabled:            true,
		Method:             http.MethodPost,
		Path:               "/v1/chat/completions",
		Model:              "model",
		RouteReason:        "prefix_block_affinity",
		PrefixHash:         "prefix",
		PrefixBlocks:       []string{"block-1", "block-2"},
		PrefixCacheMatches: 2,
		PromptTokens:       100,
		OutputTokens:       50,
		TokenPressure:      150,
		Stream:             true,
		CandidateCount:     2,
		EngineMetricsAtRoute: llmEngineMetricsSnapshot{
			RunningRequests:       1,
			WaitingRequests:       2,
			TTFTMs:                180,
			TPOTMs:                25,
			DecodeTokensPerSecond: 240,
			GPUCacheUsageMilli:    830,
			PrefixCacheHitMilli:   700,
		},
	}
	tracker := pb.startLLMRequest(info, "container-a", "")
	tracker.markFirstResponse(http.StatusOK)
	tracker.finish()

	select {
	case event := <-events.ch:
		if event.WorkspaceID != "workspace-ext" || event.AppID != "app-ext" || event.StubID != "stub" {
			t.Fatalf("bad scope in event: %+v", event)
		}
		if event.ContainerID != "container-a" || event.RouteReason != "prefix_block_affinity" {
			t.Fatalf("bad route event: %+v", event)
		}
		if event.PromptTokens != 100 || event.OutputTokens != 50 || event.TokenPressure != 150 {
			t.Fatalf("bad token fields: %+v", event)
		}
		if event.EngineRunningRequests != 1 ||
			event.EngineWaitingRequests != 2 ||
			event.EngineTTFTMs != 180 ||
			event.EngineTPOTMs != 25 ||
			event.EngineDecodeTokensPerS != 240 ||
			event.EngineGPUCacheUsageMilli != 830 ||
			event.EnginePrefixCacheMilli != 700 {
			t.Fatalf("bad engine metric fields: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for LLM route event")
	}
}

func TestForwardRequestPreservesLLMRequestAndResponseBodies(t *testing.T) {
	rdb := newLLMTestRedis(t)
	requestBody := `{"model":"model","stream":true,"messages":[{"role":"system","content":"preserve bytes"},{"role":"user","content":"Return the JSON exactly."}],"max_tokens":8}`
	responseBody := "data: {\"id\":\"chatcmpl-preserve\",\"choices\":[{\"delta\":{\"content\":\"alpha\"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\" beta\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("vllm:num_requests_running 0\n"))
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("backend path = %q, want /v1/chat/completions", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != requestBody {
			t.Fatalf("request body changed:\n got: %q\nwant: %q", string(body), requestBody)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Backend-Trace", "preserved")
		_, _ = w.Write([]byte(responseBody))
	}))
	t.Cleanup(backend.Close)

	pb := newLLMTestProxyBuffer(rdb)
	pb.availableContainers = []container{
		{
			id:              "container-a",
			addressMap:      map[int32]string{8000: strings.TrimPrefix(backend.URL, "http://")},
			readyAddressMap: map[int32]string{8000: strings.TrimPrefix(backend.URL, "http://")},
		},
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("port", "subPath")
	ctx.SetParamValues("8000", "v1/chat/completions")

	if err := pb.ForwardRequest(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Body.String(); got != responseBody {
		t.Fatalf("response body changed:\n got: %q\nwant: %q", got, responseBody)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("content type = %q, want text/event-stream", got)
	}
	if got := rec.Header().Get("X-Backend-Trace"); got != "preserved" {
		t.Fatalf("backend header = %q, want preserved", got)
	}
}

func TestForwardRequestLoadBalancesLLMInferenceAcrossReplicas(t *testing.T) {
	rdb := newLLMTestRedis(t)
	containerHits := map[string]int{}
	var hitLock sync.Mutex
	backendFor := func(containerID string) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/metrics" {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write([]byte(`vllm:num_requests_running 0
vllm:num_requests_waiting 0
vllm:time_to_first_token_seconds_sum 0.2
vllm:time_to_first_token_seconds_count 1
vllm:time_per_output_token_seconds_sum 0.03
vllm:time_per_output_token_seconds_count 1
vllm:generation_tokens_total 10
vllm:gpu_cache_usage_perc 0.5
vllm:gpu_prefix_cache_hit_rate 0.7
`))
				return
			}
			if r.URL.Path != "/v1/chat/completions" {
				t.Fatalf("backend path = %q, want /v1/chat/completions", r.URL.Path)
			}
			hitLock.Lock()
			containerHits[containerID]++
			hitLock.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"chatcmpl-test","choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":12,"completion_tokens":3,"total_tokens":15}}`))
		}))
		t.Cleanup(server.Close)
		return server
	}

	backendA := backendFor("container-a")
	backendB := backendFor("container-b")
	events := &captureLLMRouteEvents{ch: make(chan types.EventLLMRouteSchema, 8)}
	pb := newLLMTestProxyBuffer(rdb)
	pb.workspace.ExternalId = "workspace-ext"
	pb.stubType = types.StubTypePodDeployment
	pb.eventRepo = events
	pb.availableContainers = []container{
		{
			id:              "container-a",
			addressMap:      map[int32]string{8000: strings.TrimPrefix(backendA.URL, "http://")},
			readyAddressMap: map[int32]string{8000: strings.TrimPrefix(backendA.URL, "http://")},
		},
		{
			id:              "container-b",
			addressMap:      map[int32]string{8000: strings.TrimPrefix(backendB.URL, "http://")},
			readyAddressMap: map[int32]string{8000: strings.TrimPrefix(backendB.URL, "http://")},
		},
	}

	seedEngineMetrics := func(lowPressureContainerID, highPressureContainerID string) {
		now := time.Now().UnixMilli()
		if err := writePodLLMEngineMetrics(context.Background(), rdb, "workspace", "stub", lowPressureContainerID, llmEngineMetricsSnapshot{
			TTFTMs:                20,
			TPOTMs:                3,
			DecodeTokensPerSecond: 300,
			PrefixCacheHitMilli:   700,
			UpdatedAtUnixMs:       now,
		}); err != nil {
			t.Fatal(err)
		}
		if err := writePodLLMEngineMetrics(context.Background(), rdb, "workspace", "stub", highPressureContainerID, llmEngineMetricsSnapshot{
			RunningRequests:       3,
			WaitingRequests:       1,
			TTFTMs:                800,
			TPOTMs:                40,
			GPUCacheUsageMilli:    920,
			DecodeTokensPerSecond: 20,
			UpdatedAtUnixMs:       now,
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, tt := range []struct {
		prompt                string
		lowPressureContainer  string
		highPressureContainer string
	}{
		{prompt: "load-balance prompt 1", lowPressureContainer: "container-a", highPressureContainer: "container-b"},
		{prompt: "load-balance prompt 2", lowPressureContainer: "container-b", highPressureContainer: "container-a"},
	} {
		seedEngineMetrics(tt.lowPressureContainer, tt.highPressureContainer)

		e := echo.New()
		body := `{"model":"model","messages":[{"role":"user","content":"` + tt.prompt + `"}],"max_tokens":8}`
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		ctx := e.NewContext(req, rec)
		ctx.SetParamNames("port", "subPath")
		ctx.SetParamValues("8000", "v1/chat/completions")

		if err := pb.ForwardRequest(ctx); err != nil {
			t.Fatal(err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%q", rec.Code, http.StatusOK, rec.Body.String())
		}
	}

	hitLock.Lock()
	gotA, gotB := containerHits["container-a"], containerHits["container-b"]
	hitLock.Unlock()
	if gotA == 0 || gotB == 0 {
		t.Fatalf("expected inference requests to reach both replicas, hits=%v", containerHits)
	}

	seen := map[string]types.EventLLMRouteSchema{}
	for len(seen) < 2 {
		select {
		case event := <-events.ch:
			seen[event.ContainerID] = event
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for LLM route events, seen=%v", seen)
		}
	}
	for containerID, event := range seen {
		if event.WorkspaceID != "workspace-ext" || event.StubID != "stub" || event.StubType != types.StubTypePodDeployment {
			t.Fatalf("bad event scope for %s: %+v", containerID, event)
		}
		if event.StatusCode != http.StatusOK || event.CandidateCount != 2 || event.ReadyContainerCount != 2 {
			t.Fatalf("bad route event for %s: %+v", containerID, event)
		}
		if event.PromptTokens == 0 || event.OutputTokens == 0 || event.TokenPressure == 0 {
			t.Fatalf("missing useful token metrics for %s: %+v", containerID, event)
		}
		if event.EngineTTFTMs == 0 || event.EngineDecodeTokensPerS == 0 {
			t.Fatalf("missing useful engine metrics for %s: %+v", containerID, event)
		}
	}
}

func TestLLMAdmissionDeniesOverActiveStreamLimit(t *testing.T) {
	rdb := newLLMTestRedis(t)
	pb := newLLMTestProxyBuffer(rdb)
	pb.stubConfig.MaxPendingTasks = 1

	key := Keys.podLLMPressure("workspace", "stub", llmPressureTargetTotal)
	if err := rdb.HSet(context.Background(), key, "active_streams", 1, "token_pressure", 1).Err(); err != nil {
		t.Fatal(err)
	}

	denied, reason := pb.llmAdmissionDenied(&llmRequestInfo{TokenPressure: 1})
	if !denied || !strings.Contains(reason, "active stream") {
		t.Fatalf("denied=%v reason=%q", denied, reason)
	}
}

func TestLLMReadyAddressMapRequiresOpenAIHTTPReadiness(t *testing.T) {
	var ready atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	t.Cleanup(backend.Close)

	pb := &PodProxyBuffer{
		ctx:        context.Background(),
		stubConfig: &types.StubConfigV1{ServingProtocol: "openai"},
	}
	address := strings.TrimPrefix(backend.URL, "http://")
	if got := pb.readyAddressMap(map[int32]string{8000: address}); len(got) != 0 {
		t.Fatalf("LLM TCP-ready container was marked ready before OpenAI readiness: %+v", got)
	}

	ready.Store(true)
	if got := pb.readyAddressMap(map[int32]string{8000: address}); got[8000] != address {
		t.Fatalf("LLM ready address = %q, want %q", got[8000], address)
	}
}

func TestLLMReadinessProbePathsDedupesMetricsPath(t *testing.T) {
	paths := llmReadinessProbePaths(&types.StubConfigV1{
		LLM: &types.LLMConfig{MetricsPath: "/v1/models"},
	})
	if len(paths) == 0 || paths[0] != "/v1/models" {
		t.Fatalf("first readiness path = %v", paths)
	}
	count := 0
	for _, path := range paths {
		if path == "/v1/models" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("/v1/models appeared %d times in %v", count, paths)
	}
}

func newLLMEchoContext(method, subPath, body string) echo.Context {
	e := echo.New()
	req := httptest.NewRequest(method, "/"+strings.TrimPrefix(subPath, "/"), strings.NewReader(body))
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("subPath")
	ctx.SetParamValues(strings.TrimPrefix(subPath, "/"))
	return ctx
}

func newLLMTestRedis(t *testing.T) *common.RedisClient {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}
	return rdb
}

func newLLMTestProxyBuffer(rdb *common.RedisClient) *PodProxyBuffer {
	return &PodProxyBuffer{
		ctx:           context.Background(),
		rdb:           rdb,
		workspace:     &types.Workspace{Name: "workspace"},
		stubId:        "stub",
		stubConfig:    &types.StubConfigV1{ServingProtocol: "openai", MaxPendingTasks: 100},
		containerRepo: repository.NewContainerRedisRepositoryForTest(rdb),
		buffer:        abstractions.NewRingBuffer[*connection](1),
	}
}

type captureLLMRouteEvents struct {
	ch chan types.EventLLMRouteSchema
}

func (c *captureLLMRouteEvents) PushLLMRouteEvent(event types.EventLLMRouteSchema) {
	c.ch <- event
}
