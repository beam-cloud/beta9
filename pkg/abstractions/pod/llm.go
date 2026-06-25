package pod

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const (
	llmServingProtocolOpenAI = "openai"
	llmPressureTargetTotal   = "total"

	llmMaxInspectBytes            int64         = 128 * 1024
	llmAffinityHashChars                        = 24
	llmPrefixChars                              = 4096
	llmPrefixBlockChars                         = 512
	llmMaxPrefixBlocks                          = 8
	llmDefaultContextLen                        = 4096
	llmDefaultOutputTokens        int64         = 256
	llmPressureTTL                time.Duration = 30 * time.Second
	llmAffinityTTL                time.Duration = 10 * time.Minute
	llmReadinessTimeout           time.Duration = 5 * time.Second
	llmReadinessBodyLimit         int64         = 4 * 1024
	llmMetricsTimeout             time.Duration = 750 * time.Millisecond
	llmMetricsBodyLimit           int64         = 512 * 1024
	llmMetricsTTL                 time.Duration = 30 * time.Second
	llmMetricsStaleAfter          time.Duration = 5 * time.Second
	llmMetricsRefreshEvery        time.Duration = 2 * time.Second
	llmAffinityImbalanceThreshold               = 2
	llmSpreadScoreMax             int64         = 128
	llmConnectionWeight                         = 1000
	llmActiveStreamWeight                       = 1000
	llmTokenPressureWeight                      = 100
	llmEngineRunningWeight                      = 1200
	llmEngineWaitingWeight                      = 2500
	llmEngineTTFTWeight                         = 2
	llmEngineTPOTWeight                         = 2
	llmEngineCachePressureWeight                = 4
	llmEngineDecodeBonusCap                     = 500
	llmEngineCacheHitBonusCap                   = 250
	llmSessionAffinityBonus                     = 900
	llmExactPrefixBonus                         = 700
	llmPrefixBlockBonus                         = 250
	llmMaxEventErrorChars                       = 512
)

var (
	llmEngineMetricRedisFields = []string{
		"running_requests",
		"waiting_requests",
		"ttft_ms",
		"tpot_ms",
		"decode_tokens_per_second",
		"prompt_tokens_per_second",
		"gpu_cache_usage_milli",
		"prefix_cache_hit_milli",
		"generation_tokens_total",
		"prompt_tokens_total",
		"prefix_cache_hits_total",
		"prefix_cache_queries_total",
		"ttft_sum_seconds",
		"ttft_count",
		"tpot_sum_seconds",
		"tpot_count",
		"updated_at_ms",
	}

	llmMetricRunningRequests = []string{
		"vllm:num_requests_running",
		"vllm_num_requests_running",
	}
	llmMetricWaitingRequests = []string{
		"vllm:num_requests_waiting",
		"vllm_num_requests_waiting",
	}
	llmMetricGPUCacheUsage = []string{
		"vllm:kv_cache_usage_perc",
		"vllm_kv_cache_usage_perc",
		"vllm:gpu_cache_usage_perc",
		"vllm_gpu_cache_usage_perc",
		"vllm:gpu_cache_usage",
		"vllm_gpu_cache_usage",
	}
	llmMetricPrefixCacheHitRate = []string{
		"vllm:gpu_prefix_cache_hit_rate",
		"vllm_gpu_prefix_cache_hit_rate",
		"vllm:prefix_cache_hit_rate",
		"vllm_prefix_cache_hit_rate",
	}
	llmMetricGenerationTokens = []string{
		"vllm:generation_tokens_total",
		"vllm_generation_tokens_total",
	}
	llmMetricPromptTokens = []string{
		"vllm:prompt_tokens_total",
		"vllm_prompt_tokens_total",
		"vllm:prompt_tokens_processed_total",
		"vllm_prompt_tokens_processed_total",
	}
	llmMetricPrefixCacheHits = []string{
		"vllm:prefix_cache_hits_total",
		"vllm_prefix_cache_hits_total",
		"vllm:gpu_prefix_cache_hits_total",
		"vllm_gpu_prefix_cache_hits_total",
	}
	llmMetricPrefixCacheQueries = []string{
		"vllm:prefix_cache_queries_total",
		"vllm_prefix_cache_queries_total",
		"vllm:gpu_prefix_cache_queries_total",
		"vllm_gpu_prefix_cache_queries_total",
	}
	llmMetricTTFTSumSeconds = []string{
		"vllm:time_to_first_token_seconds_sum",
		"vllm_time_to_first_token_seconds_sum",
	}
	llmMetricTTFTCount = []string{
		"vllm:time_to_first_token_seconds_count",
		"vllm_time_to_first_token_seconds_count",
	}
	llmMetricTPOTSumSeconds = []string{
		"vllm:time_per_output_token_seconds_sum",
		"vllm_time_per_output_token_seconds_sum",
	}
	llmMetricTPOTCount = []string{
		"vllm:time_per_output_token_seconds_count",
		"vllm_time_per_output_token_seconds_count",
	}
)

type llmRequestInfo struct {
	Enabled               bool
	Method                string
	Path                  string
	RequestID             string
	Model                 string
	SessionKey            string
	SessionHash           string
	PrefixHash            string
	PrefixBlocks          []string
	AffinityKey           string
	PromptTokens          int64
	OutputTokens          int64
	TokenPressure         int64
	Stream                bool
	RouteReason           string
	RouteScore            int64
	CandidateCount        int
	ReadyContainerCount   int
	PrefixCacheMatches    int
	QueueWait             time.Duration
	TotalPressureAtRoute  llmPressureSnapshot
	TargetPressureAtRoute llmPressureSnapshot
	EngineMetricsAtRoute  llmEngineMetricsSnapshot
}

type llmPressureSnapshot struct {
	ActiveStreams int64
	TokenPressure int64
}

type llmRequestTracker struct {
	pb             *PodProxyBuffer
	info           *llmRequestInfo
	containerID    string
	targetAddress  string
	startedAt      time.Time
	firstResponse  time.Time
	statusCode     int
	backendError   bool
	errorMessage   string
	pressureBooked bool
}

type llmRouteEventPusher interface {
	PushLLMRouteEvent(event types.EventLLMRouteSchema)
}

type llmEngineMetricsSnapshot struct {
	RunningRequests         int64
	WaitingRequests         int64
	TTFTMs                  int64
	TPOTMs                  int64
	DecodeTokensPerSecond   int64
	PromptTokensPerSecond   int64
	GPUCacheUsageMilli      int64
	PrefixCacheHitMilli     int64
	GenerationTokensTotal   float64
	PromptTokensTotal       float64
	PrefixCacheHitsTotal    float64
	PrefixCacheQueriesTotal float64
	TTFTSumSeconds          float64
	TTFTCount               float64
	TPOTSumSeconds          float64
	TPOTCount               float64
	UpdatedAtUnixMs         int64
}

func (pb *PodProxyBuffer) inspectLLMRequest(ctx echo.Context) (*llmRequestInfo, error) {
	if !llmEnabled(pb.stubConfig) {
		return nil, nil
	}

	path := llmRequestPath(ctx)
	if !isOpenAIPath(path) {
		return nil, nil
	}

	req := ctx.Request()
	info := &llmRequestInfo{
		Enabled:      true,
		Method:       req.Method,
		Path:         path,
		RequestID:    llmRequestID(req),
		Model:        llmConfiguredModel(pb.stubConfig),
		OutputTokens: llmDefaultOutputTokens,
		RouteReason:  "least_pressure",
	}

	if req.Body == nil || req.Method == http.MethodGet || req.Method == http.MethodHead {
		info.PromptTokens = 1
		info.TokenPressure = info.PromptTokens + info.OutputTokens
		setLLMAffinity(info, path)
		return info, nil
	}

	body, overflow, err := readAndRestoreRequestBody(req, llmMaxInspectBytes)
	if err != nil {
		return nil, err
	}

	if overflow {
		info.PromptTokens = estimateTokensFromText(string(body))
		info.TokenPressure = info.PromptTokens + info.OutputTokens
		setLLMAffinity(info, string(body))
		return info, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		info.PromptTokens = estimateTokensFromText(string(body))
		info.TokenPressure = info.PromptTokens + info.OutputTokens
		setLLMAffinity(info, string(body))
		return info, nil
	}

	if model, ok := payload["model"].(string); ok && strings.TrimSpace(model) != "" {
		info.Model = strings.TrimSpace(model)
	}
	info.Stream = boolValue(payload["stream"])
	info.OutputTokens = requestedOutputTokens(payload)
	promptText := promptTextFromOpenAIPayload(payload)
	info.PromptTokens = estimateTokensFromText(promptText)
	if info.PromptTokens == 0 {
		info.PromptTokens = estimateTokensFromText(string(body))
	}

	info.SessionKey = llmSessionKey(req, payload)
	if info.SessionKey != "" {
		info.SessionHash = hashLLMPrefix(info.Model, "session:"+info.SessionKey)
	}
	setLLMAffinity(info, promptText)
	info.TokenPressure = info.PromptTokens + info.OutputTokens
	if info.TokenPressure <= 0 {
		info.TokenPressure = 1
	}
	return info, nil
}

func llmEnabled(config *types.StubConfigV1) bool {
	if config == nil {
		return false
	}
	return strings.EqualFold(config.ServingProtocol, llmServingProtocolOpenAI)
}

func llmConfiguredModel(config *types.StubConfigV1) string {
	if config == nil || config.LLM == nil {
		return ""
	}
	if config.LLM.ServedModelName != "" {
		return config.LLM.ServedModelName
	}
	return config.LLM.ModelID
}

func llmContextLength(config *types.StubConfigV1) int64 {
	if config == nil || config.LLM == nil || config.LLM.ContextLength <= 0 {
		return llmDefaultContextLen
	}
	return int64(config.LLM.ContextLength)
}

func llmReadinessProbePaths(config *types.StubConfigV1) []string {
	paths := []string{"/v1/models", "/health", "/server_info", "/get_model_info"}
	if config != nil && config.LLM != nil && strings.TrimSpace(config.LLM.MetricsPath) != "" {
		paths = append(paths, config.LLM.MetricsPath)
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		path = "/" + strings.TrimPrefix(strings.TrimSpace(path), "/")
		if path == "/" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	return out
}

func (pb *PodProxyBuffer) checkContainerReady(address string, timeout time.Duration) bool {
	if llmEnabled(pb.stubConfig) {
		return pb.checkLLMContainerReady(address, timeout)
	}
	return pb.checkContainerAvailableWithTimeout(address, timeout)
}

func (pb *PodProxyBuffer) checkLLMContainerReady(address string, timeout time.Duration) bool {
	if address == "" {
		return false
	}
	if timeout < llmReadinessTimeout {
		timeout = llmReadinessTimeout
	}

	client := &http.Client{
		Transport: pb.backendTransport(address, timeout),
		Timeout:   timeout,
	}
	for _, path := range llmReadinessProbePaths(pb.stubConfig) {
		ctx, cancel := context.WithTimeout(pb.baseContext(), timeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, podBackendURL("http", address, path, ""), nil)
		if err != nil {
			cancel()
			continue
		}
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, llmReadinessBodyLimit))
		_ = resp.Body.Close()
		cancel()

		if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
			return true
		}
	}
	return false
}

func llmRequestPath(ctx echo.Context) string {
	subPath := ctx.Param("subPath")
	if subPath == "" {
		subPath = ctx.Request().URL.Path
	}
	if !strings.HasPrefix(subPath, "/") {
		subPath = "/" + subPath
	}
	if openAIPath, ok := normalizedOpenAIPath(subPath); ok {
		return openAIPath
	}
	return subPath
}

func isOpenAIPath(path string) bool {
	_, ok := normalizedOpenAIPath(path)
	return ok
}

func normalizedOpenAIPath(path string) (string, bool) {
	path = strings.TrimRight(path, "/")
	if path == "" {
		path = "/"
	}
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/models":
		return path, true
	}

	for _, openAIPath := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/models"} {
		if strings.HasSuffix(path, openAIPath) {
			return openAIPath, true
		}
	}
	return path, false
}

func readAndRestoreRequestBody(req *http.Request, maxBytes int64) ([]byte, bool, error) {
	limited := io.LimitReader(req.Body, maxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, false, err
	}

	overflow := int64(len(body)) > maxBytes
	req.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), req.Body))
	if overflow {
		return body[:int(maxBytes)], true, nil
	}
	return body, false, nil
}

func requestedOutputTokens(payload map[string]any) int64 {
	for _, key := range []string{"max_tokens", "max_completion_tokens"} {
		value := int64Value(payload[key])
		if value > 0 {
			return value
		}
	}
	return llmDefaultOutputTokens
}

func promptTextFromOpenAIPayload(payload map[string]any) string {
	var parts []string
	appendText := func(value string) {
		if strings.TrimSpace(value) != "" {
			parts = append(parts, value)
		}
	}

	switch messages := payload["messages"].(type) {
	case []any:
		for _, message := range messages {
			m, ok := message.(map[string]any)
			if !ok {
				continue
			}
			appendText(openAIContentText(m["content"]))
		}
	}

	switch prompt := payload["prompt"].(type) {
	case string:
		appendText(prompt)
	case []any:
		for _, item := range prompt {
			if text, ok := item.(string); ok {
				appendText(text)
			}
		}
	}

	switch input := payload["input"].(type) {
	case string:
		appendText(input)
	case []any:
		for _, item := range input {
			if text, ok := item.(string); ok {
				appendText(text)
			}
		}
	}

	return strings.Join(parts, "\n")
}

func openAIContentText(value any) string {
	switch content := value.(type) {
	case string:
		return content
	case []any:
		var parts []string
		for _, item := range content {
			part, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := part["text"].(string); ok {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func llmSessionKey(req *http.Request, payload map[string]any) string {
	for _, header := range []string{"X-Beam-LLM-Session", "X-Beam-Session", "X-Session-ID"} {
		if value := strings.TrimSpace(req.Header.Get(header)); value != "" {
			return value
		}
	}
	if user, ok := payload["user"].(string); ok {
		return strings.TrimSpace(user)
	}
	return ""
}

func llmRequestID(req *http.Request) string {
	for _, header := range []string{"X-Request-ID", "X-Request-Id", "X-Amzn-Trace-Id", "Traceparent"} {
		if value := strings.TrimSpace(req.Header.Get(header)); value != "" {
			return value
		}
	}
	return ""
}

func estimateTokensFromText(text string) int64 {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	runes := len([]rune(text))
	tokens := int64(math.Ceil(float64(runes) / 4.0))
	if tokens < 1 {
		return 1
	}
	return tokens
}

func setLLMAffinity(info *llmRequestInfo, promptText string) {
	if info == nil {
		return
	}
	info.PrefixHash = hashLLMPrefix(info.Model, promptText)
	info.PrefixBlocks = llmPrefixBlockHashes(info.Model, promptText)
	if info.SessionHash != "" {
		info.AffinityKey = info.SessionHash
		return
	}
	info.AffinityKey = info.PrefixHash
}

func hashLLMPrefix(model, text string) string {
	normalized := truncateLLMText(normalizeLLMText(text), llmPrefixChars)
	sum := sha256.Sum256([]byte(model + "\n" + normalized))
	return hex.EncodeToString(sum[:])[:llmAffinityHashChars]
}

func llmPrefixBlockHashes(model, text string) []string {
	normalized := truncateLLMText(normalizeLLMText(text), llmPrefixChars)
	if normalized == "" {
		return nil
	}

	runes := []rune(normalized)
	hashes := make([]string, 0, min(len(runes)/llmPrefixBlockChars+1, llmMaxPrefixBlocks))
	for end := llmPrefixBlockChars; end < len(runes) && len(hashes) < llmMaxPrefixBlocks; end += llmPrefixBlockChars {
		hashes = append(hashes, hashLLMPrefix(model, "block:"+string(runes[:end])))
	}
	if len(hashes) < llmMaxPrefixBlocks {
		hashes = append(hashes, hashLLMPrefix(model, "block:"+string(runes)))
	}
	return hashes
}

func normalizeLLMText(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

func truncateLLMText(text string, maxChars int) string {
	if maxChars <= 0 || text == "" {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxChars {
		return text
	}
	return string(runes[:maxChars])
}

func boolValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		parsed, _ := strconv.ParseBool(v)
		return parsed
	default:
		return false
	}
}

func int64Value(value any) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case json.Number:
		out, _ := v.Int64()
		return out
	case string:
		out, _ := strconv.ParseInt(v, 10, 64)
		return out
	default:
		return 0
	}
}

func (pb *PodProxyBuffer) llmAdmissionDenied(info *llmRequestInfo) (bool, string) {
	if info == nil || pb.rdb == nil || pb.workspace == nil || pb.stubConfig == nil {
		return false, ""
	}

	snapshot, err := sharedPodLLMPressure(context.Background(), pb.rdb, pb.workspace.Name, pb.stubId)
	if err != nil {
		return false, ""
	}
	info.TotalPressureAtRoute = snapshot

	maxActive := int64(pb.stubConfig.MaxPendingTasks)
	if maxActive <= 0 {
		maxActive = 100
	}
	if snapshot.ActiveStreams >= maxActive {
		return true, "LLM service is at active stream capacity"
	}

	maxTokens := maxActive * llmContextLength(pb.stubConfig)
	if maxTokens <= 0 {
		maxTokens = maxActive * llmDefaultContextLen
	}
	if snapshot.TokenPressure+info.TokenPressure > maxTokens {
		return true, "LLM service is at token capacity"
	}
	return false, ""
}

func (pb *PodProxyBuffer) reserveLLMContainerForPort(port int32, info *llmRequestInfo) (container, bool, bool, bool) {
	containers := pb.availableContainerSnapshot()
	if len(containers) == 0 {
		return container{}, false, false, false
	}

	routeState := pb.llmRoutingState(info)
	hasPort := false
	readyCount := 0
	candidates := make([]llmContainerCandidate, 0, len(containers))
	for _, c := range containers {
		if _, ok := c.addressMap[port]; !ok {
			continue
		}
		hasPort = true
		if _, ready := c.readyAddressMap[port]; !ready {
			continue
		}
		readyCount++
		candidates = append(candidates, pb.scoreLLMContainer(c, routeState, info, c.readyAddressMap[port]))
	}
	if len(candidates) == 0 {
		return container{}, false, true, hasPort
	}

	selected := pb.selectLLMContainerCandidate(candidates, routeState, info)
	if err := pb.incrementContainerConnections(selected.container.id); err != nil {
		return container{}, false, true, hasPort
	}
	if info != nil {
		info.RouteReason = selected.reason
		info.RouteScore = selected.score
		info.CandidateCount = len(candidates)
		info.ReadyContainerCount = readyCount
		info.PrefixCacheMatches = selected.prefixMatches
		info.TargetPressureAtRoute = selected.pressure
		info.EngineMetricsAtRoute = selected.engineMetrics
	}
	return selected.container, true, true, true
}

type llmRoutingState struct {
	exactContainer string
	exactIsSession bool
	prefixMatches  map[string]int
}

type llmContainerCandidate struct {
	container     container
	score         int64
	loadScore     int64
	queueDepth    int64
	reason        string
	pressure      llmPressureSnapshot
	engineMetrics llmEngineMetricsSnapshot
	prefixMatches int
}

func (pb *PodProxyBuffer) scoreLLMContainer(c container, routeState llmRoutingState, info *llmRequestInfo, address string) llmContainerCandidate {
	connections := int64(c.connections)
	if sharedConnections, err := pb.sharedContainerConnectionCount(c.id); err == nil && int64(sharedConnections) > connections {
		connections = int64(sharedConnections)
	}

	pressure, _ := pb.llmPressureSnapshot(c.id)
	engineMetrics, _ := pb.llmEngineMetricsSnapshot(c.id)
	if engineMetrics.Stale(time.Now()) {
		pb.refreshLLMEngineMetricsAsync(c.id, address)
	}
	contextLen := llmContextLength(pb.stubConfig)
	if contextLen <= 0 {
		contextLen = llmDefaultContextLen
	}
	loadScore := connections*llmConnectionWeight +
		pressure.ActiveStreams*llmActiveStreamWeight +
		(pressure.TokenPressure*llmTokenPressureWeight)/contextLen +
		llmEngineMetricsScore(engineMetrics)

	score := loadScore + llmSpreadScore(c.id, info)
	reason := "least_pressure"
	prefixMatches := routeState.prefixMatches[c.id]
	return llmContainerCandidate{
		container:     c,
		score:         score,
		loadScore:     loadScore,
		queueDepth:    llmCandidateQueueDepth(connections, pressure, engineMetrics),
		reason:        reason,
		pressure:      pressure,
		engineMetrics: engineMetrics,
		prefixMatches: prefixMatches,
	}
}

func (pb *PodProxyBuffer) selectLLMContainerCandidate(candidates []llmContainerCandidate, routeState llmRoutingState, info *llmRequestInfo) llmContainerCandidate {
	if len(candidates) == 1 {
		return applyLLMAffinityScore(candidates[0], routeState)
	}

	if llmCandidatesBalanced(candidates) {
		for i := range candidates {
			candidates[i] = applyLLMAffinityScore(candidates[i], routeState)
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].score == candidates[j].score {
				return candidates[i].container.id < candidates[j].container.id
			}
			return candidates[i].score < candidates[j].score
		})
		return candidates[0]
	}

	reason := "power_of_two_load"
	if llmHasAffinitySignal(candidates, routeState) {
		reason = "load_imbalance"
	}
	return pb.selectPowerOfTwoLLMContainer(candidates, info, reason)
}

func applyLLMAffinityScore(candidate llmContainerCandidate, routeState llmRoutingState) llmContainerCandidate {
	if routeState.exactContainer != "" && routeState.exactContainer == candidate.container.id {
		if routeState.exactIsSession {
			candidate.score -= llmSessionAffinityBonus
			candidate.reason = "session_affinity"
		} else {
			candidate.score -= llmExactPrefixBonus
			candidate.reason = "prefix_affinity"
		}
	} else if candidate.prefixMatches > 0 {
		candidate.score -= int64(candidate.prefixMatches) * llmPrefixBlockBonus
		candidate.reason = "prefix_block_affinity"
	}

	return candidate
}

func llmCandidatesBalanced(candidates []llmContainerCandidate) bool {
	if len(candidates) < 2 {
		return true
	}
	minDepth, maxDepth := candidates[0].queueDepth, candidates[0].queueDepth
	for _, candidate := range candidates[1:] {
		if candidate.queueDepth < minDepth {
			minDepth = candidate.queueDepth
		}
		if candidate.queueDepth > maxDepth {
			maxDepth = candidate.queueDepth
		}
	}
	return maxDepth-minDepth <= llmAffinityImbalanceThreshold
}

func llmHasAffinitySignal(candidates []llmContainerCandidate, routeState llmRoutingState) bool {
	if routeState.exactContainer != "" {
		return true
	}
	for _, candidate := range candidates {
		if candidate.prefixMatches > 0 {
			return true
		}
	}
	return false
}

func (pb *PodProxyBuffer) selectPowerOfTwoLLMContainer(candidates []llmContainerCandidate, info *llmRequestInfo, reason string) llmContainerCandidate {
	if len(candidates) == 1 {
		candidates[0].reason = reason
		return candidates[0]
	}

	left, right := pb.llmPowerOfTwoIndices(len(candidates), info)
	selected := candidates[left]
	other := candidates[right]
	if other.score < selected.score || (other.score == selected.score && other.container.id < selected.container.id) {
		selected = other
	}
	selected.reason = reason
	return selected
}

func (pb *PodProxyBuffer) llmPowerOfTwoIndices(count int, info *llmRequestInfo) (int, int) {
	if count <= 1 {
		return 0, 0
	}

	var counter uint64
	if pb != nil {
		counter = pb.llmRouteCounter.Add(1)
	} else {
		counter = uint64(time.Now().UnixNano())
	}

	key := ""
	if info != nil {
		key = strings.Join([]string{info.Model, info.Path, info.AffinityKey, info.RequestID}, "\n")
	}
	sum := sha256.Sum256([]byte(key + "\n" + strconv.FormatUint(counter, 10)))
	left := int(binary.BigEndian.Uint64(sum[:8]) % uint64(count))
	right := int(binary.BigEndian.Uint64(sum[8:16]) % uint64(count-1))
	if right >= left {
		right++
	}
	return left, right
}

func llmCandidateQueueDepth(connections int64, pressure llmPressureSnapshot, engineMetrics llmEngineMetricsSnapshot) int64 {
	return connections + pressure.ActiveStreams + engineMetrics.RunningRequests + engineMetrics.WaitingRequests
}

func llmSpreadScore(containerID string, info *llmRequestInfo) int64 {
	if containerID == "" || info == nil {
		return 0
	}

	key := info.AffinityKey
	if key == "" {
		key = info.PrefixHash
	}
	if key == "" {
		key = info.Model + ":" + info.Path
	}

	hash := sha256.Sum256([]byte(key + "\n" + containerID))
	return int64(binary.BigEndian.Uint16(hash[:2])) % llmSpreadScoreMax
}

func (pb *PodProxyBuffer) preferredLLMContainer(info *llmRequestInfo) string {
	if info == nil || info.AffinityKey == "" || pb.rdb == nil || pb.workspace == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	containerID, err := pb.rdb.Get(ctx, Keys.podLLMAffinity(pb.workspace.Name, pb.stubId, info.AffinityKey)).Result()
	if err != nil {
		return ""
	}
	return containerID
}

func (pb *PodProxyBuffer) llmRoutingState(info *llmRequestInfo) llmRoutingState {
	state := llmRoutingState{}
	if info == nil {
		return state
	}
	state.exactContainer = pb.preferredLLMContainer(info)
	state.exactIsSession = info.SessionHash != "" && info.AffinityKey == info.SessionHash
	state.prefixMatches = pb.llmPrefixContainerMatches(info)
	return state
}

func (pb *PodProxyBuffer) llmPrefixContainerMatches(info *llmRequestInfo) map[string]int {
	matches := map[string]int{}
	if info == nil || len(info.PrefixBlocks) == 0 || pb.rdb == nil || pb.workspace == nil {
		return matches
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := pb.rdb.Pipeline()
	cmds := make([]interface{ Result() (string, error) }, 0, len(info.PrefixBlocks))
	for _, block := range info.PrefixBlocks {
		if block == "" {
			continue
		}
		cmds = append(cmds, pipe.Get(ctx, Keys.podLLMPrefixAffinity(pb.workspace.Name, pb.stubId, block)))
	}
	if len(cmds) == 0 {
		return matches
	}
	_, _ = pipe.Exec(ctx)
	for _, cmd := range cmds {
		containerID, err := cmd.Result()
		if err != nil || containerID == "" {
			continue
		}
		matches[containerID]++
	}
	return matches
}

func (pb *PodProxyBuffer) recordLLMAffinity(info *llmRequestInfo, containerID string) {
	if info == nil || containerID == "" || pb.rdb == nil || pb.workspace == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pipe := pb.rdb.Pipeline()
	if info.AffinityKey != "" {
		pipe.Set(ctx, Keys.podLLMAffinity(pb.workspace.Name, pb.stubId, info.AffinityKey), containerID, llmAffinityTTL)
	}
	for _, block := range info.PrefixBlocks {
		if block == "" {
			continue
		}
		pipe.Set(ctx, Keys.podLLMPrefixAffinity(pb.workspace.Name, pb.stubId, block), containerID, llmAffinityTTL)
	}
	_, _ = pipe.Exec(ctx)
}

func (pb *PodProxyBuffer) llmPressureSnapshot(target string) (llmPressureSnapshot, error) {
	if pb.rdb == nil || pb.workspace == nil || target == "" {
		return llmPressureSnapshot{}, nil
	}
	return readPodLLMPressure(context.Background(), pb.rdb, pb.workspace.Name, pb.stubId, target)
}

func readPodLLMPressure(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId, target string) (llmPressureSnapshot, error) {
	values, err := rdb.HMGet(ctx, Keys.podLLMPressure(workspaceName, stubId, target), "active_streams", "token_pressure").Result()
	if err != nil {
		return llmPressureSnapshot{}, err
	}

	return llmPressureSnapshot{
		ActiveStreams: redisFieldInt64(values[0]),
		TokenPressure: redisFieldInt64(values[1]),
	}, nil
}

func sharedPodLLMPressure(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId string) (llmPressureSnapshot, error) {
	if rdb == nil || workspaceName == "" || stubId == "" {
		return llmPressureSnapshot{}, nil
	}
	return readPodLLMPressure(ctx, rdb, workspaceName, stubId, llmPressureTargetTotal)
}

func llmMetricsPath(config *types.StubConfigV1) string {
	if config != nil && config.LLM != nil && strings.TrimSpace(config.LLM.MetricsPath) != "" {
		return "/" + strings.TrimPrefix(strings.TrimSpace(config.LLM.MetricsPath), "/")
	}
	return "/metrics"
}

func (s llmEngineMetricsSnapshot) Stale(now time.Time) bool {
	if s.UpdatedAtUnixMs <= 0 {
		return true
	}
	return now.Sub(time.UnixMilli(s.UpdatedAtUnixMs)) > llmMetricsStaleAfter
}

func (s llmEngineMetricsSnapshot) HasData() bool {
	return s.RunningRequests != 0 ||
		s.WaitingRequests != 0 ||
		s.TTFTMs != 0 ||
		s.TPOTMs != 0 ||
		s.DecodeTokensPerSecond != 0 ||
		s.PromptTokensPerSecond != 0 ||
		s.GPUCacheUsageMilli != 0 ||
		s.PrefixCacheHitMilli != 0 ||
		s.GenerationTokensTotal != 0 ||
		s.PromptTokensTotal != 0 ||
		s.PrefixCacheHitsTotal != 0 ||
		s.PrefixCacheQueriesTotal != 0 ||
		s.TTFTCount != 0 ||
		s.TPOTCount != 0
}

func (pb *PodProxyBuffer) llmEngineMetricsSnapshot(containerID string) (llmEngineMetricsSnapshot, error) {
	if pb.rdb == nil || pb.workspace == nil || containerID == "" {
		return llmEngineMetricsSnapshot{}, nil
	}
	return readPodLLMEngineMetrics(context.Background(), pb.rdb, pb.workspace.Name, pb.stubId, containerID)
}

func readPodLLMEngineMetrics(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId, containerID string) (llmEngineMetricsSnapshot, error) {
	values, err := rdb.HMGet(ctx, Keys.podLLMEngineMetrics(workspaceName, stubId, containerID), llmEngineMetricRedisFields...).Result()
	if err != nil {
		return llmEngineMetricsSnapshot{}, err
	}

	return llmEngineMetricsSnapshot{
		RunningRequests:         redisFieldInt64(values[0]),
		WaitingRequests:         redisFieldInt64(values[1]),
		TTFTMs:                  redisFieldInt64(values[2]),
		TPOTMs:                  redisFieldInt64(values[3]),
		DecodeTokensPerSecond:   redisFieldInt64(values[4]),
		PromptTokensPerSecond:   redisFieldInt64(values[5]),
		GPUCacheUsageMilli:      redisFieldInt64(values[6]),
		PrefixCacheHitMilli:     redisFieldInt64(values[7]),
		GenerationTokensTotal:   redisFieldFloat64(values[8]),
		PromptTokensTotal:       redisFieldFloat64(values[9]),
		PrefixCacheHitsTotal:    redisFieldFloat64(values[10]),
		PrefixCacheQueriesTotal: redisFieldFloat64(values[11]),
		TTFTSumSeconds:          redisFieldFloat64(values[12]),
		TTFTCount:               redisFieldFloat64(values[13]),
		TPOTSumSeconds:          redisFieldFloat64(values[14]),
		TPOTCount:               redisFieldFloat64(values[15]),
		UpdatedAtUnixMs:         redisFieldInt64(values[16]),
	}, nil
}

func writePodLLMEngineMetrics(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId, containerID string, snapshot llmEngineMetricsSnapshot) error {
	if rdb == nil || workspaceName == "" || stubId == "" || containerID == "" || !snapshot.HasData() {
		return nil
	}
	key := Keys.podLLMEngineMetrics(workspaceName, stubId, containerID)
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"running_requests":           snapshot.RunningRequests,
		"waiting_requests":           snapshot.WaitingRequests,
		"ttft_ms":                    snapshot.TTFTMs,
		"tpot_ms":                    snapshot.TPOTMs,
		"decode_tokens_per_second":   snapshot.DecodeTokensPerSecond,
		"prompt_tokens_per_second":   snapshot.PromptTokensPerSecond,
		"gpu_cache_usage_milli":      snapshot.GPUCacheUsageMilli,
		"prefix_cache_hit_milli":     snapshot.PrefixCacheHitMilli,
		"generation_tokens_total":    strconv.FormatFloat(snapshot.GenerationTokensTotal, 'f', -1, 64),
		"prompt_tokens_total":        strconv.FormatFloat(snapshot.PromptTokensTotal, 'f', -1, 64),
		"prefix_cache_hits_total":    strconv.FormatFloat(snapshot.PrefixCacheHitsTotal, 'f', -1, 64),
		"prefix_cache_queries_total": strconv.FormatFloat(snapshot.PrefixCacheQueriesTotal, 'f', -1, 64),
		"ttft_sum_seconds":           strconv.FormatFloat(snapshot.TTFTSumSeconds, 'f', -1, 64),
		"ttft_count":                 strconv.FormatFloat(snapshot.TTFTCount, 'f', -1, 64),
		"tpot_sum_seconds":           strconv.FormatFloat(snapshot.TPOTSumSeconds, 'f', -1, 64),
		"tpot_count":                 strconv.FormatFloat(snapshot.TPOTCount, 'f', -1, 64),
		"updated_at_ms":              snapshot.UpdatedAtUnixMs,
	})
	pipe.Expire(ctx, key, llmMetricsTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (pb *PodProxyBuffer) refreshLLMEngineMetricsAsync(containerID, address string) {
	if pb == nil || !llmEnabled(pb.stubConfig) || pb.rdb == nil || pb.workspace == nil || containerID == "" || address == "" {
		return
	}
	now := time.Now()
	if next, ok := pb.llmMetricsRefreshAfter.Load(containerID); ok {
		if nextUnix, ok := next.(int64); ok && now.UnixNano() < nextUnix {
			return
		}
	}
	pb.llmMetricsRefreshAfter.Store(containerID, now.Add(llmMetricsRefreshEvery).UnixNano())
	go func() {
		_ = pb.refreshLLMEngineMetrics(containerID, address)
	}()
}

func (pb *PodProxyBuffer) refreshLLMEngineMetrics(containerID, address string) error {
	if pb == nil || !llmEnabled(pb.stubConfig) || pb.rdb == nil || pb.workspace == nil || containerID == "" || address == "" {
		return nil
	}
	previous, _ := pb.llmEngineMetricsSnapshot(containerID)

	ctx, cancel := context.WithTimeout(pb.baseContext(), llmMetricsTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, podBackendURL("http", address, llmMetricsPath(pb.stubConfig), ""), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/plain")

	client := &http.Client{
		Transport: pb.backendTransport(address, llmMetricsTimeout),
		Timeout:   llmMetricsTimeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, llmReadinessBodyLimit))
		return nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, llmMetricsBodyLimit))
	if err != nil {
		return err
	}
	snapshot := llmEngineMetricsFromPrometheus(body, previous, time.Now())
	if !snapshot.HasData() {
		return nil
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return writePodLLMEngineMetrics(ctx, pb.rdb, pb.workspace.Name, pb.stubId, containerID, snapshot)
}

func llmEngineMetricsFromPrometheus(body []byte, previous llmEngineMetricsSnapshot, now time.Time) llmEngineMetricsSnapshot {
	samples := parsePrometheusSamples(body)
	snapshot := llmEngineMetricsSnapshot{
		RunningRequests:         roundMetric(metricValue(samples, llmMetricRunningRequests...)),
		WaitingRequests:         roundMetric(metricValue(samples, llmMetricWaitingRequests...)),
		GPUCacheUsageMilli:      ratioMilli(metricValue(samples, llmMetricGPUCacheUsage...)),
		PrefixCacheHitMilli:     ratioMilli(metricValue(samples, llmMetricPrefixCacheHitRate...)),
		GenerationTokensTotal:   metricValue(samples, llmMetricGenerationTokens...),
		PromptTokensTotal:       metricValue(samples, llmMetricPromptTokens...),
		PrefixCacheHitsTotal:    metricValue(samples, llmMetricPrefixCacheHits...),
		PrefixCacheQueriesTotal: metricValue(samples, llmMetricPrefixCacheQueries...),
		TTFTSumSeconds:          metricValue(samples, llmMetricTTFTSumSeconds...),
		TTFTCount:               metricValue(samples, llmMetricTTFTCount...),
		TPOTSumSeconds:          metricValue(samples, llmMetricTPOTSumSeconds...),
		TPOTCount:               metricValue(samples, llmMetricTPOTCount...),
		UpdatedAtUnixMs:         now.UnixMilli(),
	}

	snapshot.TTFTMs = histogramMeanMillis(snapshot.TTFTSumSeconds, snapshot.TTFTCount, previous.TTFTSumSeconds, previous.TTFTCount)
	snapshot.TPOTMs = histogramMeanMillis(snapshot.TPOTSumSeconds, snapshot.TPOTCount, previous.TPOTSumSeconds, previous.TPOTCount)
	if snapshot.PrefixCacheHitMilli == 0 {
		snapshot.PrefixCacheHitMilli = ratioDeltaMilli(snapshot.PrefixCacheHitsTotal, snapshot.PrefixCacheQueriesTotal, previous.PrefixCacheHitsTotal, previous.PrefixCacheQueriesTotal)
	}
	elapsed := float64(snapshot.UpdatedAtUnixMs-previous.UpdatedAtUnixMs) / float64(time.Second/time.Millisecond)
	if elapsed > 0 {
		if delta := snapshot.GenerationTokensTotal - previous.GenerationTokensTotal; delta > 0 {
			snapshot.DecodeTokensPerSecond = roundMetric(delta / elapsed)
		}
		if delta := snapshot.PromptTokensTotal - previous.PromptTokensTotal; delta > 0 {
			snapshot.PromptTokensPerSecond = roundMetric(delta / elapsed)
		}
	}
	if snapshot.DecodeTokensPerSecond == 0 && snapshot.TPOTMs > 0 {
		snapshot.DecodeTokensPerSecond = 1000 / snapshot.TPOTMs
	}
	return snapshot
}

func parsePrometheusSamples(body []byte) map[string]float64 {
	samples := map[string]float64{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 4096), int(llmMetricsBodyLimit))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		name := fields[0]
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		if name == "" {
			continue
		}
		samples[name] += value
	}
	return samples
}

func metricValue(samples map[string]float64, names ...string) float64 {
	for _, name := range names {
		if value, ok := samples[name]; ok {
			return value
		}
	}
	return 0
}

func histogramMeanMillis(sum, count, previousSum, previousCount float64) int64 {
	deltaCount := count - previousCount
	if deltaCount > 0 {
		return roundMetric(((sum - previousSum) / deltaCount) * 1000)
	}
	if count > 0 {
		return roundMetric((sum / count) * 1000)
	}
	return 0
}

func ratioMilli(value float64) int64 {
	if value <= 0 {
		return 0
	}
	if value <= 1.5 {
		return roundMetric(value * 1000)
	}
	return roundMetric(value * 10)
}

func ratioDeltaMilli(numerator, denominator, previousNumerator, previousDenominator float64) int64 {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	numeratorDelta := numerator - previousNumerator
	denominatorDelta := denominator - previousDenominator
	if denominatorDelta > 0 && numeratorDelta >= 0 {
		return ratioMilli(numeratorDelta / denominatorDelta)
	}
	return ratioMilli(numerator / denominator)
}

func roundMetric(value float64) int64 {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return int64(math.Round(value))
}

func llmEngineMetricsScore(snapshot llmEngineMetricsSnapshot) int64 {
	score := snapshot.RunningRequests*llmEngineRunningWeight +
		snapshot.WaitingRequests*llmEngineWaitingWeight +
		snapshot.TTFTMs*llmEngineTTFTWeight +
		snapshot.TPOTMs*llmEngineTPOTWeight

	if snapshot.GPUCacheUsageMilli > 850 {
		score += (snapshot.GPUCacheUsageMilli - 850) * llmEngineCachePressureWeight
	}
	if snapshot.DecodeTokensPerSecond > 0 {
		score -= minInt64(snapshot.DecodeTokensPerSecond, llmEngineDecodeBonusCap)
	}
	if snapshot.PrefixCacheHitMilli > 0 {
		score -= minInt64(snapshot.PrefixCacheHitMilli/4, llmEngineCacheHitBonusCap)
	}
	return score
}

func redisFieldInt64(value any) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case string:
		out, _ := strconv.ParseInt(v, 10, 64)
		return out
	case []byte:
		out, _ := strconv.ParseInt(string(v), 10, 64)
		return out
	default:
		return 0
	}
}

func redisFieldFloat64(value any) float64 {
	switch v := value.(type) {
	case nil:
		return 0
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case string:
		out, _ := strconv.ParseFloat(v, 64)
		return out
	case []byte:
		out, _ := strconv.ParseFloat(string(v), 64)
		return out
	default:
		return 0
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (pb *PodProxyBuffer) startLLMRequest(info *llmRequestInfo, containerID, targetAddress string) *llmRequestTracker {
	if info == nil || !info.Enabled {
		return nil
	}

	tracker := &llmRequestTracker{
		pb:            pb,
		info:          info,
		containerID:   containerID,
		targetAddress: targetAddress,
		startedAt:     time.Now(),
	}
	pb.recordLLMAffinity(info, containerID)
	tracker.incrementPressure()
	return tracker
}

func (t *llmRequestTracker) incrementPressure() {
	if t == nil || t.pb == nil || t.pb.rdb == nil || t.pb.workspace == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := t.pb.rdb.Pipeline()
	for _, target := range []string{llmPressureTargetTotal, t.containerID} {
		if target == "" {
			continue
		}
		key := Keys.podLLMPressure(t.pb.workspace.Name, t.pb.stubId, target)
		pipe.HIncrBy(ctx, key, "active_streams", 1)
		pipe.HIncrBy(ctx, key, "token_pressure", t.info.TokenPressure)
		pipe.Expire(ctx, key, llmPressureTTL)
	}
	if _, err := pipe.Exec(ctx); err == nil {
		t.pressureBooked = true
	}
}

func (t *llmRequestTracker) markFirstResponse(statusCode int) {
	if t == nil {
		return
	}
	if t.firstResponse.IsZero() {
		t.firstResponse = time.Now()
	}
	t.statusCode = statusCode
}

func (t *llmRequestTracker) markError(message string) {
	if t == nil {
		return
	}
	t.backendError = true
	t.errorMessage = truncateEventError(message)
	if t.statusCode == 0 {
		t.statusCode = http.StatusBadGateway
	}
}

func (t *llmRequestTracker) finish() {
	if t == nil {
		return
	}
	if t.statusCode == 0 {
		t.statusCode = http.StatusOK
	}
	if t.pressureBooked {
		t.decrementPressure()
	}
	duration := time.Since(t.startedAt)
	metrics.RecordPodLLMRequest(metrics.PodLLMRequestSample{
		WorkspaceName:  t.pb.workspaceName(),
		StubID:         t.pb.stubId,
		Model:          t.info.Model,
		Engine:         llmEngine(t.pb.stubConfig),
		RouteReason:    t.info.RouteReason,
		Stream:         t.info.Stream,
		StatusCode:     t.statusCode,
		BackendError:   t.backendError,
		PromptTokens:   t.info.PromptTokens,
		OutputTokens:   t.info.OutputTokens,
		TokenPressure:  t.info.TokenPressure,
		Duration:       duration,
		TimeToFirst:    t.timeToFirst(),
		ContainerIDSet: t.containerID != "",
	})
	if !t.backendError && t.statusCode < http.StatusInternalServerError {
		t.pb.refreshLLMEngineMetricsAsync(t.containerID, t.targetAddress)
	}
	t.pb.pushLLMRouteEvent(t.info, t.containerID, t.statusCode, t.backendError, t.errorMessage, t.startedAt, t.firstResponse, duration)
}

func (t *llmRequestTracker) decrementPressure() {
	if t.pb.rdb == nil || t.pb.workspace == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := t.pb.rdb.Pipeline()
	for _, target := range []string{llmPressureTargetTotal, t.containerID} {
		if target == "" {
			continue
		}
		key := Keys.podLLMPressure(t.pb.workspace.Name, t.pb.stubId, target)
		pipe.HIncrBy(ctx, key, "active_streams", -1)
		pipe.HIncrBy(ctx, key, "token_pressure", -t.info.TokenPressure)
		pipe.Expire(ctx, key, llmPressureTTL)
	}
	_, _ = pipe.Exec(ctx)
}

func (t *llmRequestTracker) timeToFirst() time.Duration {
	if t.firstResponse.IsZero() {
		return 0
	}
	return t.firstResponse.Sub(t.startedAt)
}

func (pb *PodProxyBuffer) pushLLMRouteEvent(info *llmRequestInfo, containerID string, statusCode int, backendError bool, errorMessage string, startedAt time.Time, firstResponse time.Time, duration time.Duration) {
	if pb == nil || pb.eventRepo == nil || info == nil || !info.Enabled {
		return
	}
	timestamp := time.Now().UTC()
	if !startedAt.IsZero() {
		timestamp = startedAt.UTC()
	}
	event := types.EventLLMRouteSchema{
		Timestamp:                timestamp,
		WorkspaceID:              pb.workspaceExternalID(),
		AppID:                    pb.appID,
		StubID:                   pb.stubId,
		StubType:                 pb.stubType,
		ContainerID:              containerID,
		Method:                   info.Method,
		Path:                     info.Path,
		RequestID:                info.RequestID,
		Model:                    info.Model,
		Engine:                   llmEngine(pb.stubConfig),
		RouteReason:              info.RouteReason,
		RouteScore:               info.RouteScore,
		CandidateCount:           info.CandidateCount,
		ReadyContainerCount:      info.ReadyContainerCount,
		SessionHash:              info.SessionHash,
		PrefixHash:               info.PrefixHash,
		PrefixBlockCount:         len(info.PrefixBlocks),
		PrefixCacheMatches:       info.PrefixCacheMatches,
		PromptTokens:             info.PromptTokens,
		OutputTokens:             info.OutputTokens,
		TokenPressure:            info.TokenPressure,
		Stream:                   info.Stream,
		TotalActiveStreams:       info.TotalPressureAtRoute.ActiveStreams,
		TotalTokenPressure:       info.TotalPressureAtRoute.TokenPressure,
		ContainerActiveStreams:   info.TargetPressureAtRoute.ActiveStreams,
		ContainerTokenPressure:   info.TargetPressureAtRoute.TokenPressure,
		EngineRunningRequests:    info.EngineMetricsAtRoute.RunningRequests,
		EngineWaitingRequests:    info.EngineMetricsAtRoute.WaitingRequests,
		EngineTTFTMs:             info.EngineMetricsAtRoute.TTFTMs,
		EngineTPOTMs:             info.EngineMetricsAtRoute.TPOTMs,
		EngineDecodeTokensPerS:   info.EngineMetricsAtRoute.DecodeTokensPerSecond,
		EngineGPUCacheUsageMilli: info.EngineMetricsAtRoute.GPUCacheUsageMilli,
		EnginePrefixCacheMilli:   info.EngineMetricsAtRoute.PrefixCacheHitMilli,
		QueueWaitMs:              durationMillis(info.QueueWait),
		DurationMs:               durationMillis(duration),
		StatusCode:               statusCode,
		BackendError:             backendError,
		ErrorMessage:             truncateEventError(errorMessage),
	}
	if !firstResponse.IsZero() && !startedAt.IsZero() {
		event.FirstResponseMs = durationMillis(firstResponse.Sub(startedAt))
	}

	go pb.eventRepo.PushLLMRouteEvent(event)
}

func (pb *PodProxyBuffer) pushRejectedLLMRouteEvent(info *llmRequestInfo, statusCode int, reason string) {
	if info == nil || !info.Enabled {
		return
	}
	info.RouteReason = "admission_denied"
	pb.pushLLMRouteEvent(info, "", statusCode, false, reason, time.Now(), time.Time{}, 0)
}

func (pb *PodProxyBuffer) recordFailedQueuedLLMRoute(conn *connection, statusCode int, reason string) {
	if conn == nil || conn.llm == nil || !conn.llm.Enabled {
		return
	}
	startedAt := conn.enqueuedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	} else {
		conn.llm.QueueWait = time.Since(startedAt)
	}
	conn.llm.RouteReason = "queue_failed"
	pb.pushLLMRouteEvent(conn.llm, "", statusCode, false, reason, startedAt, time.Time{}, conn.llm.QueueWait)
}

func (pb *PodProxyBuffer) workspaceExternalID() string {
	if pb == nil || pb.workspace == nil {
		return ""
	}
	if pb.workspace.ExternalId != "" {
		return pb.workspace.ExternalId
	}
	return pb.workspace.Name
}

func durationMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}

func truncateEventError(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	runes := []rune(message)
	if len(runes) <= llmMaxEventErrorChars {
		return message
	}
	return string(runes[:llmMaxEventErrorChars])
}

func llmEngine(config *types.StubConfigV1) string {
	if config == nil || config.LLM == nil {
		return ""
	}
	return config.LLM.Engine
}
