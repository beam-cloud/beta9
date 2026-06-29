package pod

import (
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
	openAIPaths = []string{
		"/v1/chat/completions",
		"/v1/completions",
		"/v1/embeddings",
		"/v1/models",
	}
	llmSessionHeaders   = []string{"X-Beam-LLM-Session", "X-Beam-Session", "X-Session-ID"}
	llmRequestIDHeaders = []string{"X-Request-ID", "X-Request-Id", "X-Amzn-Trace-Id", "Traceparent"}
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
		finalizeLLMRequestInfo(info, string(body), "")
		return info, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		finalizeLLMRequestInfo(info, string(body), "")
		return info, nil
	}

	if model, ok := payload["model"].(string); ok && strings.TrimSpace(model) != "" {
		info.Model = strings.TrimSpace(model)
	}
	info.Stream = boolValue(payload["stream"])
	info.OutputTokens = requestedOutputTokens(payload)
	promptText := promptTextFromOpenAIPayload(payload)

	info.SessionKey = llmSessionKey(req, payload)
	if info.SessionKey != "" {
		info.SessionHash = hashLLMPrefix(info.Model, "session:"+info.SessionKey)
	}
	finalizeLLMRequestInfo(info, promptText, string(body))
	return info, nil
}

func finalizeLLMRequestInfo(info *llmRequestInfo, promptText, fallbackText string) {
	if info == nil {
		return
	}
	info.PromptTokens = estimateTokensFromText(promptText)
	if info.PromptTokens == 0 {
		info.PromptTokens = estimateTokensFromText(fallbackText)
	}
	info.TokenPressure = info.PromptTokens + info.OutputTokens
	if info.TokenPressure <= 0 {
		info.TokenPressure = 1
	}
	setLLMAffinity(info, promptText)
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
	for _, openAIPath := range openAIPaths {
		if path == openAIPath || strings.HasSuffix(path, openAIPath) {
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
	for _, header := range llmSessionHeaders {
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
	for _, header := range llmRequestIDHeaders {
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
	tokens := int64(math.Ceil(float64(len([]rune(text))) / 4.0))
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
	if err := t.updatePressure(1, t.info.TokenPressure); err == nil {
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
	if t == nil {
		return
	}
	_ = t.updatePressure(-1, -t.info.TokenPressure)
}

func (t *llmRequestTracker) updatePressure(activeStreamsDelta, tokenPressureDelta int64) error {
	if t == nil || t.info == nil || t.pb == nil || t.pb.rdb == nil || t.pb.workspace == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	pipe := t.pb.rdb.Pipeline()
	for _, target := range []string{llmPressureTargetTotal, t.containerID} {
		if target == "" {
			continue
		}
		key := Keys.podLLMPressure(t.pb.workspace.Name, t.pb.stubId, target)
		pipe.HIncrBy(ctx, key, "active_streams", activeStreamsDelta)
		pipe.HIncrBy(ctx, key, "token_pressure", tokenPressureDelta)
		pipe.Expire(ctx, key, llmPressureTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
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
	llm := config.EffectiveLLMConfig()
	if llm == nil {
		return ""
	}
	return llm.Engine
}

func llmEnabled(config *types.StubConfigV1) bool {
	if config == nil {
		return false
	}
	return strings.EqualFold(config.EffectiveServingProtocol(), llmServingProtocolOpenAI)
}

func llmConfiguredModel(config *types.StubConfigV1) string {
	llm := config.EffectiveLLMConfig()
	if llm == nil {
		return ""
	}
	if llm.ServedModelName != "" {
		return llm.ServedModelName
	}
	return llm.ModelID
}

func llmContextLength(config *types.StubConfigV1) int64 {
	llm := config.EffectiveLLMConfig()
	if llm == nil || llm.ContextLength <= 0 {
		return llmDefaultContextLen
	}
	return int64(llm.ContextLength)
}

func llmReadinessProbePaths(config *types.StubConfigV1) []string {
	paths := []string{"/v1/models", "/health", "/server_info", "/get_model_info"}
	if llm := config.EffectiveLLMConfig(); llm != nil && strings.TrimSpace(llm.MetricsPath) != "" {
		paths = append(paths, llm.MetricsPath)
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
