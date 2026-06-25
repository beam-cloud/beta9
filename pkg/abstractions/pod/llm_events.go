package pod

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
)

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
