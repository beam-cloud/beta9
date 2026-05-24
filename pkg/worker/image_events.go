package worker

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/prometheus/procfs"
	"github.com/rs/zerolog/log"
)

const (
	clipReadEventQueueSize       = 65536
	clipReadPIDResolveMaxParents = 64
	clipReadAggregateTopN        = 20
)

type clipReadAggregate struct {
	request     *types.ContainerRequest
	startedAt   time.Time
	lastAt      time.Time
	success     bool
	readCount   int64
	errorCount  int64
	bytesRead   int64
	total       time.Duration
	byAccess    map[string]*clipReadRollup
	byOperation map[string]*clipReadRollup
	bySource    map[string]*clipReadRollup
	byLayer     map[string]*clipReadRollup
	byContent   map[string]*clipReadRollup
	firstError  string
	sampleAttrs map[string]string
}

type clipReadRollup struct {
	Operation        string `json:"operation,omitempty"`
	Path             string `json:"path,omitempty"`
	Source           string `json:"source,omitempty"`
	LayerDigest      string `json:"layer_digest,omitempty"`
	DecompressedHash string `json:"decompressed_hash,omitempty"`
	ContentHash      string `json:"content_hash,omitempty"`
	Count            int64  `json:"count"`
	ErrorCount       int64  `json:"error_count,omitempty"`
	TotalUs          int64  `json:"total_us"`
	MaxUs            int64  `json:"max_us"`
	TotalMs          int64  `json:"total_ms"`
	MaxMs            int64  `json:"max_ms"`
	BytesRead        int64  `json:"bytes_read"`
}

func (c *ImageClient) observeClipRead(event clipCommon.ReadTraceEvent) {
	if c == nil || c.clipReadEvents == nil || !strings.HasPrefix(event.Operation, "clip.") {
		return
	}

	select {
	case c.clipReadEvents <- event:
	default:
		log.Debug().
			Str("operation", event.Operation).
			Uint32("caller_pid", event.CallerPID).
			Dur("duration", event.Duration).
			Msg("dropping clip read event because queue is full")
	}
}

func (c *ImageClient) runClipReadEventReporter() {
	for event := range c.clipReadEvents {
		c.recordClipReadEvent(event)
	}
}

func (c *ImageClient) recordClipReadEvent(event clipCommon.ReadTraceEvent) {
	request, ok := c.resolveClipReadRequest(int(event.CallerPID))
	if !ok || request == nil {
		if event.Duration >= 100*time.Millisecond || !event.Success {
			log.Debug().
				Str("operation", event.Operation).
				Uint32("caller_pid", event.CallerPID).
				Str("path", event.Path).
				Dur("duration", event.Duration).
				Msg("could not resolve clip read event to active container")
		}
		return
	}

	c.clipRuntimeMu.Lock()
	aggregate := c.clipAggregates[request.ContainerId]
	if aggregate == nil {
		aggregate = newClipReadAggregate(request)
		c.clipAggregates[request.ContainerId] = aggregate
	}
	aggregate.add(event)
	c.clipRuntimeMu.Unlock()
}

func newClipReadAggregate(request *types.ContainerRequest) *clipReadAggregate {
	return &clipReadAggregate{
		request:     request,
		success:     true,
		byAccess:    map[string]*clipReadRollup{},
		byOperation: map[string]*clipReadRollup{},
		bySource:    map[string]*clipReadRollup{},
		byLayer:     map[string]*clipReadRollup{},
		byContent:   map[string]*clipReadRollup{},
		sampleAttrs: map[string]string{},
	}
}

func (a *clipReadAggregate) add(event clipCommon.ReadTraceEvent) {
	if event.StartedAt.IsZero() {
		event.StartedAt = time.Now().Add(-event.Duration)
	}
	endedAt := event.StartedAt.Add(event.Duration)
	if !event.Success {
		a.success = false
		a.errorCount++
		if a.firstError == "" {
			a.firstError = event.Error
		}
	}

	for key, value := range event.Attrs {
		if key == "" || value == "" {
			continue
		}
		switch key {
		case "cached_locally", "content_cache_available", "storage_mode":
			a.sampleAttrs[key] = value
		}
	}

	contentHash := event.Attrs["content_hash"]
	a.addRollup(a.byOperation, event.Operation, event)

	if isCanonicalClipRead(event.Operation) {
		if a.startedAt.IsZero() || event.StartedAt.Before(a.startedAt) {
			a.startedAt = event.StartedAt
		}
		if endedAt.After(a.lastAt) {
			a.lastAt = endedAt
		}
		a.readCount++
		a.bytesRead += event.BytesRead
		a.total += event.Duration
		a.addRollup(a.byAccess, clipReadRollupKey(event.Operation, event.Path, event.Source, event.LayerDigest, event.DecompressedHash, contentHash), event)
		a.addRollup(a.bySource, event.Source, event)
		if event.LayerDigest != "" {
			a.addRollup(a.byLayer, event.LayerDigest, event)
		}
		if contentID := firstNonEmptyImageValue(event.DecompressedHash, contentHash); contentID != "" {
			a.addRollup(a.byContent, contentID, event)
		}
	}
}

func (a *clipReadAggregate) addRollup(target map[string]*clipReadRollup, key string, event clipCommon.ReadTraceEvent) {
	if key == "" {
		key = "unknown"
	}
	rollup := target[key]
	if rollup == nil {
		rollup = &clipReadRollup{
			Operation:        event.Operation,
			Path:             event.Path,
			Source:           event.Source,
			LayerDigest:      event.LayerDigest,
			DecompressedHash: event.DecompressedHash,
			ContentHash:      event.Attrs["content_hash"],
		}
		target[key] = rollup
	}

	durationUs := event.Duration.Microseconds()
	rollup.Count++
	rollup.TotalUs += durationUs
	rollup.TotalMs = durationUsToMilliseconds(rollup.TotalUs)
	if durationUs > rollup.MaxUs {
		rollup.MaxUs = durationUs
		rollup.MaxMs = durationUsToMilliseconds(durationUs)
	}
	rollup.BytesRead += event.BytesRead
	if !event.Success {
		rollup.ErrorCount++
	}
}

func (c *ImageClient) pushClipReadAggregate(aggregate *clipReadAggregate, flushReason string) {
	if c == nil || c.eventRepo == nil || aggregate == nil || aggregate.request == nil || aggregate.readCount == 0 {
		return
	}

	request := aggregate.request
	wallDuration := aggregate.lastAt.Sub(aggregate.startedAt)
	if wallDuration < 0 {
		wallDuration = 0
	}
	phaseEnd := aggregate.startedAt.Add(aggregate.total)

	attrs := map[string]string{
		"aggregate":           "true",
		"bytes_read":          strconv.FormatInt(aggregate.bytesRead, 10),
		"duration_ns":         strconv.FormatInt(aggregate.total.Nanoseconds(), 10),
		"duration_us":         strconv.FormatInt(aggregate.total.Microseconds(), 10),
		"error_count":         strconv.FormatInt(aggregate.errorCount, 10),
		"first_access_at":     aggregate.startedAt.UTC().Format(time.RFC3339Nano),
		"flush_reason":        flushReason,
		"image_id":            request.ImageId,
		"last_access_at":      aggregate.lastAt.UTC().Format(time.RFC3339Nano),
		"read_count":          strconv.FormatInt(aggregate.readCount, 10),
		"top_content_json":    clipReadRollupsJSON(aggregate.byContent, clipReadAggregateTopN),
		"top_layers_json":     clipReadRollupsJSON(aggregate.byLayer, clipReadAggregateTopN),
		"top_operations_json": clipReadRollupsJSON(aggregate.byOperation, clipReadAggregateTopN),
		"top_paths_json":      clipReadRollupsJSON(aggregate.byAccess, clipReadAggregateTopN),
		"top_sources_json":    clipReadRollupsJSON(aggregate.bySource, clipReadAggregateTopN),
		"total_duration_us":   strconv.FormatInt(aggregate.total.Microseconds(), 10),
		"wall_duration_us":    strconv.FormatInt(wallDuration.Microseconds(), 10),
	}
	if aggregate.firstError != "" {
		attrs["first_error"] = aggregate.firstError
	}
	for key, value := range aggregate.sampleAttrs {
		if key == "" {
			continue
		}
		attrs[key] = value
	}

	log.Info().
		Str("container_id", request.ContainerId).
		Str("image_id", request.ImageId).
		Str("flush_reason", flushReason).
		Int64("read_count", aggregate.readCount).
		Int64("bytes_read", aggregate.bytesRead).
		Int64("total_us", aggregate.total.Microseconds()).
		Int64("wall_us", wallDuration.Microseconds()).
		Int64("error_count", aggregate.errorCount).
		Str("top_sources_json", attrs["top_sources_json"]).
		Str("top_paths_json", attrs["top_paths_json"]).
		Str("top_operations_json", attrs["top_operations_json"]).
		Str("top_content_json", attrs["top_content_json"]).
		Str("first_error", aggregate.firstError).
		Msg("clip read path summary")

	success := aggregate.success
	c.eventRepo.PushContainerPhaseEvent(types.EventContainerPhaseSchema{
		ID:          types.ContainerPhaseClipRead,
		Domain:      types.EventDomainClip,
		StartTime:   aggregate.startedAt.UTC(),
		EndTime:     phaseEnd.UTC(),
		DurationMs:  aggregate.total.Milliseconds(),
		ContainerID: request.ContainerId,
		StubID:      request.StubId,
		StubType:    string(request.Stub.Type.Kind()),
		TaskID:      taskIDFromEnv(request.Env),
		WorkspaceID: request.WorkspaceId,
		WorkerID:    c.workerId,
		Success:     &success,
		Source:      "clip.fuse",
		Attrs:       attrs,
	})
}

func isCanonicalClipRead(operation string) bool {
	return operation == string(types.ContainerPhaseClipRead) || operation == string(types.ContainerPhaseClipOCIRead)
}

func clipReadRollupKey(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(part)
		b.WriteByte('\x00')
	}
	return b.String()
}

func clipReadRollupsJSON(rollups map[string]*clipReadRollup, limit int) string {
	if len(rollups) == 0 || limit <= 0 {
		return "[]"
	}

	items := make([]clipReadRollup, 0, len(rollups))
	for _, rollup := range rollups {
		if rollup == nil {
			continue
		}
		items = append(items, *rollup)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].TotalUs != items[j].TotalUs {
			return items[i].TotalUs > items[j].TotalUs
		}
		return items[i].MaxUs > items[j].MaxUs
	})
	if len(items) > limit {
		items = items[:limit]
	}

	data, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func firstNonEmptyImageValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func durationUsToMilliseconds(durationUs int64) int64 {
	if durationUs <= 0 {
		return 0
	}
	return (durationUs + 999) / 1000
}

func (c *ImageClient) trackContainerRuntimePID(request *types.ContainerRequest, pid int) {
	if c == nil || request == nil || pid <= 0 {
		return
	}

	c.clipRuntimeMu.Lock()
	defer c.clipRuntimeMu.Unlock()

	c.clipActive[request.ContainerId] = request
	c.clipRuntimePIDs[pid] = request.ContainerId
	c.clipPIDCache[pid] = request.ContainerId
}

func (c *ImageClient) untrackContainer(containerID string) {
	if c == nil || containerID == "" {
		return
	}

	var aggregate *clipReadAggregate

	c.clipRuntimeMu.Lock()
	delete(c.clipActive, containerID)
	aggregate = c.clipAggregates[containerID]
	delete(c.clipAggregates, containerID)
	for pid, id := range c.clipRuntimePIDs {
		if id == containerID {
			delete(c.clipRuntimePIDs, pid)
		}
	}
	for pid, id := range c.clipPIDCache {
		if id == containerID {
			delete(c.clipPIDCache, pid)
		}
	}
	c.clipRuntimeMu.Unlock()

	c.pushClipReadAggregate(aggregate, "container_untracked")
}

func (c *ImageClient) resolveClipReadRequest(pid int) (*types.ContainerRequest, bool) {
	containerID := c.resolveClipReadContainerID(pid)
	if containerID == "" {
		return nil, false
	}

	c.clipRuntimeMu.RLock()
	request, ok := c.clipActive[containerID]
	c.clipRuntimeMu.RUnlock()
	return request, ok
}

func (c *ImageClient) resolveClipReadContainerID(pid int) string {
	if c == nil || pid <= 0 {
		return ""
	}

	c.clipRuntimeMu.RLock()
	if containerID, ok := c.clipPIDCache[pid]; ok {
		c.clipRuntimeMu.RUnlock()
		return containerID
	}
	if containerID, ok := c.clipRuntimePIDs[pid]; ok {
		c.clipRuntimeMu.RUnlock()
		return containerID
	}
	c.clipRuntimeMu.RUnlock()

	current := pid
	for i := 0; i < clipReadPIDResolveMaxParents && current > 1; i++ {
		c.clipRuntimeMu.RLock()
		containerID, ok := c.clipRuntimePIDs[current]
		c.clipRuntimeMu.RUnlock()
		if ok {
			c.clipRuntimeMu.Lock()
			c.clipPIDCache[pid] = containerID
			c.clipRuntimeMu.Unlock()
			return containerID
		}

		parent, err := parentPID(current)
		if err != nil || parent <= 0 || parent == current {
			return ""
		}
		current = parent
	}

	return ""
}

func parentPID(pid int) (int, error) {
	proc, err := procfs.NewProc(pid)
	if err != nil {
		return 0, fmt.Errorf("open proc %d: %w", pid, err)
	}
	stat, err := proc.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat proc %d: %w", pid, err)
	}
	return int(stat.PPID), nil
}
