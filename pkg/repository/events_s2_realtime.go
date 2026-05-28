package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	defaultS2LogReadLimit = 100
	maxS2LogReadLimit     = 1000
	s2MetricsReadLimit    = 1000
)

func (r *S2EventRepository) GetLogs(ctx context.Context, query types.LogQuery) (*types.LogsResponse, error) {
	limit := query.Limit
	if limit == 0 {
		limit = defaultS2LogReadLimit
	}
	if limit > maxS2LogReadLimit {
		limit = maxS2LogReadLimit
	}

	if err := r.ensureBasin(ctx); err != nil {
		return nil, err
	}

	streams, err := r.resolveLogStreams(ctx, query, false)
	if err != nil {
		return nil, err
	}

	response := &types.LogsResponse{
		ObjectID:   query.ObjectID,
		ObjectType: query.ObjectType,
		Logs:       []types.LogRecord{},
		Streams:    make([]string, 0, len(streams)),
	}
	for _, streamName := range streams {
		total, logs, err := r.readLogStreamPage(ctx, streamName, query, limit)
		if err != nil {
			return nil, err
		}
		response.TotalExpected += total
		response.Streams = append(response.Streams, string(streamName))
		response.Logs = append(response.Logs, logs...)
	}

	sort.SliceStable(response.Logs, func(i, j int) bool {
		if response.Logs[i].Timestamp.Equal(response.Logs[j].Timestamp) {
			return response.Logs[i].SeqNum < response.Logs[j].SeqNum
		}
		return response.Logs[i].Timestamp.Before(response.Logs[j].Timestamp)
	})

	loadedThrough := int((query.Page + 1) * limit)
	if response.TotalExpected > loadedThrough {
		response.NextCursor = strconv.FormatUint(query.Page+1, 10)
	}
	return response, nil
}

func (r *S2EventRepository) StreamLogs(ctx context.Context, query types.LogQuery) (EventStream, error) {
	if err := r.ensureBasin(ctx); err != nil {
		return nil, err
	}

	streams, err := r.resolveLogStreams(ctx, query, true)
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return nil, fmt.Errorf("no log stream target")
	}

	opts := &s2.ReadOptions{
		SeqNum:     query.SeqNum,
		TailOffset: query.TailOffset,
		Wait:       query.WaitSeconds,
		Clamp:      query.Clamp,
	}
	if query.StartTime != nil {
		timestamp := uint64(query.StartTime.UTC().UnixMilli())
		opts.Timestamp = &timestamp
	}
	session, err := r.basin.Stream(streams[0]).ReadSession(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("stream logs from s2 stream %q: %w", streams[0], err)
	}

	return &s2LogEventStream{
		session: session,
		query:   query,
	}, nil
}

func (r *S2EventRepository) GetStubMetricsTimeseries(ctx context.Context, query types.EventQuery, start time.Time, end time.Time, interval string) (*types.MetricsTimeseriesResponse, error) {
	response := &types.MetricsTimeseriesResponse{}
	if query.WorkspaceID == "" || query.StubID == "" {
		return response, nil
	}

	return r.getMetricsTimeseries(ctx, r.stubStreamName(query.WorkspaceID, query.StubID), start, end, interval)
}

func (r *S2EventRepository) GetWorkspaceMetricsTimeseries(ctx context.Context, query types.EventQuery, start time.Time, end time.Time, interval string) (*types.MetricsTimeseriesResponse, error) {
	response := &types.MetricsTimeseriesResponse{}
	if query.WorkspaceID == "" {
		return response, nil
	}

	return r.getMetricsTimeseries(ctx, r.workspaceStreamName(query.WorkspaceID), start, end, interval)
}

func (r *S2EventRepository) getMetricsTimeseries(ctx context.Context, streamName s2.StreamName, start time.Time, end time.Time, interval string) (*types.MetricsTimeseriesResponse, error) {
	response := &types.MetricsTimeseriesResponse{}
	if err := r.ensureBasin(ctx); err != nil {
		return nil, err
	}

	exists, err := r.streamExists(ctx, streamName)
	if err != nil {
		return nil, err
	}
	if !exists {
		response.Timeseries.AggregationBuckets = []types.MetricsAggregationBucket{}
		return response, nil
	}

	buckets := map[int64]*metricsBucketAccumulator{}
	seqNum := uint64(0)
	startMs := uint64(start.UTC().UnixMilli())
	endMs := uint64(end.UTC().UnixMilli())
	for {
		opts := &s2.ReadOptions{
			SeqNum: &seqNum,
			Count:  countOption(s2MetricsReadLimit),
			Until:  &endMs,
		}
		if seqNum == 0 {
			opts.Timestamp = &startMs
			opts.SeqNum = nil
		}

		batch, err := r.basin.Stream(streamName).Read(ctx, opts)
		if err != nil {
			if isS2RangeNotSatisfiable(err) {
				break
			}
			return nil, fmt.Errorf("read metrics from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			break
		}

		for _, record := range batch.Records {
			metrics, eventTime, ok := containerMetricsFromS2(record)
			if !ok {
				continue
			}
			if eventTime.Before(start) || !eventTime.Before(end) {
				continue
			}
			key := metricsBucketKey(eventTime, interval)
			acc := buckets[key]
			if acc == nil {
				acc = &metricsBucketAccumulator{key: key}
				buckets[key] = acc
			}
			acc.add(metrics)
		}

		last := batch.Records[len(batch.Records)-1]
		seqNum = last.SeqNum + 1
		if len(batch.Records) < s2MetricsReadLimit {
			break
		}
	}

	keys := make([]int64, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	response.Timeseries.AggregationBuckets = make([]types.MetricsAggregationBucket, 0, len(keys))
	for _, key := range keys {
		response.Timeseries.AggregationBuckets = append(response.Timeseries.AggregationBuckets, buckets[key].bucket())
	}
	return response, nil
}

func (r *S2EventRepository) resolveLogStreams(ctx context.Context, query types.LogQuery, createCanonical bool) ([]s2.StreamName, error) {
	addIfExists := func(streamName s2.StreamName) ([]s2.StreamName, error) {
		if streamName == "" {
			return nil, nil
		}
		if createCanonical {
			if err := r.ensureStream(ctx, streamName); err != nil {
				return nil, err
			}
			return []s2.StreamName{streamName}, nil
		}
		exists, err := r.streamExists(ctx, streamName)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}
		return []s2.StreamName{streamName}, nil
	}

	switch {
	case query.TaskID != "" && query.WorkspaceID != "":
		return addIfExists(r.taskLogStreamName(query.WorkspaceID, query.TaskID))
	case query.ContainerID != "" && query.WorkspaceID != "" && query.StubID != "":
		return addIfExists(r.containerLogStreamName(query.WorkspaceID, query.StubID, query.ContainerID))
	case query.ContainerID != "" && query.WorkspaceID != "":
		return r.findContainerLogStreams(ctx, query.WorkspaceID, query.ContainerID)
	case query.StubID != "" && query.WorkspaceID != "":
		return addIfExists(r.stubLogStreamName(query.WorkspaceID, query.StubID))
	case query.AppID != "" && query.WorkspaceID != "":
		return addIfExists(r.appLogStreamName(query.WorkspaceID, query.AppID))
	case query.WorkspaceID != "":
		return addIfExists(r.workspaceLogStreamName(query.WorkspaceID))
	default:
		return nil, nil
	}
}

func (r *S2EventRepository) findContainerLogStreams(ctx context.Context, workspaceID string, containerID string) ([]s2.StreamName, error) {
	prefix := r.workspaceLogStubPrefix(workspaceID)
	suffix := "/containers/" + eventStreamPart(containerID)
	streams := make([]s2.StreamName, 0, 1)
	iter := r.basin.Streams.Iter(ctx, &s2.ListStreamsArgs{Prefix: prefix})
	for iter.Next() {
		stream := iter.Value()
		if strings.HasSuffix(string(stream.Name), suffix) {
			streams = append(streams, stream.Name)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("list s2 container log streams for workspace %q: %w", workspaceID, err)
	}
	return streams, nil
}

func (r *S2EventRepository) readLogStreamPage(ctx context.Context, streamName s2.StreamName, query types.LogQuery, limit uint64) (int, []types.LogRecord, error) {
	tail, err := r.basin.Stream(streamName).CheckTail(ctx)
	if err != nil {
		if isS2RangeNotSatisfiable(err) {
			return 0, nil, nil
		}
		return 0, nil, fmt.Errorf("check tail for s2 log stream %q: %w", streamName, err)
	}
	total := int(tail.Tail.SeqNum)
	if total == 0 {
		return total, []types.LogRecord{}, nil
	}

	tailOffset := int64((query.Page + 1) * limit)
	if tailOffset > int64(tail.Tail.SeqNum) {
		tailOffset = int64(tail.Tail.SeqNum)
	}
	batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{
		TailOffset: &tailOffset,
		Count:      &limit,
	})
	if err != nil {
		if isS2RangeNotSatisfiable(err) {
			return total, []types.LogRecord{}, nil
		}
		return 0, nil, fmt.Errorf("read logs from s2 stream %q: %w", streamName, err)
	}

	logs := make([]types.LogRecord, 0, len(batch.Records))
	for _, record := range batch.Records {
		logRecord, ok := logRecordFromS2(record)
		if !ok || !logRecordMatchesQuery(logRecord, query) {
			continue
		}
		logs = append(logs, logRecord)
	}
	return total, logs, nil
}

func logRecordFromS2(record s2.SequencedRecord) (types.LogRecord, bool) {
	eventRecord, ok := containerEventRecordFromS2(record, types.EventQuery{EventTypes: []string{types.EventContainerLog}}, &types.ContainerEventsResponse{})
	if !ok || eventRecord.Type != types.EventContainerLog {
		return types.LogRecord{}, false
	}
	timestamp := eventRecord.Timestamp
	if timestamp.IsZero() {
		timestamp = time.UnixMilli(int64(record.Timestamp)).UTC()
	}
	return types.LogRecord{
		SeqNum:      eventRecord.SeqNum,
		StoredAtNs:  eventRecord.StoredAtNs,
		Timestamp:   timestamp,
		Message:     eventRecord.Line,
		Stream:      eventRecord.Stream,
		ContainerID: eventRecord.ContainerID,
		StubID:      eventRecord.StubID,
		StubType:    eventRecord.StubType,
		TaskID:      eventRecord.TaskID,
		WorkspaceID: eventRecord.WorkspaceID,
		AppID:       eventRecord.AppID,
		WorkerID:    eventRecord.WorkerID,
	}, true
}

func logRecordMatchesQuery(record types.LogRecord, query types.LogQuery) bool {
	if query.TaskID != "" && record.TaskID != query.TaskID {
		return false
	}
	if query.ContainerID != "" && record.ContainerID != query.ContainerID {
		return false
	}
	if query.StartTime != nil && record.Timestamp.Before(query.StartTime.UTC()) {
		return false
	}
	if query.EndTime != nil && !record.Timestamp.Before(query.EndTime.UTC()) {
		return false
	}
	if query.Query != "" && !strings.Contains(strings.ToLower(record.Message), strings.ToLower(query.Query)) {
		return false
	}
	return true
}

type s2LogEventStream struct {
	session *s2.ReadSession
	query   types.LogQuery
	current types.ContainerEventRecord
}

func (s *s2LogEventStream) Next() bool {
	for s.session.Next() {
		logRecord, ok := logRecordFromS2(s.session.Record())
		if !ok || !logRecordMatchesQuery(logRecord, s.query) {
			continue
		}
		s.current = containerEventRecordFromLogRecord(logRecord)
		return true
	}
	return false
}

func (s *s2LogEventStream) Record() types.ContainerEventRecord {
	return s.current
}

func (s *s2LogEventStream) Err() error {
	return s.session.Err()
}

func (s *s2LogEventStream) Close() error {
	return s.session.Close()
}

func containerEventRecordFromLogRecord(record types.LogRecord) types.ContainerEventRecord {
	return types.ContainerEventRecord{
		SeqNum:      record.SeqNum,
		StoredAtNs:  record.StoredAtNs,
		Timestamp:   record.Timestamp,
		Type:        types.EventContainerLog,
		Line:        record.Message,
		Stream:      record.Stream,
		ContainerID: record.ContainerID,
		StubID:      record.StubID,
		StubType:    record.StubType,
		TaskID:      record.TaskID,
		WorkspaceID: record.WorkspaceID,
		AppID:       record.AppID,
		WorkerID:    record.WorkerID,
	}
}

func containerMetricsFromS2(record s2.SequencedRecord) (types.EventContainerMetricsSchema, time.Time, bool) {
	var envelope struct {
		Type string                            `json:"type"`
		Time time.Time                         `json:"time"`
		Data types.EventContainerMetricsSchema `json:"data"`
	}
	if err := json.Unmarshal(record.Body, &envelope); err != nil {
		log.Debug().Err(err).Msg("failed to unmarshal metrics event")
		return types.EventContainerMetricsSchema{}, time.Time{}, false
	}
	if envelope.Type != types.EventContainerMetrics {
		return types.EventContainerMetricsSchema{}, time.Time{}, false
	}
	if envelope.Time.IsZero() {
		envelope.Time = time.UnixMilli(int64(record.Timestamp)).UTC()
	}
	return envelope.Data, envelope.Time, true
}

func metricsBucketKey(t time.Time, interval string) int64 {
	switch strings.ToLower(interval) {
	case "1m", "minute":
		return t.UTC().Truncate(time.Minute).UnixMilli()
	default:
		return t.UTC().Truncate(time.Hour).UnixMilli()
	}
}

type metricsBucketAccumulator struct {
	key   int64
	count int

	containerResources map[string]containerResourceSample
	containerIORates   map[string]*containerIORateAccumulator

	cpuPct             float64
	cpuTotal           float64
	cpuUsed            float64
	diskReadBytes      float64
	diskWriteBytes     float64
	diskReadRate       float64
	diskWriteRate      float64
	gpuMemoryTotal     float64
	gpuMemoryUsed      float64
	memoryRSS          float64
	memoryTotal        float64
	memoryVMS          float64
	memorySwap         float64
	networkBytesRecv   float64
	networkBytesSent   float64
	networkRecvRate    float64
	networkSentRate    float64
	networkPacketsRecv float64
	networkPacketsSent float64
}

type containerResourceSample struct {
	cpuMillicores float64
	gpuCount      float64
}

type containerIORateAccumulator struct {
	count           int
	diskReadRate    float64
	diskWriteRate   float64
	networkRecvRate float64
	networkSentRate float64
}

func (a *metricsBucketAccumulator) add(sample types.EventContainerMetricsSchema) {
	metrics := sample.ContainerMetrics
	sampleSeconds := metricSampleSeconds(metrics)
	diskReadRate := float64(metrics.DiskReadBytes) / sampleSeconds
	diskWriteRate := float64(metrics.DiskWriteBytes) / sampleSeconds
	networkRecvRate := float64(metrics.NetworkBytesRecv) / sampleSeconds
	networkSentRate := float64(metrics.NetworkBytesSent) / sampleSeconds
	a.count++
	if sample.ContainerID != "" {
		if a.containerResources == nil {
			a.containerResources = map[string]containerResourceSample{}
		}
		cpu := float64(sample.CPU)
		if cpu == 0 {
			cpu = float64(metrics.CPUTotal)
		}
		a.containerResources[sample.ContainerID] = containerResourceSample{
			cpuMillicores: cpu,
			gpuCount:      float64(sample.GPUCount),
		}
		if a.containerIORates == nil {
			a.containerIORates = map[string]*containerIORateAccumulator{}
		}
		rate := a.containerIORates[sample.ContainerID]
		if rate == nil {
			rate = &containerIORateAccumulator{}
			a.containerIORates[sample.ContainerID] = rate
		}
		rate.count++
		rate.diskReadRate += diskReadRate
		rate.diskWriteRate += diskWriteRate
		rate.networkRecvRate += networkRecvRate
		rate.networkSentRate += networkSentRate
	}
	a.cpuPct += float64(metrics.CPUPercent)
	a.cpuTotal += float64(metrics.CPUTotal)
	a.cpuUsed += float64(metrics.CPUUsed)
	a.diskReadBytes += float64(metrics.DiskReadBytes)
	a.diskWriteBytes += float64(metrics.DiskWriteBytes)
	a.diskReadRate += diskReadRate
	a.diskWriteRate += diskWriteRate
	a.gpuMemoryTotal += float64(metrics.GPUMemoryTotal)
	a.gpuMemoryUsed += float64(metrics.GPUMemoryUsed)
	a.memoryRSS += float64(metrics.MemoryRSS)
	a.memoryTotal += float64(metrics.MemoryTotal)
	a.memoryVMS += float64(metrics.MemoryVMS)
	a.memorySwap += float64(metrics.MemorySwap)
	a.networkBytesRecv += float64(metrics.NetworkBytesRecv)
	a.networkBytesSent += float64(metrics.NetworkBytesSent)
	a.networkRecvRate += networkRecvRate
	a.networkSentRate += networkSentRate
	a.networkPacketsRecv += float64(metrics.NetworkPacketsRecv)
	a.networkPacketsSent += float64(metrics.NetworkPacketsSent)
}

func metricSampleSeconds(metrics types.EventContainerMetricsData) float64 {
	if metrics.SampleIntervalMs <= 0 {
		return 1
	}
	return float64(metrics.SampleIntervalMs) / 1000
}

func (a *metricsBucketAccumulator) bucket() types.MetricsAggregationBucket {
	avg := func(total float64) types.MetricAverage {
		if a.count == 0 {
			return types.MetricAverage{}
		}
		return types.MetricAverage{Value: total / float64(a.count)}
	}
	resourceTotal := func(selector func(containerResourceSample) float64) types.MetricAverage {
		total := 0.0
		for _, resource := range a.containerResources {
			total += selector(resource)
		}
		return types.MetricAverage{Value: total}
	}
	ioRateTotal := func(fallbackTotal float64, selector func(*containerIORateAccumulator) float64) types.MetricAverage {
		if len(a.containerIORates) == 0 {
			return avg(fallbackTotal)
		}
		total := 0.0
		for _, rate := range a.containerIORates {
			if rate.count == 0 {
				continue
			}
			total += selector(rate) / float64(rate.count)
		}
		return types.MetricAverage{Value: total}
	}
	return types.MetricsAggregationBucket{
		Key:                     a.key,
		KeyAsString:             time.UnixMilli(a.key).UTC().Format(time.RFC3339),
		DocCount:                a.count,
		ContainerCount:          types.MetricAverage{Value: float64(len(a.containerResources))},
		CPUConcurrency:          resourceTotal(func(resource containerResourceSample) float64 { return resource.cpuMillicores }),
		GPUConcurrency:          resourceTotal(func(resource containerResourceSample) float64 { return resource.gpuCount }),
		DiskReadBytesRateAvg:    ioRateTotal(a.diskReadRate, func(rate *containerIORateAccumulator) float64 { return rate.diskReadRate }),
		DiskWriteBytesRateAvg:   ioRateTotal(a.diskWriteRate, func(rate *containerIORateAccumulator) float64 { return rate.diskWriteRate }),
		NetworkRecvBytesRateAvg: ioRateTotal(a.networkRecvRate, func(rate *containerIORateAccumulator) float64 { return rate.networkRecvRate }),
		NetworkSentBytesRateAvg: ioRateTotal(a.networkSentRate, func(rate *containerIORateAccumulator) float64 { return rate.networkSentRate }),
		CPUPercentAvg:           avg(a.cpuPct),
		CPUTotalAvg:             avg(a.cpuTotal),
		CPUUsedAvg:              avg(a.cpuUsed),
		DiskReadBytesAvg:        avg(a.diskReadBytes),
		DiskWriteBytesAvg:       avg(a.diskWriteBytes),
		GPUMemoryTotalBytesAvg:  avg(a.gpuMemoryTotal),
		GPUMemoryUsedBytesAvg:   avg(a.gpuMemoryUsed),
		MemoryRSSBytesAvg:       avg(a.memoryRSS),
		MemoryTotalBytesAvg:     avg(a.memoryTotal),
		MemoryVMSBytesAvg:       avg(a.memoryVMS),
		MemorySwapBytesAvg:      avg(a.memorySwap),
		NetworkRecvBytesAvg:     avg(a.networkBytesRecv),
		NetworkSentBytesAvg:     avg(a.networkBytesSent),
		NetworkRecvPacketsAvg:   avg(a.networkPacketsRecv),
		NetworkSentPacketsAvg:   avg(a.networkPacketsSent),
	}
}

func isS2RangeNotSatisfiable(err error) bool {
	var s2Err *s2.S2Error
	return err != nil && (strings.Contains(err.Error(), "416") || (errors.As(err, &s2Err) && s2Err.Status == httpStatusRangeNotSatisfiable))
}

const httpStatusRangeNotSatisfiable = 416
