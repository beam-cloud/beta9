package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

// maxStubCacheReadRecords bounds how many records ReadStubCacheRequiredContent
// will page through. Required content is coalesced to a small number of events
// per stub, so this is far above realistic sizes; it exists only to guarantee
// the read terminates even if the stream is continuously appended.
const maxStubCacheReadRecords = 200000

// stubCacheContents holds the required content coalesced so far from each
// stub's cache stream.
type stubCacheContents struct {
	mu       sync.Mutex
	byStream map[s2.StreamName]*stubCacheContent
}

type stubCacheContent struct {
	mu         sync.Mutex
	nextSeqNum uint64
	items      map[string]types.CacheRequiredContentItem
}

func (c *stubCacheContents) get(streamName s2.StreamName) *stubCacheContent {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.byStream == nil {
		c.byStream = map[s2.StreamName]*stubCacheContent{}
	}
	content := c.byStream[streamName]
	if content == nil {
		content = &stubCacheContent{items: map[string]types.CacheRequiredContentItem{}}
		c.byStream[streamName] = content
	}
	return content
}

// ReadStubCacheRequiredContent reads the coalesced required-content set for a
// stub from the dedicated S2 cache stream. Items are merged by
// immutable content identity or mutable source path, keeping the latest report.
func (r *S2EventRepository) ReadStubCacheRequiredContent(ctx context.Context, workspaceID, stubID string) ([]types.CacheRequiredContentItem, error) {
	if workspaceID == "" || stubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required to read stub cache required content")
	}

	streamName := r.stubCacheStreamName(workspaceID, stubID)
	content := r.stubCache.get(streamName)
	content.mu.Lock()
	defer content.mu.Unlock()

	// Read only records appended since this repository last coalesced the stub.
	// Reconciliation calls this repeatedly for recent stubs; restarting from
	// sequence 0 every cycle makes read bytes grow with stream age.
	recordsRead := 0
	for recordsRead < maxStubCacheReadRecords {
		count := uint64(defaultS2EventReadLimit)
		batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{
			SeqNum: &content.nextSeqNum,
			Count:  &count,
		})
		if isS2ReadEmpty(err) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read stub cache required content from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			break
		}
		recordsRead += len(batch.Records)

		for _, record := range batch.Records {
			mergeStubCacheRequiredContentRecord(content.items, record.Body)
		}

		content.nextSeqNum = batch.Records[len(batch.Records)-1].SeqNum + 1
		if uint64(len(batch.Records)) < count {
			break
		}
	}
	if recordsRead >= maxStubCacheReadRecords {
		return nil, fmt.Errorf("stub cache required-content read reached record cap (%d); snapshot incomplete", maxStubCacheReadRecords)
	}

	return stubCacheRequiredContentItems(content.items), nil
}

func mergeStubCacheRequiredContentRecord(merged map[string]types.CacheRequiredContentItem, body []byte) {
	var envelope struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return
	}
	if envelope.Type != types.EventStubCacheRequiredContent {
		return
	}
	var schema types.EventStubCacheRequiredContentSchema
	if err := json.Unmarshal(envelope.Data, &schema); err != nil {
		return
	}
	for _, item := range schema.Items {
		if item.Kind == "" {
			item.Kind = schema.Kind
		}
		if item.Retired {
			retireStubCacheRequiredContentItem(merged, item)
			continue
		}
		if item.Hash == "" {
			continue
		}
		mergeStubCacheRequiredContentItem(merged, item)
	}
}

// retireStubCacheRequiredContentItem withdraws a stub's checkpoint if it is
// still the one named. A checkpoint reported since stays: the record of a
// restore can be published after the next pause's. What was retired is
// remembered in its place, so that a report of it that is published late is
// not taken for a new checkpoint.
func retireStubCacheRequiredContentItem(merged map[string]types.CacheRequiredContentItem, item types.CacheRequiredContentItem) {
	if item.Kind != types.CacheContentKindCheckpoint || item.CheckpointID == "" {
		return
	}
	key := stubCacheRequiredContentItemKey(item)
	if current, ok := merged[key]; ok && !current.Retired && current.CheckpointID != item.CheckpointID {
		return
	}
	merged[key] = item
}

func stubCacheRequiredContentItems(merged map[string]types.CacheRequiredContentItem) []types.CacheRequiredContentItem {
	items := make([]types.CacheRequiredContentItem, 0, len(merged))
	for _, item := range merged {
		if !item.Retired {
			items = append(items, item)
		}
	}
	return items
}

func mergeStubCacheRequiredContentItem(merged map[string]types.CacheRequiredContentItem, item types.CacheRequiredContentItem) {
	item = normalizeStubCacheRequiredContentItem(item)
	if item.Kind == types.CacheContentKindCheckpoint {
		if current, ok := merged[stubCacheRequiredContentItemKey(item)]; ok && current.Retired && current.CheckpointID == item.CheckpointID {
			return
		}
	}
	if item.Kind == types.CacheContentKindDiskSnapshot && item.DiskName != "" {
		latest := latestDiskSnapshotContentGeneration(merged, item.DiskName)
		if item.SnapshotGeneration < latest {
			return
		}
		if item.SnapshotGeneration > latest {
			pruneDiskSnapshotContentGenerationsBefore(merged, item.DiskName, item.SnapshotGeneration)
		}
	}
	merged[stubCacheRequiredContentItemKey(item)] = item
}

func normalizeStubCacheRequiredContentItem(item types.CacheRequiredContentItem) types.CacheRequiredContentItem {
	if item.Kind != types.CacheContentKindDiskSnapshot || (item.DiskName != "" && item.SnapshotGeneration > 0) {
		return item
	}
	diskName, generation := diskSnapshotContentIdentity(item.Source)
	if item.DiskName == "" {
		item.DiskName = diskName
	}
	if item.SnapshotGeneration == 0 {
		item.SnapshotGeneration = generation
	}
	return item
}

func stubCacheRequiredContentItemKey(item types.CacheRequiredContentItem) string {
	if item.Kind == types.CacheContentKindDiskSnapshot && item.DiskName != "" {
		return strings.Join([]string{
			string(item.Kind),
			item.DiskName,
			strconv.FormatInt(item.SnapshotGeneration, 10),
			item.Hash,
			item.RoutingKey,
		}, "\x00")
	}
	return item.CoalescingKey()
}

func latestDiskSnapshotContentGeneration(merged map[string]types.CacheRequiredContentItem, diskName string) int64 {
	var latest int64
	for _, item := range merged {
		if item.Kind == types.CacheContentKindDiskSnapshot && item.DiskName == diskName && item.SnapshotGeneration > latest {
			latest = item.SnapshotGeneration
		}
	}
	return latest
}

func pruneDiskSnapshotContentGenerationsBefore(merged map[string]types.CacheRequiredContentItem, diskName string, generation int64) {
	for key, item := range merged {
		if item.Kind == types.CacheContentKindDiskSnapshot && item.DiskName == diskName && item.SnapshotGeneration < generation {
			delete(merged, key)
		}
	}
}

func diskSnapshotContentIdentity(source string) (string, int64) {
	parts := strings.Split(strings.Trim(source, "/"), "/")
	if len(parts) < 3 || parts[0] != "durable-disks" {
		return "", 0
	}
	if len(parts) >= 5 && parts[2] == "snapshots" {
		generation, _ := strconv.ParseInt(parts[3], 10, 64)
		return parts[1], generation
	}
	if parts[2] == "chunks" {
		return parts[1], 0
	}
	return "", 0
}
