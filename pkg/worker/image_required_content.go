package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
)

const (
	requiredContentBatchFlushInterval = 250 * time.Millisecond
	requiredContentBatchQueueSize     = 8192
)

type requiredContentBatcher struct {
	client requiredContentBatchReporter
	ch     chan cache.RequiredContentItem
}

type requiredContentBatchReporter interface {
	Locality() string
	ReportRequiredContentBatch(ctx context.Context, items []cache.RequiredContentItem) error
}

type requiredContentBatchGroup struct {
	items map[string]cache.RequiredContentItem
}

func (g *requiredContentBatchGroup) add(item cache.RequiredContentItem) {
	itemKey := requiredContentBatchItemKey(item)
	if existing, ok := g.items[itemKey]; ok {
		item = cache.MergeRequiredContentItem(existing, item)
	}
	g.items[itemKey] = item
}

func (g *requiredContentBatchGroup) itemList() []cache.RequiredContentItem {
	if g == nil || len(g.items) == 0 {
		return nil
	}
	items := make([]cache.RequiredContentItem, 0, len(g.items))
	for _, item := range g.items {
		items = append(items, item)
	}
	return items
}

func newRequiredContentBatcher(client *cache.Client) *requiredContentBatcher {
	if client == nil {
		return nil
	}
	b := &requiredContentBatcher{
		client: client,
		ch:     make(chan cache.RequiredContentItem, requiredContentBatchQueueSize),
	}
	go b.run()
	return b
}

func (b *requiredContentBatcher) Enqueue(item cache.RequiredContentItem) {
	if b == nil || b.client == nil || item.Hash == "" {
		return
	}
	select {
	case b.ch <- item:
	default:
		go b.ReportNow(context.Background(), []cache.RequiredContentItem{item})
	}
}

func (b *requiredContentBatcher) ReportNow(ctx context.Context, items []cache.RequiredContentItem) {
	if b == nil || b.client == nil || len(items) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	groups := map[string]*requiredContentBatchGroup{}
	for _, item := range items {
		b.addToGroups(groups, item)
	}
	b.flushGroups(ctx, groups)
}

func (b *requiredContentBatcher) run() {
	ticker := time.NewTicker(requiredContentBatchFlushInterval)
	defer ticker.Stop()

	groups := map[string]*requiredContentBatchGroup{}
	for {
		select {
		case item := <-b.ch:
			b.addToGroups(groups, item)
		case <-ticker.C:
			b.flushGroups(context.Background(), groups)
			groups = map[string]*requiredContentBatchGroup{}
		}
	}
}

func (b *requiredContentBatcher) flushGroups(ctx context.Context, groups map[string]*requiredContentBatchGroup) {
	var wg sync.WaitGroup
	for _, group := range groups {
		items := group.itemList()
		if len(items) == 0 {
			continue
		}
		wg.Add(1)
		go func(items []cache.RequiredContentItem) {
			defer wg.Done()
			reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_ = b.client.ReportRequiredContentBatch(reportCtx, items)
		}(items)
	}
	wg.Wait()
}

func (b *requiredContentBatcher) addToGroups(groups map[string]*requiredContentBatchGroup, item cache.RequiredContentItem) {
	item = b.normalize(item)
	if item.Hash == "" || item.Locality == "" || item.WorkspaceID == "" || item.StubID == "" {
		return
	}
	groupKey := requiredContentBatchGroupKey(item)
	group := groups[groupKey]
	if group == nil {
		group = &requiredContentBatchGroup{items: map[string]cache.RequiredContentItem{}}
		groups[groupKey] = group
	}
	group.add(item)
}

func (b *requiredContentBatcher) normalize(item cache.RequiredContentItem) cache.RequiredContentItem {
	item = item.Normalized()
	if item.Locality == "" {
		item.Locality = b.client.Locality()
	}
	return item
}

func requiredContentBatchGroupKey(item cache.RequiredContentItem) string {
	return item.Locality + "\x00" + item.WorkspaceID + "\x00" + item.StubID + "\x00" + string(item.Kind)
}

func requiredContentBatchItemKey(item cache.RequiredContentItem) string {
	if item.RoutingKey == "" {
		item.RoutingKey = item.Hash
	}
	return item.Hash + "\x00" + item.RoutingKey
}

func (c *ImageClient) reportClipRequiredContentMetadata(ctx context.Context, request *types.ContainerRequest, meta *clipCommon.ClipArchiveMetadata) {
	if c == nil || c.requiredContent == nil || request == nil || meta == nil {
		return
	}
	items := c.clipRequiredContentItems(request, meta)
	if len(items) == 0 {
		return
	}
	go c.requiredContent.ReportNow(ctx, items)
}

func (c *ImageClient) clipRequiredContentItems(request *types.ContainerRequest, meta *clipCommon.ClipArchiveMetadata) []cache.RequiredContentItem {
	if ociInfo, ok := ociStorageInfo(meta); ok {
		return c.clipOCIRequiredContentItems(request, ociInfo)
	}
	return c.clipV1RequiredContentItems(request, meta)
}

func (c *ImageClient) clipOCIRequiredContentItems(request *types.ContainerRequest, ociInfo *clipCommon.OCIStorageInfo) []cache.RequiredContentItem {
	if request == nil || ociInfo == nil || len(ociInfo.DecompressedHashByLayer) == 0 {
		return nil
	}
	items := make([]cache.RequiredContentItem, 0, len(ociInfo.DecompressedHashByLayer))
	for layerDigest, decompressedHash := range ociInfo.DecompressedHashByLayer {
		if decompressedHash == "" {
			continue
		}
		items = append(items, cache.RequiredContentItem{
			WorkspaceID:  request.WorkspaceId,
			StubID:       request.StubId,
			Kind:         cache.RequiredContentKindClipOCI,
			Hash:         decompressedHash,
			RoutingKey:   decompressedHash,
			ExpectedHash: decompressedHash,
			Source: cache.RequiredContentSource{
				Type:        cache.RequiredContentSourceOCIRegistry,
				Descriptor:  fmt.Sprintf("image:%s layer:%s", request.ImageId, layerDigest),
				Registry:    ociInfo.RegistryURL,
				Repository:  ociInfo.Repository,
				Reference:   ociInfo.Reference,
				LayerDigest: layerDigest,
			},
		})
	}
	return items
}

func (c *ImageClient) clipV1RequiredContentItems(request *types.ContainerRequest, meta *clipCommon.ClipArchiveMetadata) []cache.RequiredContentItem {
	if request == nil || meta == nil || meta.Index == nil {
		return nil
	}
	seen := map[string]struct{}{}
	items := []cache.RequiredContentItem{}
	meta.Index.Ascend(meta.Index.Min(), func(value interface{}) bool {
		node, ok := value.(*clipCommon.ClipNode)
		if !ok || node == nil || node.ContentHash == "" {
			return true
		}
		if _, ok := seen[node.ContentHash]; ok {
			return true
		}
		seen[node.ContentHash] = struct{}{}
		size := int64(node.Attr.Size)
		items = append(items, cache.RequiredContentItem{
			WorkspaceID:  request.WorkspaceId,
			StubID:       request.StubId,
			Kind:         cache.RequiredContentKindClipV1,
			Hash:         node.ContentHash,
			RoutingKey:   node.ContentHash,
			ExpectedHash: node.ContentHash,
			SizeBytes:    size,
			Source: cache.RequiredContentSource{
				Type:       cache.RequiredContentSourceUnknown,
				Descriptor: fmt.Sprintf("image:%s", request.ImageId),
			},
		})
		return true
	})
	return items
}

func (c *ImageClient) reportClipRequiredContentCacheEvent(request *types.ContainerRequest, event imageContentCacheTrace) {
	if c == nil || c.requiredContent == nil || request == nil {
		return
	}
	if !strings.HasPrefix(event.Operation, "store_") || event.Result.IsErrorLike() || event.Hash == "" {
		return
	}
	kind := cache.RequiredContentKindClipV1
	if strings.Contains(event.Kind, "oci") {
		kind = cache.RequiredContentKindClipOCI
	}
	size := event.Bytes
	if size <= 0 {
		return
	}
	item := cache.RequiredContentItem{
		WorkspaceID:  request.WorkspaceId,
		StubID:       request.StubId,
		Kind:         kind,
		Hash:         event.Hash,
		RoutingKey:   firstNonEmptyImageValue(event.RoutingKey, event.Hash),
		ExpectedHash: event.Hash,
		SizeBytes:    size,
		Source: cache.RequiredContentSource{
			Type:       cache.RequiredContentSourceUnknown,
			Descriptor: fmt.Sprintf("image:%s kind:%s", request.ImageId, event.Kind),
		},
	}
	c.requiredContent.Enqueue(item)
}
