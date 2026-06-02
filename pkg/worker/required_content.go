package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
)

const requiredContentReportBatchSize = 256

func (c *ImageClient) reportClipRequiredContentMetadata(ctx context.Context, request *types.ContainerRequest, meta *clipCommon.ClipArchiveMetadata) {
	if c == nil || c.cacheClient == nil || request == nil || meta == nil {
		return
	}
	items := c.clipRequiredContentItems(request, meta)
	if len(items) == 0 {
		return
	}
	go c.reportRequiredContentItems(ctx, items)
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
	if c == nil || c.cacheClient == nil || request == nil {
		return
	}
	if !strings.HasPrefix(event.Operation, "store_") || event.Result == "error" || event.Hash == "" {
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
	go c.reportRequiredContentItems(context.Background(), []cache.RequiredContentItem{item})
}

func (c *ImageClient) reportRequiredContentItems(ctx context.Context, items []cache.RequiredContentItem) {
	if c == nil || c.cacheClient == nil || len(items) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for start := 0; start < len(items); start += requiredContentReportBatchSize {
		end := start + requiredContentReportBatchSize
		if end > len(items) {
			end = len(items)
		}
		reportCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = c.cacheClient.ReportRequiredContentBatch(reportCtx, items[start:end])
		cancel()
	}
}
