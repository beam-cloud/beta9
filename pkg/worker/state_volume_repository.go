package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

const (
	stateBlockObjectPrefix = "state/block.v1/objects"
	stateManifestMaxBytes  = 16 << 20
)

func stateBlockObjectKey(digest string) (string, error) {
	if !blockV1DigestPattern.MatchString(digest) {
		return "", fmt.Errorf("invalid block.v1 object digest %q", digest)
	}
	return path.Join(stateBlockObjectPrefix, digest[:2], digest), nil
}

type workspaceBlockV1CAS struct {
	client *clients.WorkspaceStorageClient
	cache  blockV1ContentCache
}

type blockV1ContentCache interface {
	ReadContentInto(ctx context.Context, hash string, offset int64, dst []byte, opts cache.ClientOptions) (int64, error)
	StoreContentFromS3Source(source cache.S3ContentSource, opts cache.StoreContentOptions) (string, error)
}

func (c *workspaceBlockV1CAS) Put(ctx context.Context, digest string, size int64, body io.Reader) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("workspace block.v1 CAS is unavailable")
	}
	key, err := stateBlockObjectKey(digest)
	if err != nil {
		return err
	}
	if size <= 0 || size > BlockV1ChunkSize && size > stateManifestMaxBytes {
		return fmt.Errorf("invalid block.v1 object size %d", size)
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return fmt.Errorf("read block.v1 object %s: %w", digest, err)
	}
	if int64(len(data)) != size {
		return fmt.Errorf("block.v1 object %s size mismatch: got %d, want %d", digest, len(data), size)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("block.v1 object %s digest mismatch", digest)
	}
	exists, head, err := c.client.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("head block.v1 object %s: %w", digest, err)
	}
	if exists {
		if head == nil || head.ContentLength == nil || *head.ContentLength != size {
			return fmt.Errorf("existing block.v1 object %s has unexpected size", digest)
		}
		if err := c.verifyOriginObject(ctx, key, digest, size); err != nil {
			return fmt.Errorf("verify existing block.v1 object %s: %w", digest, err)
		}
		c.populateCacheFromOrigin(ctx, key, digest, size, false)
		return nil
	}
	if err := c.client.UploadWithReader(ctx, key, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("upload block.v1 object %s: %w", digest, err)
	}
	// A successful upload response is not the durability boundary: proxies and
	// object-store shims can acknowledge a truncated/corrupt body. Re-read the
	// immutable key and authenticate exact size+digest before any manifest or DB
	// generation is allowed to reference it.
	if err := c.verifyOriginObject(ctx, key, digest, size); err != nil {
		return fmt.Errorf("verify uploaded block.v1 object %s: %w", digest, err)
	}
	c.populateCacheFromOrigin(ctx, key, digest, size, false)
	return nil
}

func (c *workspaceBlockV1CAS) verifyOriginObject(ctx context.Context, key, digest string, size int64) error {
	exists, head, err := c.client.Head(ctx, key)
	if err != nil {
		return err
	}
	if !exists || head == nil || head.ContentLength == nil || *head.ContentLength != size {
		return fmt.Errorf("origin HEAD returned unexpected size")
	}
	existing, err := c.client.DownloadWithReader(ctx, key)
	if err != nil {
		return err
	}
	existingData, readErr := io.ReadAll(io.LimitReader(existing, size+1))
	closeErr := existing.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if int64(len(existingData)) != size || !blockV1BytesMatchDigest(existingData, digest) {
		return fmt.Errorf("origin returned unexpected bytes")
	}
	return nil
}

func (c *workspaceBlockV1CAS) Get(ctx context.Context, digest string, expectedSize int64) (io.ReadCloser, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("workspace block.v1 CAS is unavailable")
	}
	key, err := stateBlockObjectKey(digest)
	if err != nil {
		return nil, err
	}
	if expectedSize <= 0 || expectedSize > BlockV1ChunkSize && expectedSize > stateManifestMaxBytes {
		return nil, fmt.Errorf("invalid expected block.v1 object size %d", expectedSize)
	}
	if c.cache != nil {
		data := make([]byte, expectedSize)
		read, cacheErr := c.cache.ReadContentInto(ctx, digest, 0, data, cache.ClientOptions{RoutingKey: digest})
		if cacheErr == nil && read == expectedSize && blockV1BytesMatchDigest(data, digest) {
			return io.NopCloser(bytes.NewReader(data)), nil
		}
	}

	origin, err := c.client.DownloadWithReader(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download block.v1 object %s: %w", digest, err)
	}
	data, readErr := io.ReadAll(io.LimitReader(origin, expectedSize+1))
	closeErr := origin.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read block.v1 object %s: %w", digest, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close block.v1 object %s: %w", digest, closeErr)
	}
	if int64(len(data)) != expectedSize || !blockV1BytesMatchDigest(data, digest) {
		return nil, fmt.Errorf("origin block.v1 object %s failed exact size/digest verification", digest)
	}
	// A miss or same-size corrupt cache page is repaired from the authenticated
	// immutable origin. StoreContentFromS3Source sends ExpectedHash=digest, so
	// the cache server validates all bytes before atomically replacing pages.
	c.populateCacheFromOrigin(ctx, key, digest, expectedSize, true)
	return io.NopCloser(bytes.NewReader(data)), nil
}

func blockV1BytesMatchDigest(data []byte, digest string) bool {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == digest
}

func (c *workspaceBlockV1CAS) populateCacheFromOrigin(ctx context.Context, key, digest string, expectedSize int64, force bool) {
	if c == nil || c.cache == nil || c.client == nil || c.client.WorkspaceStorage == nil {
		return
	}
	if !force && expectedSize > 0 {
		data := make([]byte, expectedSize)
		if read, err := c.cache.ReadContentInto(ctx, digest, 0, data, cache.ClientOptions{RoutingKey: digest}); err == nil && read == expectedSize && blockV1BytesMatchDigest(data, digest) {
			return
		}
	}
	storage := c.client.WorkspaceStorage
	if storage.BucketName == nil || storage.Region == nil || storage.AccessKey == nil || storage.SecretKey == nil {
		return
	}
	endpoint := ""
	if storage.EndpointUrl != nil {
		endpoint = *storage.EndpointUrl
	}
	if _, err := c.cache.StoreContentFromS3Source(cache.S3ContentSource{
		Path: key, CachePath: digest, BucketName: *storage.BucketName, Region: *storage.Region,
		EndpointURL: endpoint, AccessKey: *storage.AccessKey, SecretKey: *storage.SecretKey, ForcePathStyle: true,
	}, cache.StoreContentOptions{RoutingKey: digest, Lock: true}); err != nil {
		log.Debug().Err(err).Str("digest", digest).Msg("block.v1 cache population deferred to reconciliation")
	}
}

type repositoryBlockV1Resolver struct {
	workspaceID string
	repository  pb.BackendRepositoryServiceClient
	cas         BlockV1CAS
}

func (r *repositoryBlockV1Resolver) ResolveBlockV1Manifest(ctx context.Context, generationID string) (BlockV1Manifest, error) {
	if r == nil || r.repository == nil || r.cas == nil {
		return BlockV1Manifest{}, fmt.Errorf("block.v1 manifest resolver is unavailable")
	}
	response, err := r.repository.GetVolumeGeneration(ctx, &pb.GetVolumeGenerationRequest{
		WorkspaceId: r.workspaceID, GenerationId: generationID,
	})
	if err != nil {
		return BlockV1Manifest{}, err
	}
	if response == nil || !response.Ok || response.Generation == nil {
		message := "volume generation not found"
		if response != nil && response.ErrorMsg != "" {
			message = response.ErrorMsg
		}
		return BlockV1Manifest{}, fmt.Errorf("resolve generation %q: %s", generationID, message)
	}
	generation := response.Generation
	if generation.ExternalId != generationID || !blockV1DigestPattern.MatchString(generation.ManifestDigest) {
		return BlockV1Manifest{}, fmt.Errorf("generation %q has invalid identity or manifest digest", generationID)
	}
	expectedKey, err := stateBlockObjectKey(generation.ManifestDigest)
	if err != nil {
		return BlockV1Manifest{}, err
	}
	if generation.ManifestKey != expectedKey {
		return BlockV1Manifest{}, fmt.Errorf("generation %q has untrusted manifest key %q", generationID, generation.ManifestKey)
	}
	reader, err := r.cas.Get(ctx, generation.ManifestDigest, generation.ManifestSizeBytes)
	if err != nil {
		return BlockV1Manifest{}, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, stateManifestMaxBytes+1))
	if err != nil {
		return BlockV1Manifest{}, err
	}
	if len(data) == 0 || len(data) > stateManifestMaxBytes || int64(len(data)) != generation.ManifestSizeBytes {
		return BlockV1Manifest{}, fmt.Errorf("generation %q manifest has invalid size", generationID)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != generation.ManifestDigest {
		return BlockV1Manifest{}, fmt.Errorf("generation %q manifest digest mismatch", generationID)
	}
	var manifest BlockV1Manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return BlockV1Manifest{}, fmt.Errorf("decode generation %q manifest: %w", generationID, err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return BlockV1Manifest{}, fmt.Errorf("generation %q manifest has trailing data", generationID)
	}
	canonical, canonicalDigest, err := EncodeBlockV1ManifestCanonical(manifest)
	if err != nil {
		return BlockV1Manifest{}, err
	}
	if canonicalDigest != generation.ManifestDigest || !bytes.Equal(canonical, data) {
		return BlockV1Manifest{}, fmt.Errorf("generation %q manifest is not canonical", generationID)
	}
	if manifest.GenerationID != generationID || manifest.VolumeID != generation.VolumeId {
		return BlockV1Manifest{}, fmt.Errorf("generation %q manifest identity mismatch", generationID)
	}
	storedSize := int64(0)
	for _, chunk := range manifest.Chunks {
		storedSize += chunk.SizeBytes
	}
	if manifest.ParentGenerationID != generation.ParentGenerationId ||
		manifest.CloneParentGenerationID != generation.CloneParentGenerationId ||
		manifest.Generation != generation.Generation ||
		manifest.VirtualSizeBytes != generation.LogicalSizeBytes ||
		int64(len(manifest.Chunks)) != generation.ChunkCount ||
		storedSize != generation.StoredSizeBytes {
		return BlockV1Manifest{}, fmt.Errorf("generation %q repository metadata does not match its manifest", generationID)
	}
	return manifest, nil
}
