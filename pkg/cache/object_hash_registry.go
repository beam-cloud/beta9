package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const volumeObjectHashMetadataVersion = "volume-object-hash-v1"

// VolumeObjectIdentity names one immutable view of a workspace volume object.
// Scope is the workspace id; the remaining fields come from the origin HEAD.
type VolumeObjectIdentity struct {
	Scope    string
	Endpoint string
	Bucket   string
	Path     string
	ETag     string
	Size     uint64
}

func canonicalVolumeObjectETag(etag string) string {
	etag = strings.TrimSpace(etag)
	if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
		return etag[1 : len(etag)-1]
	}
	return etag
}

func normalizeVolumeObjectIdentity(identity VolumeObjectIdentity) (VolumeObjectIdentity, error) {
	identity.Scope = strings.TrimSpace(identity.Scope)
	identity.Endpoint = strings.TrimRight(strings.TrimSpace(identity.Endpoint), "/")
	identity.Bucket = strings.TrimSpace(identity.Bucket)
	identity.ETag = canonicalVolumeObjectETag(identity.ETag)
	if identity.Scope == "" || identity.Bucket == "" || identity.Path == "" || identity.ETag == "" || identity.Size == 0 {
		return VolumeObjectIdentity{}, errors.New("volume object identity is incomplete")
	}
	return identity, nil
}

func volumeObjectHashMetadataID(identity VolumeObjectIdentity) string {
	value := strings.Join([]string{
		volumeObjectHashMetadataVersion,
		identity.Scope,
		identity.Endpoint,
		identity.Bucket,
		identity.Path,
		identity.ETag,
		fmt.Sprintf("%d", identity.Size),
	}, "\x00")
	sum := sha256.Sum256([]byte(value))
	return volumeObjectHashMetadataVersion + ":" + hex.EncodeToString(sum[:])
}

func validVolumeContentHash(hash string) bool {
	if len(hash) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func volumeObjectHashMetadataMissing(err error) bool {
	if err == nil {
		return false
	}
	var notFound *ErrNodeNotFound
	if errors.As(err, &notFound) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "cachefs node not found") || strings.Contains(message, "fs node not found")
}

// LookupVolumeObjectContentHash resolves a previously verified content hash.
// The full immutable identity is encoded into the metadata id, so stale object
// versions cannot satisfy a lookup after an in-place replacement.
func (c *Client) LookupVolumeObjectContentHash(ctx context.Context, identity VolumeObjectIdentity) (string, bool, error) {
	if c == nil || c.metadataStore == nil {
		return "", false, errors.New("cache metadata store is unavailable")
	}
	identity, err := normalizeVolumeObjectIdentity(identity)
	if err != nil {
		return "", false, err
	}
	id := volumeObjectHashMetadataID(identity)
	metadata, err := c.metadataStore.GetFsNode(ctx, id)
	if err != nil {
		if volumeObjectHashMetadataMissing(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if metadata == nil || metadata.ID != id || metadata.PID != volumeObjectHashMetadataVersion ||
		metadata.Name != identity.ETag || metadata.Path != identity.Path || metadata.Size != identity.Size ||
		!validVolumeContentHash(metadata.Hash) {
		return "", false, nil
	}
	return metadata.Hash, true, nil
}

// StoreVolumeObjectContentHash durably records a verified immutable
// identity-to-hash mapping. Existing conflicting mappings are rejected instead
// of silently changing the content address for one object identity.
func (c *Client) StoreVolumeObjectContentHash(ctx context.Context, identity VolumeObjectIdentity, hash string) error {
	if c == nil || c.metadataStore == nil {
		return errors.New("cache metadata store is unavailable")
	}
	identity, err := normalizeVolumeObjectIdentity(identity)
	if err != nil {
		return err
	}
	if !validVolumeContentHash(hash) {
		return fmt.Errorf("invalid volume content hash %q", hash)
	}
	id := volumeObjectHashMetadataID(identity)
	existing, err := c.metadataStore.GetFsNode(ctx, id)
	if err == nil && existing != nil {
		if existing.ID != id || existing.PID != volumeObjectHashMetadataVersion ||
			existing.Name != identity.ETag || existing.Path != identity.Path || existing.Size != identity.Size ||
			!validVolumeContentHash(existing.Hash) {
			return errors.New("existing volume object hash metadata is invalid")
		}
		if existing.Hash != hash {
			return fmt.Errorf("conflicting content hash for immutable volume object identity: existing=%s new=%s", existing.Hash, hash)
		}
		return nil
	}
	if err != nil && !volumeObjectHashMetadataMissing(err) {
		return err
	}
	return c.metadataStore.SetFsNode(ctx, id, &FSMetadata{
		ID:   id,
		PID:  volumeObjectHashMetadataVersion,
		Name: identity.ETag,
		Path: identity.Path,
		Hash: hash,
		Size: identity.Size,
	})
}
