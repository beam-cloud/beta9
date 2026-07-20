package cache

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVolumeObjectContentHashRegistrySurvivesQuotedETagForms(t *testing.T) {
	metadata := NewMockCacheMetadataStore()
	client := &Client{metadataStore: metadata}
	identity := VolumeObjectIdentity{
		Scope:    "workspace-1",
		Endpoint: "https://storage.example.test/",
		Bucket:   "models",
		Path:     "volumes/volume-1/model.safetensors",
		ETag:     `"multipart-etag-12"`,
		Size:     3_087_467_144,
	}
	hash := strings.Repeat("a", 64)

	require.NoError(t, client.StoreVolumeObjectContentHash(context.Background(), identity, hash))

	identity.Endpoint = "https://storage.example.test"
	identity.ETag = "multipart-etag-12"
	got, found, err := client.LookupVolumeObjectContentHash(context.Background(), identity)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hash, got)
}

func TestVolumeObjectContentHashRegistryRejectsReplacementAndConflict(t *testing.T) {
	metadata := NewMockCacheMetadataStore()
	client := &Client{metadataStore: metadata}
	identity := VolumeObjectIdentity{
		Scope:    "workspace-1",
		Endpoint: "https://storage.example.test",
		Bucket:   "models",
		Path:     "volumes/volume-1/model.safetensors",
		ETag:     "etag-v1",
		Size:     1024,
	}
	hash := strings.Repeat("b", 64)
	require.NoError(t, client.StoreVolumeObjectContentHash(context.Background(), identity, hash))

	require.Error(t, client.StoreVolumeObjectContentHash(context.Background(), identity, strings.Repeat("c", 64)))

	replacement := identity
	replacement.ETag = "etag-v2"
	got, found, err := client.LookupVolumeObjectContentHash(context.Background(), replacement)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, got)

	otherWorkspace := identity
	otherWorkspace.Scope = "workspace-2"
	got, found, err = client.LookupVolumeObjectContentHash(context.Background(), otherWorkspace)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, got)
}

func TestVolumeObjectContentHashRegistryValidatesIdentityAndHash(t *testing.T) {
	client := &Client{metadataStore: NewMockCacheMetadataStore()}
	identity := VolumeObjectIdentity{Scope: "workspace", Bucket: "bucket", Path: "path", ETag: "etag", Size: 1}

	require.Error(t, client.StoreVolumeObjectContentHash(context.Background(), identity, "not-a-sha256"))
	identity.Scope = ""
	require.Error(t, client.StoreVolumeObjectContentHash(context.Background(), identity, strings.Repeat("d", 64)))
}
