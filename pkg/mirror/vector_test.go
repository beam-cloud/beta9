package mirror

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The manifest vector is shared with the Rust client: a tree with explicit
// extents and the canonical bytes every writer must encode it to. Run with
// UPDATE_MIRROR_VECTORS=1 to regenerate it after a deliberate format change.
type manifestVector struct {
	Description string        `json:"description"`
	Meta        string        `json:"meta"`
	Root        vectorEntry   `json:"root"`
	Entries     []vectorEntry `json:"entries"`
	// The body, stored uncompressed (envelope codec 0).
	RawManifest       string `json:"raw_manifest_hex"`
	RawManifestSHA256 string `json:"raw_manifest_sha256"`
}

type vectorEntry struct {
	Path    string         `json:"path,omitempty"`
	Kind    string         `json:"kind"`
	Mode    uint32         `json:"mode"`
	MtimeNs int64          `json:"mtime_ns"`
	Target  string         `json:"target,omitempty"`
	Extents []vectorExtent `json:"extents,omitempty"`
}

type vectorExtent struct {
	Object     string `json:"object"`
	ObjectSize uint64 `json:"object_size"`
	Offset     uint32 `json:"offset"`
	StoredLen  uint32 `json:"stored_len"`
	RawLen     uint32 `json:"raw_len"`
}

const manifestVectorPath = "testdata/manifest-vector.json"

func TestManifestMatchesSharedVector(t *testing.T) {
	data, err := os.ReadFile(manifestVectorPath)
	require.NoError(t, err)
	var vector manifestVector
	require.NoError(t, json.Unmarshal(data, &vector))

	builder := NewBuilder()
	builder.SetRoot(vector.Root.Mode, vector.Root.MtimeNs)
	// Entries are listed parents first but otherwise out of order: the
	// encoding must not depend on the order they were added in.
	for _, entry := range vector.Entries {
		switch entry.Kind {
		case "dir":
			require.NoError(t, builder.AddDir(entry.Path, entry.Mode, entry.MtimeNs))
		case "symlink":
			require.NoError(t, builder.AddSymlink(entry.Path, entry.Target, entry.MtimeNs))
		case "file":
			extents := make([]FileExtent, 0, len(entry.Extents))
			for _, extent := range entry.Extents {
				digest, err := ParseDigest(extent.Object)
				require.NoError(t, err)
				extents = append(extents, FileExtent{Object: Object{Digest: digest, Size: extent.ObjectSize}, Offset: extent.Offset, StoredLen: extent.StoredLen, RawLen: extent.RawLen})
			}
			require.NoError(t, builder.AddFile(entry.Path, entry.Mode, entry.MtimeNs, extents))
		default:
			t.Fatalf("unknown kind %q", entry.Kind)
		}
	}
	raw, manifest, err := builder.Encode([]byte(vector.Meta), true)
	require.NoError(t, err)

	if os.Getenv("UPDATE_MIRROR_VECTORS") != "" {
		vector.RawManifest = hex.EncodeToString(raw)
		vector.RawManifestSHA256 = manifest.Digest().String()
		updated, err := json.MarshalIndent(vector, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(manifestVectorPath, append(updated, '\n'), 0o644))
	}
	require.Equal(t, vector.RawManifest, hex.EncodeToString(raw))
	require.Equal(t, vector.RawManifestSHA256, manifest.Digest().String())

	// The compressed envelope holds the same body. Its bytes depend on the
	// zstd implementation, so only the decoded form is comparable.
	compressed, decoded, err := builder.Encode([]byte(vector.Meta), false)
	require.NoError(t, err)
	require.Less(t, len(compressed), len(raw))
	require.Equal(t, raw[envelopeSize:], decoded.body)
	reparsed, err := Parse(raw)
	require.NoError(t, err)
	require.Equal(t, decoded.body, reparsed.body)

	index, ok := reparsed.Find("projects/app/src/main.rs")
	require.True(t, ok)
	require.Equal(t, uint64(5000), reparsed.Entry(index).Size)
	index, ok = reparsed.Find("projects/current")
	require.True(t, ok)
	require.Equal(t, "app/src", string(reparsed.Target(index)))
}
