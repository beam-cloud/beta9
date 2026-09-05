package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"log/slog"

	"github.com/beam-cloud/beta9/pkg/cache"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	ggcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const cachedBaseImageTag = "cached"

type ociIndexFile struct {
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []v1.Descriptor `json:"manifests"`
}

// cachedLayer is one layer of a base image as the content cache holds it:
// either the compressed blob a registry serves (hash = layer digest) or the
// plain tar the indexer seeds after inflating it (hash = diffID).
type cachedLayer struct {
	desc       v1.Descriptor // as in the registry manifest
	hash       string        // content cache hash to read
	size       int64         // bytes at that hash
	compressed bool
}

// baseImage resolves sourceImage in the registry and looks each layer up in
// the content cache, returning the cached layers in manifest order and the
// descriptors of those the cache lacks. A layer counts as cached when its
// compressed blob or its decompressed tar is there; every image the worker
// indexes seeds the latter, so an image built here is restorable without a
// registry pull as soon as it is published.
func (c *ImageClient) baseImage(ctx context.Context, request *types.ContainerRequest, sourceImage string) (v1.Image, []cachedLayer, []v1.Descriptor, error) {
	ref, err := name.ParseReference(sourceImage)
	if err != nil {
		return nil, nil, nil, err
	}

	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(v1.Platform{OS: "linux", Architecture: runtime.GOARCH}),
	}
	if auth := c.remoteImageAuthenticator(ctx, request, ref); auth != nil {
		remoteOpts = append(remoteOpts, remote.WithAuth(auth))
	} else {
		remoteOpts = append(remoteOpts, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}

	img, err := remote.Image(ref, remoteOpts...)
	if err != nil {
		return nil, nil, nil, err
	}
	manifest, err := img.Manifest()
	if err != nil {
		return nil, nil, nil, err
	}
	config, err := img.ConfigFile()
	if err != nil {
		return nil, nil, nil, err
	}

	lookup := func(hash string) (int64, bool) {
		metadata, err := c.cacheClient.CacheFSMetadata(ctx, imageLayerContentCachePath(hash))
		if err != nil || metadata == nil || metadata.Hash == "" || metadata.Size == 0 {
			if err != nil {
				log.Debug().Err(err).Str("hash", hash).Msg("base image layer cache lookup failed")
			}
			return 0, false
		}
		return int64(metadata.Size), true
	}

	var cached []cachedLayer
	var missing []v1.Descriptor
	for i, layer := range manifest.Layers {
		// The layout written from the cache files blobs under sha256/, so a
		// layer addressed by another algorithm is only usable by pulling it.
		if layer.Digest.Algorithm != "sha256" {
			missing = append(missing, layer)
			continue
		}
		if size, ok := lookup(layer.Digest.Hex); ok && size == layer.Size {
			cached = append(cached, cachedLayer{desc: layer, hash: layer.Digest.Hex, size: size, compressed: true})
			continue
		}
		if i < len(config.RootFS.DiffIDs) && config.RootFS.DiffIDs[i].Algorithm == "sha256" {
			diffID := config.RootFS.DiffIDs[i]
			if size, ok := lookup(diffID.Hex); ok {
				cached = append(cached, cachedLayer{desc: layer, hash: diffID.Hex, size: size})
				continue
			}
		}
		missing = append(missing, layer)
	}
	return img, cached, missing, nil
}

// uncompressedMediaType maps a layer media type to its uncompressed form.
func uncompressedMediaType(mediaType ggcrtypes.MediaType) ggcrtypes.MediaType {
	switch mediaType {
	case ggcrtypes.OCILayer, ggcrtypes.OCIRestrictedLayer, ggcrtypes.OCILayerZStd, ggcrtypes.OCIUncompressedLayer:
		return ggcrtypes.OCIUncompressedLayer
	default:
		return ggcrtypes.DockerUncompressedLayer
	}
}

// warmBaseImageLayers downloads the given layers of img and stores them in
// the content cache, so the next build restores the base image from cache
// instead of pulling it. Meant to run alongside the build, after buildah has
// finished its own pull of the same bytes.
func (c *ImageClient) warmBaseImageLayers(ctx context.Context, request *types.ContainerRequest, img v1.Image, missing []v1.Descriptor, dir string) {
	contentCache := newImageContentCache(c.cacheClient, request.ImageId, "base-image-layer", nil)
	if contentCache == nil || len(missing) == 0 {
		return
	}
	layers, err := img.Layers()
	if err != nil {
		return
	}
	byDigest := map[v1.Hash]v1.Layer{}
	for _, layer := range layers {
		if digest, err := layer.Digest(); err == nil {
			byDigest[digest] = layer
		}
	}
	started := time.Now()
	var bytes int64
	for _, desc := range missing {
		layer, ok := byDigest[desc.Digest]
		if !ok || ctx.Err() != nil || desc.Digest.Algorithm != "sha256" {
			continue
		}
		path := filepath.Join(dir, desc.Digest.Hex)
		if err := downloadLayerBlob(layer, desc, path); err != nil {
			log.Warn().Err(err).Str("layer_digest", desc.Digest.String()).Msg("base image layer not warmed")
			os.Remove(path)
			continue
		}
		if _, err := contentCache.StoreContentFromLocalPath(path, desc.Digest.Hex, struct{ RoutingKey string }{RoutingKey: desc.Digest.Hex}); err != nil {
			log.Warn().Err(err).Str("layer_digest", desc.Digest.String()).Msg("base image layer not stored in content cache")
		} else {
			bytes += desc.Size
		}
		os.Remove(path)
	}
	log.Info().Int("layers", len(missing)).Int64("bytes", bytes).Dur("duration", time.Since(started)).Msg("warmed base image layers into content cache")
}

// downloadLayerBlob writes the layer's compressed bytes to path and verifies
// them against the descriptor.
func downloadLayerBlob(layer v1.Layer, desc v1.Descriptor, path string) error {
	rc, err := layer.Compressed()
	if err != nil {
		return err
	}
	defer rc.Close()
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()
	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, hasher), rc)
	if err != nil {
		return err
	}
	if n != desc.Size || hex.EncodeToString(hasher.Sum(nil)) != desc.Digest.Hex {
		return fmt.Errorf("layer %s: got %d bytes, digest mismatch or short read", desc.Digest, n)
	}
	return out.Sync()
}

// cachedBaseImageOCIRef restores a base image from distributed content
// cache into a temporary OCI layout when every compressed layer blob is
// already present. It avoids registry blob downloads while still letting
// buildah ingest the image through its normal containers/storage path.
// When layers are missing it returns them, so the caller can warm the
// cache for the next build.
func (c *ImageClient) cachedBaseImageOCIRef(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest, sourceImage, buildPath string) (ref string, cached bool, img v1.Image, missing []v1.Descriptor, err error) {
	if c.cacheClient == nil || sourceImage == "" {
		return "", false, nil, nil, nil
	}
	var layers []cachedLayer
	img, layers, missing, err = c.baseImage(ctx, request, sourceImage)
	if err != nil {
		return "", false, nil, nil, err
	}
	if len(missing) > 0 {
		log.Info().Str("source_image", sourceImage).Int("cached_layers", len(layers)).Int("missing_layers", len(missing)).Msg("base image not fully in content cache, pulling")
		return "", false, img, missing, nil
	}
	manifest, err := img.Manifest()
	if err != nil {
		return "", false, nil, nil, err
	}

	layoutDir := filepath.Join(buildPath, "base-oci")
	if err := os.RemoveAll(layoutDir); err != nil {
		return "", false, nil, nil, err
	}
	if err := os.MkdirAll(filepath.Join(layoutDir, "blobs", "sha256"), 0o755); err != nil {
		return "", false, nil, nil, err
	}

	if err := os.WriteFile(filepath.Join(layoutDir, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o644); err != nil {
		return "", false, nil, nil, err
	}

	// Layers restored from their decompressed tar are described as such:
	// the manifest is the registry's with those descriptors rewritten
	// (uncompressed media type, digest = diffID, size = tar size). The
	// config, and so the image identity buildah records, is unchanged.
	manifest = manifest.DeepCopy()
	rewritten := false
	for i, layer := range layers {
		if layer.compressed {
			continue
		}
		manifest.Layers[i].MediaType = uncompressedMediaType(layer.desc.MediaType)
		manifest.Layers[i].Digest = v1.Hash{Algorithm: "sha256", Hex: layer.hash}
		manifest.Layers[i].Size = layer.size
		rewritten = true
	}
	rawManifest, err := img.RawManifest()
	if err != nil {
		return "", false, nil, nil, err
	}
	if rewritten {
		if rawManifest, err = json.Marshal(manifest); err != nil {
			return "", false, nil, nil, err
		}
	}
	manifestDigest, _, err := v1.SHA256(bytes.NewReader(rawManifest))
	if err != nil {
		return "", false, nil, nil, err
	}
	if err := writeOCIBlob(layoutDir, manifestDigest, rawManifest); err != nil {
		return "", false, nil, nil, err
	}

	rawConfig, err := img.RawConfigFile()
	if err != nil {
		return "", false, nil, nil, err
	}
	if err := writeOCIBlob(layoutDir, manifest.Config.Digest, rawConfig); err != nil {
		return "", false, nil, nil, err
	}

	started := time.Now()
	var restored int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(imageLayerPrepareConcurrency)
	for _, layer := range layers {
		layer := layer
		restored += layer.size
		g.Go(func() error {
			return c.writeContentCacheBlobToOCI(gctx, layoutDir, layer.hash, layer.size)
		})
	}
	if err := g.Wait(); err != nil {
		return "", false, nil, nil, err
	}

	desc, err := partial.Descriptor(img)
	if err != nil {
		return "", false, nil, nil, err
	}
	desc.Digest = manifestDigest
	desc.Size = int64(len(rawManifest))
	if desc.Annotations == nil {
		desc.Annotations = map[string]string{}
	}
	desc.Annotations["org.opencontainers.image.ref.name"] = cachedBaseImageTag

	indexBytes, err := json.Marshal(ociIndexFile{
		SchemaVersion: 2,
		Manifests:     []v1.Descriptor{*desc},
	})
	if err != nil {
		return "", false, nil, nil, err
	}
	if err := os.WriteFile(filepath.Join(layoutDir, "index.json"), indexBytes, 0o644); err != nil {
		return "", false, nil, nil, err
	}

	log.Info().Str("source_image", sourceImage).Int("layers", len(layers)).Int64("bytes", restored).Bool("decompressed", rewritten).
		Dur("duration", time.Since(started)).Msg("restored base image from content cache")
	outputLogger.Info("Restored base image layers from cache\n")
	return fmt.Sprintf("oci:%s:%s", layoutDir, cachedBaseImageTag), true, img, nil, nil
}

func writeOCIBlob(layoutDir string, digest v1.Hash, data []byte) error {
	if digest.Algorithm != "sha256" {
		return fmt.Errorf("unsupported OCI blob digest algorithm: %s", digest.Algorithm)
	}
	return os.WriteFile(filepath.Join(layoutDir, "blobs", "sha256", digest.Hex), data, 0o644)
}

// writeContentCacheBlobToOCI copies the content cache entry at hash into the
// layout as blob sha256:hash.
func (c *ImageClient) writeContentCacheBlobToOCI(ctx context.Context, layoutDir string, hash string, size int64) error {
	out, err := os.Create(filepath.Join(layoutDir, "blobs", "sha256", hash))
	if err != nil {
		return err
	}
	defer out.Close()

	const chunkSize = 4 * 1024 * 1024
	buf := make([]byte, chunkSize)
	var offset int64
	metadata, err := c.cacheClient.CacheFSMetadata(ctx, imageLayerContentCachePath(hash))
	if err != nil || metadata == nil || metadata.Hash == "" {
		return fmt.Errorf("cached layer metadata missing for %s: %w", hash, err)
	}

	for offset < size {
		length := min(int64(chunkSize), size-offset)
		read, err := c.cacheClient.ReadContentInto(ctx, metadata.Hash, offset, buf[:length], cache.ClientOptions{RoutingKey: imageLayerContentCachePath(hash)})
		if err != nil {
			return err
		}
		if read != length {
			return fmt.Errorf("short cached base layer read for %s: expected %d bytes, got %d", hash, length, read)
		}
		if _, err := out.Write(buf[:read]); err != nil {
			return err
		}
		offset += read
	}
	return nil
}

func (c *ImageClient) remoteImageAuthenticator(ctx context.Context, request *types.ContainerRequest, ref name.Reference) authn.Authenticator {
	registryHost := ref.Context().RegistryStr()
	if !c.brokeredImageAccessRequest(request) && request.BuildOptions.SourceImageCreds != "" {
		if provider := c.parseAndCreateProvider(ctx, request.BuildOptions.SourceImageCreds, registryHost, request.ImageId, "source image"); provider != nil {
			if cfg, err := provider.GetCredentials(ctx, registryHost, ref.Context().RepositoryStr()); err == nil && cfg != nil {
				return authn.FromConfig(*cfg)
			}
		}
	}

	creds := c.gatewayRegistryCredentials(ctx, registryHost, request)
	if creds == "" {
		if c.brokeredImageAccessRequest(request) {
			return authn.Anonymous
		}
		return nil
	}
	parsed, err := reg.ParseCredentialsFromJSON(creds)
	if err != nil || len(parsed) == 0 {
		parts := strings.SplitN(creds, ":", 2)
		if len(parts) == 2 {
			parsed = map[string]string{"USERNAME": parts[0], "PASSWORD": parts[1]}
		}
	}
	provider := reg.CredentialsToProvider(ctx, registryHost, parsed)
	if provider == nil {
		return nil
	}
	cfg, err := provider.GetCredentials(ctx, registryHost, ref.Context().RepositoryStr())
	if err != nil || cfg == nil {
		return nil
	}
	return authn.FromConfig(*cfg)
}
