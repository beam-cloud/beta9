package worker

import (
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
	"github.com/rs/zerolog/log"
)

const cachedBaseImageTag = "cached"

type ociIndexFile struct {
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []v1.Descriptor `json:"manifests"`
}

// baseImage resolves sourceImage in the registry and reports which of its
// compressed layers the content cache is missing.
func (c *ImageClient) baseImage(ctx context.Context, request *types.ContainerRequest, sourceImage string) (v1.Image, []v1.Descriptor, error) {
	ref, err := name.ParseReference(sourceImage)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	manifest, err := img.Manifest()
	if err != nil {
		return nil, nil, err
	}

	var missing []v1.Descriptor
	for _, layer := range manifest.Layers {
		key := strings.TrimPrefix(layer.Digest.String(), "sha256:")
		metadata, err := c.cacheClient.CacheFSMetadata(ctx, imageLayerContentCachePath(key))
		if err != nil || metadata == nil || metadata.Hash == "" || int64(metadata.Size) != layer.Size {
			if err != nil {
				log.Debug().Err(err).Str("layer_digest", layer.Digest.String()).Msg("base image layer cache lookup failed")
			}
			missing = append(missing, layer)
		}
	}
	return img, missing, nil
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
		if !ok || ctx.Err() != nil {
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
	img, missing, err = c.baseImage(ctx, request, sourceImage)
	if err != nil {
		return "", false, nil, nil, err
	}
	if len(missing) > 0 {
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

	rawManifest, err := img.RawManifest()
	if err != nil {
		return "", false, nil, nil, err
	}
	manifestDigest, err := img.Digest()
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

	for _, layer := range manifest.Layers {
		if err := c.writeContentCacheBlobToOCI(ctx, layoutDir, layer.Digest, layer.Size); err != nil {
			return "", false, nil, nil, err
		}
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

	outputLogger.Info("Restored base image layers from cache\n")
	return fmt.Sprintf("oci:%s:%s", layoutDir, cachedBaseImageTag), true, img, nil, nil
}

func writeOCIBlob(layoutDir string, digest v1.Hash, data []byte) error {
	if digest.Algorithm != "sha256" {
		return fmt.Errorf("unsupported OCI blob digest algorithm: %s", digest.Algorithm)
	}
	return os.WriteFile(filepath.Join(layoutDir, "blobs", "sha256", digest.Hex), data, 0o644)
}

func (c *ImageClient) writeContentCacheBlobToOCI(ctx context.Context, layoutDir string, digest v1.Hash, size int64) error {
	if digest.Algorithm != "sha256" {
		return fmt.Errorf("unsupported OCI layer digest algorithm: %s", digest.Algorithm)
	}

	out, err := os.Create(filepath.Join(layoutDir, "blobs", "sha256", digest.Hex))
	if err != nil {
		return err
	}
	defer out.Close()

	const chunkSize = 4 * 1024 * 1024
	buf := make([]byte, chunkSize)
	var offset int64
	metadata, err := c.cacheClient.CacheFSMetadata(ctx, imageLayerContentCachePath(digest.Hex))
	if err != nil || metadata == nil || metadata.Hash == "" {
		return fmt.Errorf("cached layer metadata missing for %s: %w", digest.String(), err)
	}

	for offset < size {
		length := min(int64(chunkSize), size-offset)
		read, err := c.cacheClient.ReadContentInto(ctx, metadata.Hash, offset, buf[:length], cache.ClientOptions{RoutingKey: imageLayerContentCachePath(digest.Hex)})
		if err != nil {
			return err
		}
		if read != length {
			return fmt.Errorf("short cached base layer read for %s: expected %d bytes, got %d", digest.String(), length, read)
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
