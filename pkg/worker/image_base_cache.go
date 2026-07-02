package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
const baseImageCacheSeedTimeout = 15 * time.Minute

type ociIndexFile struct {
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []v1.Descriptor `json:"manifests"`
}

// cachedBaseImageOCIRef restores a base image from distributed content
// cache into a temporary OCI layout when every compressed layer blob is
// already present. It avoids registry blob downloads while still letting
// buildah ingest the image through its normal containers/storage path.
func (c *ImageClient) cachedBaseImageOCIRef(ctx context.Context, outputLogger *slog.Logger, request *types.ContainerRequest, sourceImage, buildPath string) (string, bool, error) {
	if c.cacheClient == nil || sourceImage == "" {
		return "", false, nil
	}

	ref, err := name.ParseReference(sourceImage)
	if err != nil {
		return "", false, err
	}

	platform := targetImagePlatform(request)
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(v1.Platform{OS: platform.OS, Architecture: platform.Architecture}),
	}
	if auth := c.remoteImageAuthenticator(ctx, request, ref); auth != nil {
		remoteOpts = append(remoteOpts, remote.WithAuth(auth))
	} else {
		remoteOpts = append(remoteOpts, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}

	img, err := remote.Image(ref, remoteOpts...)
	if err != nil {
		return "", false, err
	}

	manifest, err := img.Manifest()
	if err != nil {
		return "", false, err
	}

	for _, layer := range manifest.Layers {
		key := strings.TrimPrefix(layer.Digest.String(), "sha256:")
		metadata, err := c.cacheClient.CacheFSMetadata(ctx, imageLayerContentCachePath(key))
		if err != nil || metadata == nil || metadata.Hash == "" || int64(metadata.Size) != layer.Size {
			if err != nil {
				log.Debug().Err(err).Str("layer_digest", layer.Digest.String()).Msg("base image layer cache lookup failed")
			}
			return "", false, nil
		}
	}

	layoutDir := filepath.Join(buildPath, "base-oci")
	if err := os.RemoveAll(layoutDir); err != nil {
		return "", false, err
	}
	if err := os.MkdirAll(filepath.Join(layoutDir, "blobs", "sha256"), 0o755); err != nil {
		return "", false, err
	}

	if err := os.WriteFile(filepath.Join(layoutDir, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o644); err != nil {
		return "", false, err
	}

	rawManifest, err := img.RawManifest()
	if err != nil {
		return "", false, err
	}
	manifestDigest, err := img.Digest()
	if err != nil {
		return "", false, err
	}
	if err := writeOCIBlob(layoutDir, manifestDigest, rawManifest); err != nil {
		return "", false, err
	}

	rawConfig, err := img.RawConfigFile()
	if err != nil {
		return "", false, err
	}
	if err := writeOCIBlob(layoutDir, manifest.Config.Digest, rawConfig); err != nil {
		return "", false, err
	}

	for _, layer := range manifest.Layers {
		if err := c.writeContentCacheBlobToOCI(ctx, layoutDir, layer.Digest, layer.Size); err != nil {
			return "", false, err
		}
	}

	desc, err := partial.Descriptor(img)
	if err != nil {
		return "", false, err
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
		return "", false, err
	}
	if err := os.WriteFile(filepath.Join(layoutDir, "index.json"), indexBytes, 0o644); err != nil {
		return "", false, err
	}

	outputLogger.Info("Restored base image layers from cache\n")
	return fmt.Sprintf("oci:%s:%s", layoutDir, cachedBaseImageTag), true, nil
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

// seedBaseImageBlobsFromRegistry best-effort seeds the original compressed
// source-image layer blobs into the embedded content cache. This runs after a
// normal buildah pull miss; it duplicates the first pull's network I/O, but it
// gives subsequent workers a distributed-cache source for the exact compressed
// blobs buildah needs for the base image.
func (c *ImageClient) seedBaseImageBlobsFromRegistry(request *types.ContainerRequest, sourceImage string) {
	if c.cacheClient == nil || request == nil || sourceImage == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), baseImageCacheSeedTimeout)
	defer cancel()

	ref, err := name.ParseReference(sourceImage)
	if err != nil {
		return
	}

	platform := targetImagePlatform(request)
	remoteOpts := []remote.Option{
		remote.WithContext(ctx),
		remote.WithPlatform(v1.Platform{OS: platform.OS, Architecture: platform.Architecture}),
	}
	if auth := c.remoteImageAuthenticator(ctx, request, ref); auth != nil {
		remoteOpts = append(remoteOpts, remote.WithAuth(auth))
	} else {
		remoteOpts = append(remoteOpts, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}

	img, err := remote.Image(ref, remoteOpts...)
	if err != nil {
		log.Debug().Err(err).Str("source_image", sourceImage).Msg("failed to inspect source image for cache seed")
		return
	}

	layers, err := img.Layers()
	if err != nil {
		return
	}

	for _, layer := range layers {
		digest, err := layer.Digest()
		if err != nil {
			continue
		}
		size, err := layer.Size()
		if err != nil {
			continue
		}
		key := digest.Hex
		cachePath := imageLayerContentCachePath(key)
		if metadata, err := c.cacheClient.CacheFSMetadata(ctx, cachePath); err == nil && metadata != nil && metadata.Hash != "" && int64(metadata.Size) == size {
			continue
		}

		rc, err := layer.Compressed()
		if err != nil {
			log.Debug().Err(err).Str("layer_digest", digest.String()).Msg("failed to read source image layer for cache seed")
			continue
		}

		tmp, err := os.CreateTemp("", "base-layer-*.blob")
		if err != nil {
			rc.Close()
			continue
		}
		tmpPath := tmp.Name()
		_, copyErr := io.Copy(tmp, rc)
		closeErr := tmp.Close()
		rc.Close()
		if copyErr != nil || closeErr != nil {
			_ = os.Remove(tmpPath)
			continue
		}

		_, err = c.cacheClient.StoreContentFromLocalFile(cache.LocalContentSource{
			Path:      tmpPath,
			CachePath: cachePath,
		}, cache.StoreContentOptions{
			RoutingKey: cachePath,
			Lock:       true,
		})
		_ = os.Remove(tmpPath)
		if err != nil {
			log.Debug().Err(err).Str("layer_digest", digest.String()).Msg("failed to seed source image layer cache")
			continue
		}
		log.Debug().Str("layer_digest", digest.String()).Int64("bytes", size).Msg("seeded source image layer in embedded cache")
	}
}
