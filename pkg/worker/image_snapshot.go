package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/rs/zerolog/log"
)

// errNoLayeredBase means the sandbox's image has no OCI reference to stack a
// layer on, so a snapshot has to archive the merged root filesystem instead.
var errNoLayeredBase = errors.New("image has no OCI base reference")

// ArchiveLayer publishes a running sandbox's filesystem as a new image: the
// overlay upper directory becomes one OCI layer appended to the image the
// sandbox started from. The base layers are already in a registry, so the
// push uploads only the delta, the indexer skips the layers it has indexed
// before, and the new layer's content is seeded into the content cache while
// it is indexed. Archiving the merged root shipped the whole image again for
// every snapshot.
func (c *ImageClient) ArchiveLayer(ctx context.Context, request *types.ContainerRequest, upperDir, imageId string, progress chan<- int) error {
	baseRef, ok := c.v2ImageRefs.Get(request.ImageId)
	if !ok || baseRef == "" {
		return errNoLayeredBase
	}
	report := func(pct int) {
		if progress != nil {
			select {
			case progress <- pct:
			default:
			}
		}
	}

	tmpdir, err := os.MkdirTemp("", "snapshot-"+imageId+"-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	base, _, err := c.remoteBaseImage(ctx, request, baseRef)
	if err != nil {
		return fmt.Errorf("fetch base image %s: %w", baseRef, err)
	}
	started := time.Now()
	layers, err := packOverlayLayers(upperDir, filepath.Join(tmpdir, "layers"), layerMediaTypeFor(base))
	if err != nil {
		return fmt.Errorf("pack snapshot layers: %w", err)
	}
	packed := time.Since(started)
	report(20)

	img, err := appendLayers(base, layers, "beta9 sandbox filesystem snapshot")
	if err != nil {
		return err
	}

	result, err := c.publishLayeredImageWithProgress(ctx, request, img, layers, imageId, tmpdir, report)
	if err != nil {
		return err
	}
	report(100)
	compressed, content, _ := layerStats(layers)
	log.Info().Str("image_id", imageId).Str("base_ref", baseRef).Str("image_tag", result.imageTag).
		Int("layers", len(layers)).Int64("layer_bytes", compressed).Int64("content_bytes", content).
		Dur("pack", packed).Dur("push", result.pushed).Dur("index", result.indexed).
		Msg("published filesystem snapshot as image layers")
	return nil
}

type publishResult struct {
	imageTag                 string
	pushed, indexed, elapsed time.Duration
}

// publishLayeredImage makes img (a base plus freshly packed layers) available
// as imageId: it is pushed to the build registry while the clip index is
// built from a local layout, then the index archive is uploaded. The index
// reads only the manifest, the config and the new layers from the layout;
// the base layers come from the layer index cache. Should that cache miss,
// the layout read fails and the image is indexed from the registry once the
// push has landed. workDir holds the layout and the index archive.
func (c *ImageClient) publishLayeredImage(ctx context.Context, request *types.ContainerRequest, img v1.Image, layers []*packedLayer, imageId, workDir string) (*publishResult, error) {
	return c.publishLayeredImageWithProgress(ctx, request, img, layers, imageId, workDir, nil)
}

// publishLayeredImageWithProgress is publishLayeredImage reporting, through
// progress (which may be nil), a percentage as the stages complete: the layers
// are already packed when it is called (20%), the index being built brings it
// to 60%, the push landing to 90%, and the final archive upload is what is
// left before the caller's 100%.
func (c *ImageClient) publishLayeredImageWithProgress(ctx context.Context, request *types.ContainerRequest, img v1.Image, layers []*packedLayer, imageId, workDir string, progress func(int)) (*publishResult, error) {
	report := func(pct int) {
		if progress != nil {
			progress(pct)
		}
	}
	buildRegistry := c.getBuildRegistry()
	imageTag := fmt.Sprintf("%s/%s:%s", buildRegistry, c.config.ImageService.BuildRepositoryName, imageId)
	targetRef, err := name.ParseReference(imageTag, c.buildRegistryNameOptions()...)
	if err != nil {
		return nil, err
	}
	pushOpts := []remote.Option{remote.WithContext(ctx), remote.WithJobs(layerSplitMaxLayers)}
	if auth := c.buildRegistryAuthenticator(ctx, request, buildRegistry); auth != nil {
		pushOpts = append(pushOpts, remote.WithAuth(auth))
	} else {
		pushOpts = append(pushOpts, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}
	// The indexer resolves credentials and the content-cache routing key from
	// the image id, so the new image has to be known before it is indexed.
	// Until the push has landed the tag does not exist, so the mapping is
	// withdrawn if publishing fails; otherwise a later ArchiveLayer of a
	// sandbox running this image would stack on a base nobody can fetch.
	c.v2ImageRefs.Set(imageId, imageTag)
	published := false
	defer func() {
		if !published {
			c.v2ImageRefs.Delete(imageId)
		}
	}()
	indexRequest := *request
	indexRequest.ImageId = imageId
	archivePath := c.archiveScratchPath(workDir, imageId)

	layoutDir := filepath.Join(workDir, "layout")
	if err := writeSparseOCILayout(layoutDir, img, layers); err != nil {
		return nil, err
	}
	started := time.Now()
	result := &publishResult{imageTag: imageTag}
	pushErr := make(chan error, 1)
	go func() {
		err := remote.Write(targetRef, img, pushOpts...)
		result.pushed = time.Since(started)
		pushErr <- err
	}()
	indexErr := c.indexImage(ctx, &indexRequest, imageTag, layoutDir, archivePath)
	result.indexed = time.Since(started)
	if indexErr == nil {
		report(60)
	}
	if err := <-pushErr; err != nil {
		return nil, fmt.Errorf("push image %s: %w", imageTag, err)
	}
	published = true
	if indexErr != nil {
		log.Warn().Err(indexErr).Str("image_id", imageId).Msg("index from local layout failed, indexing from registry")
		if indexErr = c.indexImage(ctx, &indexRequest, imageTag, "", archivePath); indexErr != nil {
			return nil, fmt.Errorf("index image: %w", indexErr)
		}
		result.indexed = time.Since(started)
	}
	report(90)
	if err := c.registry.Push(ctx, archivePath, imageId); err != nil {
		return nil, fmt.Errorf("publish index archive: %w", err)
	}
	result.elapsed = time.Since(started)
	return result, nil
}

// appendLayers stacks packed layers on base, each with a history entry.
func appendLayers(base v1.Image, layers []*packedLayer, createdBy string) (v1.Image, error) {
	adds := make([]mutate.Addendum, 0, len(layers))
	for _, packed := range layers {
		layer, err := packed.Layer()
		if err != nil {
			return nil, err
		}
		adds = append(adds, mutate.Addendum{
			Layer:   layer,
			History: v1.History{Created: v1.Time{Time: time.Now()}, CreatedBy: createdBy},
		})
	}
	return mutate.Append(base, adds...)
}

// indexImage writes the clip index archive for imageTag, reading the image
// from layoutDir when given and from the registry otherwise.
func (c *ImageClient) indexImage(ctx context.Context, request *types.ContainerRequest, imageTag, layoutDir, archivePath string) error {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	return c.createOCIImageWithProgress(ctx, quiet, request, imageTag, layoutDir, archivePath, 2)
}

func (c *ImageClient) buildRegistryNameOptions() []name.Option {
	if c.config.ImageService.BuildRegistryInsecure {
		return []name.Option{name.Insecure}
	}
	return nil
}

// buildRegistryAuthenticator mirrors the credentials the image builder pushes
// with: the request's build registry credentials, else gateway-vended ones.
func (c *ImageClient) buildRegistryAuthenticator(ctx context.Context, request *types.ContainerRequest, buildRegistry string) authn.Authenticator {
	if buildRegistry == "localhost" || strings.HasPrefix(buildRegistry, "127.0.0.1") {
		return authn.Anonymous
	}
	creds := request.BuildRegistryCredentials
	if creds == "" {
		creds = c.gatewayRegistryCredentials(ctx, buildRegistry, request)
	}
	user, pass, ok := strings.Cut(creds, ":")
	if !ok {
		return nil
	}
	return &authn.Basic{Username: user, Password: pass}
}

// parseImageReference parses ref, permitting plain HTTP only when it points
// at the build registry and that registry is configured insecure. A sandbox
// may run on an image from an external registry; that registry keeps the
// default (TLS) transport regardless of how the build registry is reached.
func (c *ImageClient) parseImageReference(ref string) (name.Reference, error) {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return nil, err
	}
	if !c.config.ImageService.BuildRegistryInsecure {
		return parsed, nil
	}
	buildRegistry, err := name.NewRegistry(c.getBuildRegistry())
	if err != nil || buildRegistry.RegistryStr() != parsed.Context().RegistryStr() {
		return parsed, nil
	}
	return name.ParseReference(ref, name.Insecure)
}

// remoteBaseImage fetches the image the sandbox runs on, for this platform.
func (c *ImageClient) remoteBaseImage(ctx context.Context, request *types.ContainerRequest, baseRef string) (v1.Image, []remote.Option, error) {
	ref, err := c.parseImageReference(baseRef)
	if err != nil {
		return nil, nil, err
	}
	opts := []remote.Option{remote.WithContext(ctx), remote.WithPlatform(v1.Platform{OS: "linux", Architecture: runtime.GOARCH})}
	if auth := c.remoteImageAuthenticator(ctx, request, ref); auth != nil {
		opts = append(opts, remote.WithAuth(auth))
	} else {
		opts = append(opts, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	}
	img, err := remote.Image(ref, opts...)
	return img, opts, err
}
