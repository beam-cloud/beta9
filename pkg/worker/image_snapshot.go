package worker

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	ggcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
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

	started := time.Now()
	layerPath := filepath.Join(tmpdir, "layer.tar.gz")
	tarPath := filepath.Join(tmpdir, "layer.tar")
	packedLayer, err := writeOverlayLayer(upperDir, layerPath, tarPath)
	if err != nil {
		return fmt.Errorf("pack snapshot layer: %w", err)
	}
	layer, err := tarball.LayerFromFile(layerPath)
	if err != nil {
		return err
	}
	packed := time.Since(started)
	report(20)

	base, baseOpts, err := c.remoteBaseImage(ctx, request, baseRef)
	if err != nil {
		return fmt.Errorf("fetch base image %s: %w", baseRef, err)
	}
	mediaType, _ := base.MediaType()
	if mediaType == ggcrtypes.OCIManifestSchema1 {
		if layer, err = tarball.LayerFromFile(layerPath, tarball.WithMediaType(ggcrtypes.OCILayer)); err != nil {
			return err
		}
	}
	img, err := mutate.Append(base, mutate.Addendum{
		Layer:   layer,
		History: v1.History{Created: v1.Time{Time: time.Now()}, CreatedBy: "beta9 sandbox filesystem snapshot"},
	})
	if err != nil {
		return err
	}

	buildRegistry := c.getBuildRegistry()
	imageTag := fmt.Sprintf("%s/%s:%s", buildRegistry, c.config.ImageService.BuildRepositoryName, imageId)
	targetRef, err := name.ParseReference(imageTag, c.buildRegistryNameOptions()...)
	if err != nil {
		return err
	}
	pushOpts := []remote.Option{remote.WithContext(ctx)}
	if auth := c.buildRegistryAuthenticator(ctx, request, buildRegistry); auth != nil {
		pushOpts = append(pushOpts, remote.WithAuth(auth))
	} else {
		pushOpts = append(pushOpts, baseOpts...)
	}
	started = time.Now()
	if err := remote.Write(targetRef, img, pushOpts...); err != nil {
		return fmt.Errorf("push snapshot image %s: %w", imageTag, err)
	}
	pushed := time.Since(started)
	report(60)

	// The indexer resolves credentials and the content-cache routing key from
	// the image id, so the new image has to be known before it is indexed.
	c.v2ImageRefs.Set(imageId, imageTag)
	snapshotRequest := *request
	snapshotRequest.ImageId = imageId
	archivePath := filepath.Join(tmpdir, fmt.Sprintf("%s.%s", imageId, c.registry.ImageFileExtension))
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	started = time.Now()
	if err := c.createOCIImageWithProgress(ctx, quiet, &snapshotRequest, imageTag, "", archivePath, 2); err != nil {
		return fmt.Errorf("index snapshot image: %w", err)
	}
	indexed := time.Since(started)
	report(90)

	if err := c.registry.Push(ctx, archivePath, imageId); err != nil {
		return fmt.Errorf("publish snapshot archive: %w", err)
	}
	// The runtime serves the layer page-wise from the content cache and only
	// materializes a layer it cannot find there. The decompressed tar is on
	// hand, so store it now and the first restore reads locally instead of
	// pulling the layer back from the registry.
	c.seedSnapshotLayer(ctx, &snapshotRequest, archivePath, layer, tarPath, packedLayer.tarHash)
	report(100)
	log.Info().Str("image_id", imageId).Str("base_ref", baseRef).Str("image_tag", imageTag).
		Int64("layer_bytes", packedLayer.compressedSize).Dur("pack", packed).Dur("push", pushed).Dur("index", indexed).
		Msg("published filesystem snapshot as image layer")
	return nil
}

// seedSnapshotLayer stores the layer's decompressed bytes in the content cache
// under the hash the index recorded for it; a mismatch means the index saw a
// different byte stream, and the runtime materializes the layer as usual.
func (c *ImageClient) seedSnapshotLayer(ctx context.Context, request *types.ContainerRequest, archivePath string, layer v1.Layer, tarPath, tarHash string) {
	contentCache := newImageContentCache(c.cacheClient, request.ImageId, "oci-layer-snapshot", nil)
	if contentCache == nil {
		return
	}
	digest, err := layer.Digest()
	if err != nil {
		return
	}
	meta, err := c.processPulledArchive(archivePath, request.ImageId)
	if err != nil {
		log.Warn().Err(err).Str("image_id", request.ImageId).Msg("snapshot layer not seeded: archive metadata unreadable")
		return
	}
	info, ok := ociStorageInfo(meta)
	if !ok || info.DecompressedHashByLayer[digest.String()] != tarHash {
		log.Warn().Str("image_id", request.ImageId).Str("layer", digest.String()).Msg("snapshot layer not seeded: index hash differs from packed tar")
		return
	}
	if _, err := contentCache.StoreContentFromLocalPath(tarPath, tarHash, struct{ RoutingKey string }{RoutingKey: tarHash}); err != nil {
		log.Warn().Err(err).Str("image_id", request.ImageId).Msg("snapshot layer not seeded in content cache")
	}
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

// remoteBaseImage fetches the image the sandbox runs on, for this platform.
func (c *ImageClient) remoteBaseImage(ctx context.Context, request *types.ContainerRequest, baseRef string) (v1.Image, []remote.Option, error) {
	ref, err := name.ParseReference(baseRef, c.buildRegistryNameOptions()...)
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

const (
	overlayOpaqueXattr = "trusted.overlay.opaque"
	whiteoutPrefix     = ".wh."
	opaqueWhiteout     = ".wh..wh..opq"
)

type packedLayer struct {
	compressedSize int64
	tarHash        string // sha256 of the uncompressed tar, the content cache key
}

// writeOverlayLayer packs an overlayfs upper directory as an OCI layer tar,
// gzip'd at gzPath and plain at tarPath. Overlay's whiteouts become the OCI
// ones: a 0/0 character device turns into an empty ".wh.<name>" entry and an
// opaque directory gains a ".wh..wh..opq" child. Files are captured at the
// size seen when their header is written; the sandbox is still running, so a
// file that shrinks meanwhile is zero-padded and one that grows is truncated
// to that size.
func writeOverlayLayer(upperDir, gzPath, tarPath string) (packedLayer, error) {
	var packed packedLayer
	out, err := os.Create(gzPath)
	if err != nil {
		return packed, err
	}
	defer out.Close()
	plain, err := os.Create(tarPath)
	if err != nil {
		return packed, err
	}
	defer plain.Close()
	gz, err := gzip.NewWriterLevel(out, gzip.BestSpeed)
	if err != nil {
		return packed, err
	}
	hasher := sha256.New()
	tw := tar.NewWriter(io.MultiWriter(gz, plain, hasher))

	type inode struct{ dev, ino uint64 }
	linked := map[inode]string{}

	err = filepath.WalkDir(upperDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(upperDir, path)
		if err != nil || rel == "." {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		st, _ := info.Sys().(*syscall.Stat_t)

		if info.Mode()&os.ModeCharDevice != 0 && st != nil && st.Rdev == 0 {
			return tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,
				Name:     filepath.Join(filepath.Dir(rel), whiteoutPrefix+filepath.Base(rel)),
				ModTime:  info.ModTime(),
			})
		}

		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = rel
		hdr.Xattrs, hdr.PAXRecords = nil, nil
		if info.IsDir() {
			hdr.Name += "/"
		}
		if hdr.Typeflag == tar.TypeReg && st != nil && st.Nlink > 1 {
			key := inode{uint64(st.Dev), uint64(st.Ino)}
			if target, seen := linked[key]; seen {
				hdr.Typeflag, hdr.Linkname, hdr.Size = tar.TypeLink, target, 0
			} else {
				linked[key] = rel
			}
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		switch {
		case hdr.Typeflag == tar.TypeReg && hdr.Size > 0:
			if err := copyFixedSize(tw, path, hdr.Size); err != nil {
				return err
			}
		case info.IsDir():
			if opaque, _ := readXattr(path, overlayOpaqueXattr); opaque == "y" {
				return tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: rel + "/" + opaqueWhiteout, ModTime: info.ModTime()})
			}
		}
		return nil
	})
	if err != nil {
		return packed, err
	}
	if err := tw.Close(); err != nil {
		return packed, err
	}
	if err := gz.Close(); err != nil {
		return packed, err
	}
	stat, err := out.Stat()
	if err != nil {
		return packed, err
	}
	packed.compressedSize = stat.Size()
	packed.tarHash = hex.EncodeToString(hasher.Sum(nil))
	return packed, nil
}

// copyFixedSize writes exactly size bytes of path to w, zero-padding if the
// file has shrunk since it was measured.
func copyFixedSize(w io.Writer, path string, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	n, err := io.CopyN(w, f, size)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < size {
		_, err = io.CopyN(w, zeroReader{}, size-n)
	}
	return err
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func readXattr(path, attr string) (string, error) {
	buf := make([]byte, 64)
	n, err := unix.Lgetxattr(path, attr, buf)
	if err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}
