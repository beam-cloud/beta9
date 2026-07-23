package image

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

const baseImageDigestCacheTTL = 5 * time.Minute

var dockerfileFromLinePattern = regexp.MustCompile(`(?i)^([ \t]*FROM[ \t]+(?:--[^ \t\r\n]+[ \t]+)*)([^ \t\r\n]+)([^\r\n]*)(\r?\n)?$`)

type baseImageDigestCacheEntry struct {
	digest    string
	expiresAt time.Time
}

type baseImageDigestCache struct {
	mu      sync.Mutex
	entries map[string]baseImageDigestCacheEntry
	group   singleflight.Group
}

func newBaseImageDigestCache() baseImageDigestCache {
	return baseImageDigestCache{
		entries: make(map[string]baseImageDigestCacheEntry),
	}
}

func (is *ContainerImageService) resolveBaseImageDigest(ctx context.Context, opts *BuildOpts, cacheable bool) {
	if opts == nil || opts.BaseImageDigest != "" || opts.BaseImageRegistry == "" || opts.BaseImageName == "" || opts.BaseImageTag == "" {
		return
	}

	sourceImage := getSourceImage(opts)
	if !cacheable {
		opts.BaseImageDigest = is.inspectBaseImageDigest(ctx, sourceImage, opts.BaseImageCreds)
		return
	}

	digest, shared := is.baseImageDigests.resolve(ctx, sourceImage, opts.BaseImageCreds, is.inspectBaseImageDigest)
	if digest == "" {
		return
	}

	opts.BaseImageDigest = digest
	if shared {
		log.Debug().Str("source_image", sourceImage).Msg("resolved base image digest from shared lookup")
	}
}

func pinImageRefToDigest(imageRef, digest string) string {
	if at := strings.LastIndex(imageRef, "@"); at >= 0 {
		return imageRef[:at] + "@" + digest
	}

	lastSlash := strings.LastIndex(imageRef, "/")
	if colon := strings.LastIndex(imageRef, ":"); colon > lastSlash {
		imageRef = imageRef[:colon]
	}
	return imageRef + "@" + digest
}

func pinDockerfileFromDigests(dockerfile string, resolve func(string) string) (string, *BaseImage) {
	lines := strings.SplitAfter(dockerfile, "\n")
	stages := make(map[string]struct{})
	var firstBase *BaseImage

	for i, line := range lines {
		matches := dockerfileFromLinePattern.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		imageRef := matches[2]
		rest := matches[3]
		fields := strings.Fields(rest)
		alias := ""
		if len(fields) >= 2 && strings.EqualFold(fields[0], "AS") {
			alias = fields[1]
		}

		_, isStage := stages[strings.ToLower(imageRef)]
		literalBase := !isStage && !strings.EqualFold(imageRef, "scratch") && !strings.ContainsAny(imageRef, "$\\")
		if literalBase {
			base, err := ExtractImageNameAndTag(imageRef)
			if err == nil {
				digest := base.Digest
				if digest == "" {
					digest = resolve(imageRef)
				}
				if digest != "" {
					pinnedRef := pinImageRefToDigest(imageRef, digest)
					lines[i] = matches[1] + pinnedRef + rest + matches[4]
					pinnedBase, err := ExtractImageNameAndTag(pinnedRef)
					if err == nil && firstBase == nil {
						firstBase = &pinnedBase
					}
				}
			}
		}

		if alias != "" {
			stages[strings.ToLower(alias)] = struct{}{}
		}
	}

	return strings.Join(lines, ""), firstBase
}

func (is *ContainerImageService) pinDockerfileBaseImages(ctx context.Context, opts *BuildOpts) {
	if opts == nil || opts.Dockerfile == "" {
		return
	}

	dockerfile, base := pinDockerfileFromDigests(opts.Dockerfile, func(imageRef string) string {
		digest, _ := is.baseImageDigests.resolve(ctx, imageRef, opts.BaseImageCreds, is.inspectBaseImageDigest)
		return digest
	})
	opts.Dockerfile = dockerfile
	if base == nil {
		return
	}

	opts.BaseImageRegistry = base.Registry
	opts.BaseImageName = base.Repo
	opts.BaseImageTag = base.Tag
	opts.BaseImageDigest = base.Digest
}

func (c *baseImageDigestCache) resolve(
	ctx context.Context,
	sourceImage string,
	creds string,
	inspect func(context.Context, string, string) string,
) (string, bool) {
	if digest := c.get(sourceImage); digest != "" {
		return digest, false
	}

	digest, _, shared := c.group.Do(sourceImage, func() (interface{}, error) {
		if digest := c.get(sourceImage); digest != "" {
			return digest, nil
		}

		digest := inspect(ctx, sourceImage, creds)
		if digest != "" {
			c.set(sourceImage, digest)
		}
		return digest, nil
	})

	resolvedDigest, _ := digest.(string)
	return resolvedDigest, shared
}

func (is *ContainerImageService) inspectBaseImageDigest(ctx context.Context, sourceImage, creds string) string {
	startedAt := time.Now()
	metadata, err := is.builder.skopeoClient.Inspect(ctx, sourceImage, creds, nil)
	if err != nil {
		log.Warn().Err(err).Str("source_image", sourceImage).Msg("failed to resolve base image digest for image identity")
		return ""
	}
	if metadata.Digest == "" {
		log.Warn().Str("source_image", sourceImage).Msg("base image digest missing from registry inspect")
		return ""
	}

	log.Debug().Str("source_image", sourceImage).Dur("duration", time.Since(startedAt)).Msg("resolved base image digest")
	return metadata.Digest
}

func (c *baseImageDigestCache) get(sourceImage string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[sourceImage]
	if !ok {
		return ""
	}
	if time.Now().After(entry.expiresAt) {
		delete(c.entries, sourceImage)
		return ""
	}
	return entry.digest
}

func (c *baseImageDigestCache) set(sourceImage, digest string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[sourceImage] = baseImageDigestCacheEntry{
		digest:    digest,
		expiresAt: time.Now().Add(baseImageDigestCacheTTL),
	}
}
