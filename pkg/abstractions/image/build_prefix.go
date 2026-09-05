package image

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// A rendered v2 Dockerfile is one instruction per line, so an image built from
// it is exactly the state after its last line. When a later build's Dockerfile
// starts with the same lines (a step appended to an image someone already
// built) the earlier image serves as FROM and only the new lines run. This is
// the cross-node counterpart of a per-step layer cache, keyed on the rendered
// lines plus the build context they may COPY from.

const (
	builtDockerfileKeyPrefix = "image:built_dockerfile:"
	builtDockerfileTTL       = 30 * 24 * time.Hour
	// minPrefixLines is FROM plus at least one instruction: reusing an image
	// that only wraps the base would be a pull of the base with extra steps.
	minPrefixLines = 2
)

// imageExists is Exists, unless a test swapped it.
func (b *Builder) imageExists(ctx context.Context, imageID string) (bool, error) {
	if b.existsOverride != nil {
		return b.existsOverride(ctx, imageID)
	}
	return b.Exists(ctx, imageID)
}

func builtDockerfileKey(buildCtxObject string, lines []string) string {
	sum := sha256.Sum256([]byte(buildCtxObject + "\x00" + strings.Join(lines, "\n")))
	return builtDockerfileKeyPrefix + hex.EncodeToString(sum[:])
}

// dockerfileLines splits a rendered Dockerfile into its instructions.
func dockerfileLines(dockerfile string) []string {
	var lines []string
	for _, line := range strings.Split(dockerfile, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// prefixReusable reports whether opts describe a Dockerfile the gateway
// rendered. Custom Dockerfiles are left alone: they may span lines, carry
// several stages or ONBUILD triggers, none of which survive being cut at a
// line boundary and re-based on another image.
func (b *Builder) prefixReusable(opts *BuildOpts) bool {
	return b.rdb != nil &&
		opts.ClipVersion == uint32(types.ClipVersion2) &&
		opts.Dockerfile != "" &&
		opts.BaseImageName != "" &&
		b.config.ImageService.BuildRegistry != "" &&
		b.config.ImageService.BuildRepositoryName != "" &&
		renderedDockerfile(opts)
}

// renderedDockerfile reports whether opts.Dockerfile has the shape
// RenderV2Dockerfile produces: FROM the resolved base image, then one
// instruction per line with no comments, continuations or further stages.
// The base image fields alone do not tell a rendered Dockerfile from a custom
// one, since base image pinning fills them in for custom Dockerfiles too.
func renderedDockerfile(opts *BuildOpts) bool {
	lines := dockerfileLines(opts.Dockerfile)
	if len(lines) == 0 || lines[0] != "FROM "+getSourceImage(opts) {
		return false
	}
	for _, line := range lines[1:] {
		if line != strings.TrimLeft(line, " \t") || strings.HasSuffix(line, "\\") || strings.HasPrefix(line, "#") {
			return false
		}
		instr, _, _ := strings.Cut(line, " ")
		switch strings.ToUpper(instr) {
		case "FROM", "ONBUILD":
			return false
		}
	}
	return true
}

// reuseBuiltPrefix looks for the longest already-built prefix of opts'
// Dockerfile and, when found, returns a Dockerfile that starts FROM that image
// and only carries the remaining lines, plus the image reference to pull.
// ARG lines of the prefix are re-declared so build secrets stay visible.
func (b *Builder) reuseBuiltPrefix(ctx context.Context, opts *BuildOpts) (dockerfile, sourceImage string, ok bool) {
	if !b.prefixReusable(opts) {
		return "", "", false
	}
	lines := dockerfileLines(opts.Dockerfile)
	for k := len(lines) - 1; k >= minPrefixLines; k-- {
		imageID, err := b.rdb.Get(ctx, builtDockerfileKey(opts.BuildCtxObject, lines[:k])).Result()
		if err != nil || imageID == "" {
			continue
		}
		exists, err := b.imageExists(ctx, imageID)
		if err != nil || !exists {
			continue
		}
		sourceImage = fmt.Sprintf("%s/%s:%s", b.config.ImageService.BuildRegistry, b.config.ImageService.BuildRepositoryName, imageID)
		var sb strings.Builder
		sb.WriteString("FROM " + sourceImage + "\n")
		for _, line := range lines[:k] {
			if strings.HasPrefix(line, "ARG ") {
				sb.WriteString(line + "\n")
			}
		}
		for _, line := range lines[k:] {
			sb.WriteString(line + "\n")
		}
		log.Info().Str("prefix_image_id", imageID).Int("reused_lines", k).Int("total_lines", len(lines)).Msg("building from an already-built image prefix")
		return sb.String(), sourceImage, true
	}
	return "", "", false
}

// recordBuiltDockerfile remembers that imageID is the result of opts'
// Dockerfile, for later builds that extend it.
func (b *Builder) recordBuiltDockerfile(ctx context.Context, opts *BuildOpts, imageID string) {
	if !b.prefixReusable(opts) {
		return
	}
	lines := dockerfileLines(opts.Dockerfile)
	if len(lines) < minPrefixLines {
		return
	}
	if err := b.rdb.Set(ctx, builtDockerfileKey(opts.BuildCtxObject, lines), imageID, builtDockerfileTTL).Err(); err != nil {
		log.Warn().Err(err).Str("image_id", imageID).Msg("record built dockerfile")
	}
}
