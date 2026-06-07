package gateway

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type agentImageArchiveReader interface {
	GetReader(ctx context.Context, key string) (io.ReadCloser, error)
}

var (
	agentImagesPath = types.AgentImagesPath

	newAgentImageArchiveReader = func(config types.AppConfig) (agentImageArchiveReader, error) {
		return registry.NewImageRegistry(config, config.ImageService.Registries.S3)
	}
)

func (g *Gateway) agentImageArchiveHandler() echo.HandlerFunc {
	return func(c echo.Context) error {
		cc, ok := c.(*auth.HttpAuthContext)
		if !ok || cc.AuthInfo == nil || cc.AuthInfo.Token == nil {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}
		if cc.AuthInfo.Token.TokenType != types.TokenTypeWorker && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
			return echo.NewHTTPError(http.StatusForbidden)
		}

		imageID := strings.TrimSpace(c.Param("image_id"))
		file := filepath.Base(c.Param("file"))
		if !validAgentImageArchiveName(imageID, file) {
			return echo.NewHTTPError(http.StatusBadRequest, "invalid image archive")
		}

		return g.serveAgentImageArchive(c, file)
	}
}

func (g *Gateway) serveAgentImageArchive(c echo.Context, file string) error {
	for _, archiveFile := range agentImageArchiveCandidates(file) {
		path := filepath.Join(agentImagesPath, archiveFile)
		if _, err := os.Stat(path); err == nil {
			return c.File(path)
		} else if !os.IsNotExist(err) {
			return err
		}

		reader, err := g.openAgentImageArchive(c.Request().Context(), archiveFile)
		if err != nil {
			if registry.IsObjectNotFound(err) {
				continue
			}
			return err
		}
		defer reader.Close()

		return c.Stream(http.StatusOK, "application/octet-stream", reader)
	}

	return echo.NewHTTPError(http.StatusNotFound, "image archive not found")
}

func (g *Gateway) openAgentImageArchive(ctx context.Context, file string) (io.ReadCloser, error) {
	imageRegistry, err := newAgentImageArchiveReader(g.Config)
	if err != nil {
		return nil, err
	}

	return imageRegistry.GetReader(ctx, file)
}

func validAgentImageArchiveName(imageID, file string) bool {
	if imageID == "" || strings.ContainsAny(imageID, `/\`) {
		return false
	}
	return file == imageID+"."+registry.LocalImageFileExtension ||
		file == imageID+"."+registry.RemoteImageFileExtension
}

func agentImageArchiveCandidates(file string) []string {
	extension := strings.TrimPrefix(filepath.Ext(file), ".")
	imageID := strings.TrimSuffix(file, "."+extension)

	candidates := []string{}
	for _, candidate := range []string{
		file,
		imageID + "." + registry.RemoteImageFileExtension,
		imageID + "." + registry.LocalImageFileExtension,
	} {
		if candidate == "" {
			continue
		}
		seen := false
		for _, existing := range candidates {
			if candidate == existing {
				seen = true
				break
			}
		}
		if !seen {
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}
