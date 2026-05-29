package gateway

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const agentImageArchiveRoot = "/images"

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

		path := filepath.Join(agentImageArchiveRoot, file)
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return echo.NewHTTPError(http.StatusNotFound, "image archive not found")
			}
			return err
		}

		return c.File(path)
	}
}

func validAgentImageArchiveName(imageID, file string) bool {
	if imageID == "" || strings.ContainsAny(imageID, `/\`) {
		return false
	}
	return file == imageID+"."+registry.LocalImageFileExtension ||
		file == imageID+"."+registry.RemoteImageFileExtension
}
