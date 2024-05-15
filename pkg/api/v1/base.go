package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const (
	HttpServerBaseRoute string = "/api/v1"
	HttpServerRootRoute string = ""
)

func WithWorkspaceAuth(callable func(ctx echo.Context) error) func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		workspaceIdParam := ctx.Param("workspaceId")

		// Also check if the workspaceId is headers (In case we decide to use headers instead of path params)
		workspaceIdHeader := ctx.Request().Header.Get("x-workspace-id")

		var workspaceId string
		if workspaceIdParam != "" {
			workspaceId = workspaceIdParam
		} else if workspaceIdHeader != "" {
			workspaceId = workspaceIdHeader
		}

		if workspaceId == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "Workspace ID is required")
		}

		cc, _ := ctx.(*auth.HttpAuthContext)
		if cc.AuthInfo.Workspace.ExternalId != workspaceId && cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
			return echo.NewHTTPError(http.StatusUnauthorized)
		}

		return callable(ctx)
	}
}
