package volume

import (
	"log"
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type volumeGroup struct {
	routeGroup *echo.Group
	gvs        *GlobalVolumeService
}

func registerVolumeRoutes(g *echo.Group, gvs *GlobalVolumeService) *volumeGroup {
	group := &volumeGroup{routeGroup: g, gvs: gvs}

	g.GET("/:workspaceId", group.ListVolumes)
	g.GET("/:workspaceId/", group.ListVolumes)

	// g.DELETE("/:workspaceId/delete/:volumeName", group.DeleteVolume)
	// g.DELETE("/:workspaceId/delete/:volumeName/", group.DeleteVolume)

	g.POST("/:workspaceId/:volumeName", group.CreateVolume)
	g.POST("/:workspaceId/:volumeName/", group.CreateVolume)

	g.GET("/:workspaceId/:volumeName/*", group.Ls)
	g.DELETE("/:workspaceId/:volumeName/*", group.Rm)
	g.PATCH("/:workspaceId/:volumeName/*", group.Mv)
	// g.POST("/:workspaceId/:volumeName/*", group.UploadFile)

	// g.GET("/:workspaceId/:volumeName/*", group.DownloadFile)

	return group
}

func (g *volumeGroup) ListVolumes(ctx echo.Context) error {
	_, err := g.authorize(ctx)
	if err != nil {
		return err
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	if volumes, err := g.gvs.listVolumes(ctx.Request().Context(), &workspace); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list volumes")
	} else {
		log.Println(volumes)
		return ctx.JSON(http.StatusOK, volumes)
	}
}

func (g *volumeGroup) DeleteVolume(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) CreateVolume(ctx echo.Context) error {
	_, err := g.authorize(ctx)
	if err != nil {
		return err
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumeName := ctx.Param("volumeName")
	if volume, err := g.gvs.getOrCreateVolume(ctx.Request().Context(), &workspace, volumeName); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create volume")
	} else {
		return ctx.JSON(http.StatusOK, volume)
	}
}

func (g *volumeGroup) UploadFile(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) DownloadFile(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) Ls(ctx echo.Context) error {
	_, err := g.authorize(ctx)
	if err != nil {
		return err
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumeName := ctx.Param("volumeName")
	path := ctx.Param("*")

	if paths, err := g.gvs.listPath(
		ctx.Request().Context(),
		volumeName+"/"+path,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list path")
	} else {
		return ctx.JSON(http.StatusOK, paths)
	}
}

func (g *volumeGroup) Rm(ctx echo.Context) error {
	_, err := g.authorize(ctx)
	if err != nil {
		return err
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumeName := ctx.Param("volumeName")
	path := ctx.Param("*")

	if _, err := g.gvs.deletePath(
		ctx.Request().Context(),
		volumeName+"/"+path,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to delete path")
	} else {
		return ctx.JSON(http.StatusOK, nil)
	}
}

func (g *volumeGroup) Mv(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) authorize(ctx echo.Context) (*auth.HttpAuthContext, error) {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return nil, echo.NewHTTPError(http.StatusUnauthorized)
	}
	return cc, nil
}
