package volume

import (
	"fmt"
	"io"
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/labstack/echo/v4"
)

type volumeGroup struct {
	routeGroup *echo.Group
	gvs        *GlobalVolumeService
}

func registerVolumeRoutes(g *echo.Group, gvs *GlobalVolumeService) *volumeGroup {
	group := &volumeGroup{routeGroup: g, gvs: gvs}

	g.GET("/:workspaceId", group.ListVolumes)

	g.POST("/:workspaceId/create/:volumeName", group.CreateVolume)
	g.PUT("/:workspaceId/upload/:volumePath*", group.UploadFile)
	g.GET("/:workspaceId/download/:volumePath*", group.DownloadFile)
	g.GET("/:workspaceId/ls/:volumePath*", group.Ls)
	g.DELETE("/:workspaceId/rm/:volumePath*", group.Rm)
	g.PATCH("/:workspaceId/mv/:volumePath*", group.Mv)

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
	_, err := g.authorize(ctx)
	if err != nil {
		return err
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	stream := ctx.Request().Body
	ch := make(chan CopyPathContent)

	go func() {
		defer close(ch)

		for {
			buf := make([]byte, 1024)
			n, err := stream.Read(buf)
			if err == io.EOF {
				break
			}

			ch <- CopyPathContent{
				Path:    volumePath,
				Content: buf[:n],
			}
		}
	}()

	if err := g.gvs.copyPathStream(
		ctx.Request().Context(),
		ch,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to upload file: %v", err))
	} else {
		return ctx.JSON(http.StatusOK, nil)
	}
}

func (g *volumeGroup) DownloadFile(ctx echo.Context) error {

	_, err := g.authorize(ctx)
	if err != nil {
		return err
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	if f, err := g.gvs.getFileFd(
		ctx.Request().Context(),
		volumePath,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to download file %v", err))
	} else {
		return ctx.Stream(http.StatusOK, "application/octet-stream", f)
	}
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

	volumePath := ctx.Param("volumePath*")
	if paths, err := g.gvs.listPath(
		ctx.Request().Context(),
		volumePath,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to list path: %v", err))
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

	volumePath := ctx.Param("volumePath*")
	if _, err := g.gvs.deletePath(
		ctx.Request().Context(),
		volumePath,
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
	return cc, nil
}
