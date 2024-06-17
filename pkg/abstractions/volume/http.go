package volume

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/labstack/echo/v4"
)

type volumeGroup struct {
	routeGroup    *echo.Group
	gvs           *GlobalVolumeService
	workspaceRepo repository.WorkspaceRepository
}

var uploadBufferSize = 1024 * 1000 * 8 // 8 Mb

func registerVolumeRoutes(g *echo.Group, gvs *GlobalVolumeService) *volumeGroup {
	group := &volumeGroup{
		routeGroup: g,
		gvs:        gvs,
	}

	g.GET("/:workspaceId", group.ListVolumes)

	g.POST("/:workspaceId/create/:volumeName", auth.WithWorkspaceAuth(group.CreateVolume))
	g.PUT("/:workspaceId/upload/:volumePath*", auth.WithWorkspaceAuth(group.UploadFile))
	g.GET("/:workspaceId/generate-download-token/:volumePath*", auth.WithWorkspaceAuth(group.GenerateDownloadToken))
	g.GET("/:workspaceId/download-with-token/:volumePath*", group.DownloadFileWithToken)
	g.GET("/:workspaceId/download/:volumePath*", auth.WithWorkspaceAuth(group.DownloadFile))
	g.GET("/:workspaceId/ls/:volumePath*", auth.WithWorkspaceAuth(group.Ls))
	g.DELETE("/:workspaceId/rm/:volumePath*", auth.WithWorkspaceAuth(group.Rm))
	g.PATCH("/:workspaceId/mv/:volumePath*", auth.WithWorkspaceAuth(group.Mv))

	return group
}

func (g *volumeGroup) ListVolumes(ctx echo.Context) error {
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
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	stream := ctx.Request().Body
	defer stream.Close()

	ch := make(chan CopyPathContent)
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	go func() {
		defer close(ch)

		for {
			buf := make([]byte, uploadBufferSize) // 8 Mb
			n, err := stream.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Failed to upload file: %v\n", err)
				break
			}

			ch <- CopyPathContent{
				Path:    decodedVolumePath,
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

func (g *volumeGroup) DownloadFileWithToken(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	token := ctx.QueryParam("token")
	if token == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid download token")
	}

	if err := g.gvs.ValidateWorkspaceVolumePathDownloadToken(workspaceId, decodedVolumePath, token); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid download token")
	}

	if path, err := g.gvs.getFilePath(
		ctx.Request().Context(),
		decodedVolumePath,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to download file %v", err))
	} else {
		return ctx.File(path)
	}
}

func (g *volumeGroup) DownloadFile(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	if path, err := g.gvs.getFilePath(
		ctx.Request().Context(),
		decodedVolumePath,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to download file %v", err))
	} else {
		return ctx.File(path)
	}
}

func (g *volumeGroup) Ls(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	if paths, err := g.gvs.listPath(
		ctx.Request().Context(),
		decodedVolumePath,
		&workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to list path: %v", err))
	} else {
		return ctx.JSON(http.StatusOK, paths)
	}
}

func (g *volumeGroup) Rm(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	if _, err := g.gvs.deletePath(
		ctx.Request().Context(),
		decodedVolumePath,
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

func (g *volumeGroup) GenerateDownloadToken(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	token, err := g.gvs.GenerateWorkspaceVolumePathDownloadToken(workspaceId, decodedVolumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate download token")
	}

	return ctx.JSON(http.StatusOK, token)
}
