package volume

import (
	"context"
	"net/http"
	"net/url"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type volumeGroup struct {
	routeGroup *echo.Group
	gvs        *GlobalVolumeService
}

func registerVolumeRoutes(g *echo.Group, gvs *GlobalVolumeService) *volumeGroup {
	group := &volumeGroup{
		routeGroup: g,
		gvs:        gvs,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListVolumes))
	g.POST("/:workspaceId/create/:volumeName", auth.WithStrictWorkspaceAuth(group.CreateVolume))
	g.PUT("/:workspaceId/upload/:volumePath*", auth.WithStrictWorkspaceAuth(group.UploadFile))
	g.GET("/:workspaceId/generate-upload-url/:volumePath*", auth.WithStrictWorkspaceAuth(group.GetUploadURL))
	g.GET("/:workspaceId/generate-download-url/:volumePath*", auth.WithWorkspaceAuth(group.GetDownloadURL))
	g.GET("/:workspaceId/download/:volumePath*", auth.WithWorkspaceAuth(group.DownloadFile))
	g.GET("/:workspaceId/ls/:volumePath*", auth.WithWorkspaceAuth(group.Ls))
	g.DELETE("/:workspaceId/rm/:volumePath*", auth.WithStrictWorkspaceAuth(group.Rm))
	g.PATCH("/:workspaceId/mv/:volumePath*", auth.WithStrictWorkspaceAuth(group.Mv))

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
	cc := ctx.(*auth.HttpAuthContext)
	decoded, err := url.QueryUnescape(ctx.Param("volumePath*"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}
	if err := g.gvs.copyPath(ctx.Request().Context(), decoded, ctx.Request().Body, cc.AuthInfo.Workspace); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return ctx.JSON(http.StatusOK, nil)
}

func (g *volumeGroup) DownloadFile(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	decoded, err := url.QueryUnescape(ctx.Param("volumePath*"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}
	location, err := g.generatePresignedURL(ctx.Request().Context(), cc.AuthInfo.Workspace, decoded, http.MethodGet)
	if err != nil {
		return err
	}
	return ctx.Redirect(http.StatusTemporaryRedirect, location)
}

func (g *volumeGroup) Ls(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceId := ctx.Param("workspaceId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceId {
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
		cc.AuthInfo.Workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	} else {
		return ctx.JSON(http.StatusOK, paths)
	}
}

func (g *volumeGroup) Rm(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	workspaceId := ctx.Param("workspaceId")

	if cc.AuthInfo.Workspace.ExternalId != workspaceId {
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
		cc.AuthInfo.Workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to delete path")
	} else {
		return ctx.JSON(http.StatusOK, nil)
	}
}

func (g *volumeGroup) Mv(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) GetDownloadURL(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	workspaceId := ctx.Param("workspaceId")

	workspace := cc.AuthInfo.Workspace
	if workspace.ExternalId != workspaceId {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	url, err := g.generatePresignedURL(ctx.Request().Context(), workspace, decodedVolumePath, http.MethodGet)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate presigned URL")
	}

	return ctx.JSON(http.StatusOK, url)
}

func (g *volumeGroup) GetUploadURL(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	workspaceId := ctx.Param("workspaceId")
	workspace := cc.AuthInfo.Workspace
	if workspace.ExternalId != workspaceId {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	url, err := g.generatePresignedURL(ctx.Request().Context(), workspace, decodedVolumePath, http.MethodPut)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate presigned URL")
	}

	return ctx.JSON(http.StatusOK, url)
}

const PresignedGetURLExpiration = 3600 // 1 hour

func (g *volumeGroup) generatePresignedURL(ctx context.Context, workspace *types.Workspace, volumePath string, urlType string) (string, error) {
	volumeName, volumePath := parseVolumeInput(volumePath)
	volume, err := g.gvs.getOrCreateVolume(ctx, workspace, volumeName)
	if err != nil {
		return "", echo.NewHTTPError(http.StatusInternalServerError, "Failed to create volume")
	}

	key, err := volumeObjectKey(volume.ExternalId, volumePath)
	if err != nil {
		return "", echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	storageClient, err := g.gvs.newWorkspaceStorageClientForPresign(ctx, workspace)
	if err != nil {
		return "", echo.NewHTTPError(http.StatusInternalServerError, "Failed to create storage client")
	}

	var url string
	switch urlType {
	case http.MethodPut:
		url, err = storageClient.GeneratePresignedPutURL(ctx, key, PresignedGetURLExpiration)
		if err != nil {
			return "", echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate upload token")
		}
	case http.MethodGet:
		url, err = storageClient.GeneratePresignedGetURL(ctx, key, PresignedGetURLExpiration)
		if err != nil {
			return "", echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate download token")
		}
	default:
		return "", echo.NewHTTPError(http.StatusBadRequest, "Invalid URL type")
	}

	return url, nil
}
