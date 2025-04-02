package volume

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"path/filepath"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type volumeGroup struct {
	routeGroup *echo.Group
	gvs        *GlobalVolumeService
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

	var eg errgroup.Group

	eg.Go(func() error {
		defer close(ch)
		for {
			buf := make([]byte, uploadBufferSize) // 8 Mb
			n, err := stream.Read(buf)
			if n > 0 {
				ch <- CopyPathContent{
					Path:    decodedVolumePath,
					Content: buf[:n],
				}
			}
			if err != nil {
				if err == io.EOF {
					return nil
				}
				log.Error().Err(err).Msg("failed to upload file")
				return err
			}
		}
	})

	eg.Go(func() error {
		return g.gvs.copyPathStream(
			ctx.Request().Context(),
			ch,
			&workspace,
		)
	})

	if err := eg.Wait(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to upload file: %v", err))
	} else {
		return ctx.JSON(http.StatusOK, nil)
	}
}

func (g *volumeGroup) DownloadFileWithToken(ctx echo.Context) error {
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
		cc.AuthInfo.Workspace,
	); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to download file %v", err))
	} else {
		return ctx.Attachment(path, filepath.Base(decodedVolumePath))
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
		return ctx.Attachment(path, filepath.Base(decodedVolumePath))
	}
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
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to list path: %v", err))
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

func (g *volumeGroup) GenerateDownloadToken(ctx echo.Context) error {
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

	if cc.AuthInfo.Workspace.StorageAvailable() {
		storageClient, err := clients.NewStorageClient(ctx.Request().Context(), cc.AuthInfo.Workspace.Name, cc.AuthInfo.Workspace.Storage)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create storage client")
		}

		volumeName := path.Base(decodedVolumePath)

		volume, err := g.gvs.getOrCreateVolume(ctx.Request().Context(), workspace, volumeName)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create volume")
		}

		key := path.Join(types.DefaultVolumesPrefix, volume.ExternalId, decodedVolumePath)
		url, err := storageClient.GeneratePresignedGetURL(ctx.Request().Context(), key, 120)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate download token")
		}

		return ctx.JSON(http.StatusOK, url)
	} else {
		token, err := g.gvs.GenerateWorkspaceVolumePathDownloadToken(workspaceId, decodedVolumePath)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate download token")
		}

		return ctx.JSON(http.StatusOK, token)
	}
}
