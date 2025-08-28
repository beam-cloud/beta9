package volume

import (
	"context"
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

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListVolumes))
	g.POST("/:workspaceId/create/:volumeName", auth.WithRestrictedWorkspaceAuth(group.CreateVolume))
	g.PUT("/:workspaceId/upload/:volumePath*", auth.WithRestrictedWorkspaceAuth(group.UploadFile))
	g.GET("/:workspaceId/generate-download-token/:volumePath*", auth.WithWorkspaceAuth(group.GenerateDownloadToken))
	g.GET("/:workspaceId/generate-upload-url/:volumePath*", auth.WithRestrictedWorkspaceAuth(group.GetUploadURL))
	g.GET("/:workspaceId/generate-download-url/:volumePath*", auth.WithWorkspaceAuth(group.GetDownloadURL))
	g.GET("/:workspaceId/download-with-token/:volumePath*", group.DownloadFileWithToken)
	g.GET("/:workspaceId/download/:volumePath*", auth.WithWorkspaceAuth(group.DownloadFile))
	g.GET("/:workspaceId/ls/:volumePath*", auth.WithWorkspaceAuth(group.Ls))
	g.DELETE("/:workspaceId/rm/:volumePath*", auth.WithRestrictedWorkspaceAuth(group.Rm))
	g.PATCH("/:workspaceId/mv/:volumePath*", auth.WithRestrictedWorkspaceAuth(group.Mv))

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
	workspaceId := ctx.Param("workspaceId")
	volumePath := ctx.Param("volumePath*")
	decodedVolumePath, err := url.QueryUnescape(volumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid volume path")
	}

	workspace, err := g.gvs.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
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
		return ctx.Attachment(path, filepath.Base(decodedVolumePath))
	}
}

func (g *volumeGroup) DownloadFile(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

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

	if cc.AuthInfo.Workspace.StorageAvailable() {
		presignedUrl, err := g.generatePresignedURL(
			ctx.Request().Context(),
			cc.AuthInfo.Workspace,
			decodedVolumePath,
			http.MethodGet,
		)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate presigned URL")
		}

		return ctx.Redirect(http.StatusTemporaryRedirect, presignedUrl)
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

	token, err := g.gvs.GenerateWorkspaceVolumePathDownloadToken(workspaceId, decodedVolumePath)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate download token")
	}

	return ctx.JSON(http.StatusOK, token)
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

	if !cc.AuthInfo.Workspace.StorageAvailable() {
		return g.GenerateDownloadToken(ctx)
	}

	url, err := g.generatePresignedURL(ctx.Request().Context(), workspace, decodedVolumePath, http.MethodGet)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate presigned URL")
	}

	return ctx.JSON(http.StatusOK, url)
}

func (g *volumeGroup) GetUploadURL(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	if !cc.AuthInfo.Workspace.StorageAvailable() {
		return ctx.JSON(http.StatusMethodNotAllowed, nil)
	}

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

	if !cc.AuthInfo.Workspace.StorageAvailable() {
		return g.GenerateDownloadToken(ctx)
	}

	url, err := g.generatePresignedURL(ctx.Request().Context(), workspace, decodedVolumePath, http.MethodPut)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate presigned URL")
	}

	return ctx.JSON(http.StatusOK, url)
}

const PresignedGetURLExpiration = 3600 // 1 hour

func (g *volumeGroup) generatePresignedURL(ctx context.Context, workspace *types.Workspace, volumePath string, urlType string) (string, error) {
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return "", echo.NewHTTPError(http.StatusInternalServerError, "Failed to create storage client")
	}

	volumeName, volumePath := parseVolumeInput(volumePath)
	volume, err := g.gvs.getOrCreateVolume(ctx, workspace, volumeName)
	if err != nil {
		return "", echo.NewHTTPError(http.StatusInternalServerError, "Failed to create volume")
	}

	key := path.Join(types.DefaultVolumesPrefix, volume.ExternalId, volumePath)

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
