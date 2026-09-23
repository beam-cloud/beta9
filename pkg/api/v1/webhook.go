package apiv1

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/labstack/echo/v4"
)

// WebhookGroup edits a workspace's webhook subscriptions; the event sink delivers (its list cache expires within seconds).
type WebhookGroup struct {
	routerGroup   *echo.Group
	workspaceRepo repository.WorkspaceRepository
}

func NewWebhookGroup(g *echo.Group, workspaceRepo repository.WorkspaceRepository) *WebhookGroup {
	group := &WebhookGroup{routerGroup: g, workspaceRepo: workspaceRepo}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.List))
	g.POST("/:workspaceId", auth.WithWorkspaceAuth(group.Create))
	g.PATCH("/:workspaceId/:webhookId", auth.WithWorkspaceAuth(group.Update))
	g.DELETE("/:workspaceId/:webhookId", auth.WithWorkspaceAuth(group.Delete))
	g.POST("/:workspaceId/:webhookId/test", auth.WithWorkspaceAuth(group.Test))

	return group
}

type WebhookRequest struct {
	URL         string   `json:"url"`
	EventTypes  []string `json:"event_types"`
	Description string   `json:"description"`
	Enabled     *bool    `json:"enabled"`
}

// The secret is returned once, on creation.
func redact(w types.WorkspaceWebhook) types.WorkspaceWebhook {
	w.Secret = ""
	return w
}

func validateWebhookURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "url must be an absolute http(s) URL")
	}
	return nil
}

func (g *WebhookGroup) List(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	webhooks, err := g.workspaceRepo.ListWebhooks(ctx.Request().Context(), cc.AuthInfo.Workspace.ExternalId)
	if err != nil {
		return HTTPInternalServerError("Failed to list webhooks")
	}
	out := make([]types.WorkspaceWebhook, 0, len(webhooks))
	for _, w := range webhooks {
		out = append(out, redact(w))
	}
	return ctx.JSON(http.StatusOK, out)
}

func (g *WebhookGroup) Create(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	var req WebhookRequest
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Invalid payload")
	}
	if err := validateWebhookURL(req.URL); err != nil {
		return err
	}

	id, err := common.GenerateObjectId()
	if err != nil {
		return HTTPInternalServerError("Failed to generate id")
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return HTTPInternalServerError("Failed to generate secret")
	}

	webhook := types.WorkspaceWebhook{
		ExternalId:  id,
		URL:         strings.TrimSpace(req.URL),
		EventTypes:  req.EventTypes,
		Secret:      "whsec_" + hex.EncodeToString(secretBytes),
		Description: req.Description,
		Enabled:     req.Enabled == nil || *req.Enabled,
		CreatedAt:   time.Now().UTC(),
	}
	if err := g.workspaceRepo.SetWebhook(ctx.Request().Context(), cc.AuthInfo.Workspace.ExternalId, webhook); err != nil {
		return HTTPInternalServerError("Failed to save webhook")
	}
	return ctx.JSON(http.StatusCreated, webhook)
}

func (g *WebhookGroup) find(ctx echo.Context, workspaceId string) (*types.WorkspaceWebhook, error) {
	webhooks, err := g.workspaceRepo.ListWebhooks(ctx.Request().Context(), workspaceId)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to list webhooks")
	}
	for i := range webhooks {
		if webhooks[i].ExternalId == ctx.Param("webhookId") {
			return &webhooks[i], nil
		}
	}
	return nil, HTTPNotFound()
}

func (g *WebhookGroup) Update(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	webhook, err := g.find(ctx, cc.AuthInfo.Workspace.ExternalId)
	if err != nil {
		return err
	}

	var req WebhookRequest
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Invalid payload")
	}
	if req.URL != "" {
		if err := validateWebhookURL(req.URL); err != nil {
			return err
		}
		webhook.URL = strings.TrimSpace(req.URL)
	}
	if req.EventTypes != nil {
		webhook.EventTypes = req.EventTypes
	}
	if req.Description != "" {
		webhook.Description = req.Description
	}
	if req.Enabled != nil {
		webhook.Enabled = *req.Enabled
	}
	if err := g.workspaceRepo.SetWebhook(ctx.Request().Context(), cc.AuthInfo.Workspace.ExternalId, *webhook); err != nil {
		return HTTPInternalServerError("Failed to save webhook")
	}
	return ctx.JSON(http.StatusOK, redact(*webhook))
}

func (g *WebhookGroup) Delete(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if _, err := g.find(ctx, cc.AuthInfo.Workspace.ExternalId); err != nil {
		return err
	}
	if err := g.workspaceRepo.DeleteWebhook(ctx.Request().Context(), cc.AuthInfo.Workspace.ExternalId, ctx.Param("webhookId")); err != nil {
		return HTTPInternalServerError("Failed to delete webhook")
	}
	return ctx.NoContent(http.StatusNoContent)
}

// Test posts a `webhook.test` event synchronously and reports the receiver's status.
func (g *WebhookGroup) Test(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	webhook, err := g.find(ctx, cc.AuthInfo.Workspace.ExternalId)
	if err != nil {
		return err
	}

	event := cloudevents.NewEvent()
	event.SetID(webhook.ExternalId + "/test")
	event.SetSource("beta9-cluster")
	event.SetType("webhook.test")
	event.SetTime(time.Now().UTC())
	if err := event.SetData(cloudevents.ApplicationJSON, map[string]string{"workspace_id": cc.AuthInfo.Workspace.ExternalId, "webhook_id": webhook.ExternalId}); err != nil {
		return HTTPInternalServerError("Failed to build test event")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	if err := repository.DeliverWebhook(ctx.Request().Context(), client, *webhook, event); err != nil {
		return ctx.JSON(http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
	}
	return ctx.JSON(http.StatusOK, map[string]any{"ok": true})
}
