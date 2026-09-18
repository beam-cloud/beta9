// Package router is where LLM traffic from agent harnesses meets beta9. Its
// first API ingests harness traces (one record per turn) and lands them on
// the workspace event stream; routing model calls to hosted endpoints will
// live alongside it.
package router

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const (
	maxTracesPerRequest = 256
	maxRequestBytes     = 32 << 20
)

type Group struct {
	eventRepo repository.EventRepository
}

func NewGroup(g *echo.Group, eventRepo repository.EventRepository) *Group {
	group := &Group{eventRepo: eventRepo}
	g.POST("/traces", group.PushTraces)
	return group
}

type PushTracesRequest struct {
	Traces []types.Trace `json:"traces"`
}

type PushTracesResponse struct {
	Accepted int `json:"accepted"`
}

// PushTraces accepts a batch of harness traces for the caller's workspace.
// Delivery is at-least-once; a trace keeps its id as the event id so readers
// can recognize a redelivery.
func (g *Group) PushTraces(ctx echo.Context) error {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok || cc.AuthInfo == nil || cc.AuthInfo.Workspace == nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "Workspace token required")
	}
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, maxRequestBytes)

	var req PushTracesRequest
	if err := ctx.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request body")
	}
	if len(req.Traces) == 0 || len(req.Traces) > maxTracesPerRequest {
		return echo.NewHTTPError(http.StatusBadRequest, "Expected 1 to 256 traces")
	}
	for i := range req.Traces {
		if err := req.Traces[i].Validate(); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		// Classes are assigned here, not trusted from the client, so every
		// source is labelled by the same rules.
		req.Traces[i].Classify()
	}

	workspaceID := cc.AuthInfo.Workspace.ExternalId
	for _, trace := range req.Traces {
		g.eventRepo.PushRouterTrace(workspaceID, trace)
	}
	return ctx.JSON(http.StatusAccepted, PushTracesResponse{Accepted: len(req.Traces)})
}
