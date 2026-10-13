package mcp

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
)

type stackSpec struct {
	AppIds    []string                   `json:"appIds"`
	Positions map[string]json.RawMessage `json:"positions,omitempty"`
	Pending   json.RawMessage            `json:"pending,omitempty"`
}

// appNames maps app ids to names and back, once per call.
func (g *Group) appNames(ctx context.Context, ws *types.Workspace) (byID, byName map[string]string, err error) {
	byID, byName = map[string]string{}, map[string]string{}
	cursor := ""
	for {
		page, err := g.backendRepo.ListAppsPaginated(ctx, ws.Id, types.AppFilter{Limit: 1000, Cursor: cursor})
		if err != nil {
			return nil, nil, err
		}
		for _, app := range page.Data {
			byID[app.ExternalId] = app.Name
			byName[app.Name] = app.ExternalId
		}
		if page.Next == "" || page.Next == cursor {
			break
		}
		cursor = page.Next
	}
	return byID, byName, nil
}

func stackView(s *types.Stack, byID map[string]string) map[string]any {
	var spec stackSpec
	_ = json.Unmarshal(s.Spec, &spec)
	apps := make([]string, 0, len(spec.AppIds))
	for _, id := range spec.AppIds {
		if name, ok := byID[id]; ok {
			apps = append(apps, name)
		}
	}
	return map[string]any{
		"name":     s.Name,
		"id":       s.ExternalId,
		"apps":     apps,
		"spec":     json.RawMessage(s.Spec),
		"revision": fmt.Sprintf("%x", sha256.Sum256(s.Spec)),
	}
}

func (g *Group) stack(ctx context.Context, ws *types.Workspace, name string) (*types.Stack, error) {
	stacks, err := g.backendRepo.ListStacks(ctx, ws.Id)
	if err != nil {
		return nil, err
	}
	for i := range stacks {
		if stacks[i].Name == name {
			return &stacks[i], nil
		}
	}
	return nil, fail("NOT_FOUND", "no stack named %q", name)
}

func (g *Group) listStacks(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	stacks, err := g.backendRepo.ListStacks(ctx, a.Workspace.Id)
	if err != nil {
		return nil, err
	}
	byID, _, err := g.appNames(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(stacks))
	for i := range stacks {
		out = append(out, stackView(&stacks[i], byID))
	}
	return out, nil
}

func resolveApps(byName map[string]string, names []string) ([]string, error) {
	ids := make([]string, 0, len(names))
	for _, n := range names {
		id, ok := byName[n]
		if !ok {
			return nil, fail("NOT_FOUND", "no app named %q", n)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (g *Group) createStack(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	byID, byName, err := g.appNames(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	ids, err := resolveApps(byName, args.strings("apps"))
	if err != nil {
		return nil, err
	}
	spec, _ := json.Marshal(stackSpec{AppIds: ids})
	s, err := g.backendRepo.CreateStack(ctx, a.Workspace.Id, args.str("name"), spec)
	if err != nil {
		return nil, err
	}
	return stackView(s, byID), nil
}

func (g *Group) updateStack(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	s, err := g.stack(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, err
	}
	if expected := args.str("expected_revision"); expected != "" && expected != fmt.Sprintf("%x", sha256.Sum256(s.Spec)) {
		return nil, fail("STALE_PLAN", "stack changed; reload it before applying")
	}
	byID, byName, err := g.appNames(ctx, a.Workspace)
	if err != nil {
		return nil, err
	}
	add, err := resolveApps(byName, args.strings("add"))
	if err != nil {
		return nil, err
	}
	remove, err := resolveApps(byName, args.strings("remove"))
	if err != nil {
		return nil, err
	}
	var spec stackSpec
	_ = json.Unmarshal(s.Spec, &spec)
	drop := map[string]bool{}
	for _, id := range remove {
		drop[id] = true
	}
	keep := map[string]bool{}
	ids := make([]string, 0, len(spec.AppIds)+len(add))
	for _, id := range append(spec.AppIds, add...) {
		if !drop[id] && !keep[id] {
			ids = append(ids, id)
			keep[id] = true
		}
	}
	spec.AppIds = ids
	for id := range spec.Positions {
		if !keep[id] {
			delete(spec.Positions, id)
		}
	}
	// Preserve fields managed by the dashboard or newer clients.
	full := map[string]any{}
	_ = json.Unmarshal(s.Spec, &full)
	if patch, ok := args["spec"].(map[string]any); ok {
		for key, value := range patch {
			full[key] = value
		}
	}
	full["appIds"] = ids
	full["positions"] = spec.Positions
	raw, err := json.Marshal(full)
	if err != nil {
		return nil, err
	}
	if len(raw) > types.StackSpecMaxBytes {
		return nil, fail("INVALID_ARGS", "stack spec exceeds 256 KiB")
	}
	writer, ok := g.backendRepo.(interface {
		UpdateStackIfUnchanged(context.Context, uint, string, string, json.RawMessage, json.RawMessage) (*types.Stack, error)
	})
	if !ok {
		return nil, fail("UNSUPPORTED", "atomic stack updates unavailable")
	}
	updated, err := writer.UpdateStackIfUnchanged(ctx, a.Workspace.Id, s.ExternalId, s.Name, s.Spec, raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fail("STALE_PLAN", "stack changed during update; reload before retrying: %s", err)
	}
	if err != nil {
		return nil, err
	}
	return stackView(updated, byID), nil
}

func (g *Group) deleteStack(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	s, err := g.stack(ctx, a.Workspace, args.str("name"))
	if err != nil {
		return nil, err
	}
	if err := g.backendRepo.DeleteStack(ctx, a.Workspace.Id, s.ExternalId); err != nil {
		return nil, err
	}
	return map[string]any{"deleted": s.Name}, nil
}
