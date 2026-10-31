package mcp

import (
	"context"
	"encoding/json"
	"net/http"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
)

func (g *Group) feedbackTool() mcpTool {
	return mcpTool{
		Name: "send_feedback",
		Description: "Tell the Beam team about friction with Beam itself, then continue the task: sending does not end it. " +
			"Send one report when a tool or command fails in a way that looks like Beam's fault, you work around a " +
			"missing capability, an error, doc, skill or tool description misled you, or something worked notably well. " +
			"Not for the user's own errors (credits, quotas, missing resources, bugs in their code). Never include " +
			"secrets, environment values or the user's data.",
		Schema: schema(props{
			"summary":               str("One sentence, at most 300 characters"),
			"kind":                  map[string]any{"type": "string", "enum": types.FeedbackKinds},
			"blocked":               map[string]any{"type": "boolean", "description": "It stopped the task"},
			"command":               str("Tool or command involved"),
			"error_code":            str("Code it returned"),
			"error_message":         str("Error text, trimmed to what matters"),
			"details":               str("What you tried and expected, briefly"),
			"suggested_improvement": str("What would have helped"),
			"model":                 str("Your model id; omit when unsure"),
			"idempotency_key":       str("Reuse on a retry to get the first report back"),
		}, "summary", "kind"),
		Category:   "core",
		Scope:      "beam:read",
		Unfiltered: true,
		Run:        g.sendFeedback,
	}
}

// sendFeedback goes through the REST route, which validates, scrubs and rate limits reports.
func (g *Group) sendFeedback(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	body, err := json.Marshal(apiv1.FeedbackRequest{
		Kind:                 args.str("kind"),
		Summary:              args.str("summary"),
		Blocked:              args.boolean("blocked"),
		Command:              args.str("command"),
		ErrorCode:            args.str("error_code"),
		ErrorMessage:         args.rawString("error_message"),
		Details:              args.rawString("details"),
		SuggestedImprovement: args.rawString("suggested_improvement"),
		Model:                args.str("model"),
		IdempotencyKey:       args.str("idempotency_key"),
	})
	if err != nil {
		return nil, err
	}

	url := g.config.GatewayService.HTTP.GetExternalURL() + apiv1.HttpServerBaseRoute + "/feedback/" + a.Workspace.ExternalId
	res, err := g.serve(ctx, http.MethodPost, url, body, nil)
	if err != nil {
		return nil, err
	}
	out := res.(map[string]any)
	reply, _ := out["body"].(map[string]any)
	status, _ := out["status"].(int)
	switch {
	case status == http.StatusTooManyRequests:
		return nil, fail("RATE_LIMITED", "%v", reply["message"])
	case status == http.StatusBadRequest || status == http.StatusRequestEntityTooLarge:
		return nil, fail("INVALID_ARGS", "%v", reply["message"])
	case status >= 300 || reply == nil:
		return nil, fail("ERROR", "send feedback: HTTP %d", status)
	}
	return reply, nil
}
