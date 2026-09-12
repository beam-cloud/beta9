package managedendpoint

import (
	"cmp"
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// handleListOpenRouterModels publishes the provider catalog: OpenRouter's v2
// schema is closed, so it stays separate from the OpenAI list.
func (r *router) handleListOpenRouterModels(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	apps, err := r.visible(ctx.Request().Context(), cc.AuthInfo)
	if err != nil {
		return errRegistry.write(ctx)
	}
	data := make([]map[string]any, 0, len(apps))
	for _, app := range apps {
		if app.OpenRouter == nil {
			continue
		}
		document := openRouterModel(app)
		if app.OpenRouter.ValidateFor(app.Spec.Kind, app.Catalog, app.Pricing) != nil || types.ValidateOpenRouterDocument(document) != nil {
			return errInvalidCatalog.write(ctx)
		}
		data = append(data, document)
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

// openRouterOutputs maps engine kinds to OpenRouter output modalities; the rest are text.
var openRouterOutputs = map[types.EndpointKind]string{types.EndpointKindEmbedding: "embeddings", types.EndpointKindImage: "image"}

func openRouterModel(app *types.ManagedEndpoint) map[string]any {
	m := app.OpenRouter
	input := map[string]any{"type": "text"}
	if app.Catalog.ContextLength > 0 {
		input["supported_inputs"] = map[string]any{"max_context_length": map[string]any{"value": app.Catalog.ContextLength, "unit": "token"}}
	}
	output := m.Output(cmp.Or(openRouterOutputs[app.Spec.Kind], "text"))
	price := func(kind, unit, cost string) map[string]any {
		return map[string]any{"type": kind, "unit": unit, "cost_usd": cost}
	}
	// Omitted charges are absent SKUs; an explicit zero is a real free SKU.
	var inputPrices, outputPrices []map[string]any
	if p := app.Pricing; p.PerToken() {
		inputPrices = append(inputPrices, price("prompt", "token", p.PromptTokens))
		if p.CachedPromptTokens != "" {
			cached := price("cached_prompt", "token", p.CachedPromptTokens)
			cached["implicit"] = true // Beam charges observed engine cache hits
			inputPrices = append(inputPrices, cached)
		}
		if p.CompletionTokens != "" {
			outputPrices = append(outputPrices, price("completion", "token", p.CompletionTokens))
		}
	}
	if len(inputPrices) > 0 {
		input["pricing"] = inputPrices
	}
	if len(outputPrices) > 0 {
		output["pricing"] = outputPrices
	}
	document := map[string]any{
		"schema_version": "2.4", "id": app.Spec.ID, "name": app.Catalog.Name, "created": app.CreatedAt.Unix(), "description": app.Catalog.Description,
		"input_modalities": []any{input}, "output_modalities": []any{output},
	}
	if app.Pricing.PerRequest() {
		document["pricing"] = []map[string]any{price("request", "request", app.Pricing.Request)}
	}
	if m.HuggingFaceID != "" {
		document["hugging_face_id"] = m.HuggingFaceID
	}
	if m.Quantization != "" {
		document["quantization"] = m.Quantization
	}
	if len(m.Datacenters) > 0 {
		document["datacenters"] = m.Datacenters
	}
	return document
}
