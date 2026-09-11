package managedendpoint

import (
	"cmp"
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// Provider documents are deliberately separate from the customer catalog.
// OpenRouter's v2 schema is closed; combining it with our OpenAI list metadata
// would invalidate each entry and couple the dashboard to provider onboarding.
func (r *router) handleListOpenRouterModels(ctx echo.Context) error {
	cc := ctx.(*auth.HttpAuthContext)
	rctx := ctx.Request().Context()
	endpoints, err := r.s.repo.ListEndpoints(rctx)
	if err != nil {
		return errRegistry.write(ctx)
	}
	fleet, err := r.s.repo.GetFleet(rctx)
	if err != nil {
		return errRegistry.write(ctx)
	}
	data := make([]map[string]any, 0, len(endpoints))
	for _, endpoint := range endpoints {
		configured := fleet.Endpoints[endpoint.Spec.ID]
		published := endpoint.Enabled() && configured.Enabled && configured.OpenRouter != nil
		if !published || !r.allowed(rctx, endpoint, cc.AuthInfo) {
			continue
		}
		if err := configured.OpenRouter.ValidateFor(&endpoint.Spec); err != nil {
			return errInvalidCatalog.write(ctx)
		}
		document := openRouterModel(endpoint, configured.OpenRouter)
		if err := types.ValidateOpenRouterDocument(document); err != nil {
			return errInvalidCatalog.write(ctx)
		}
		data = append(data, document)
	}
	return ctx.JSON(http.StatusOK, map[string]any{"object": "list", "data": data})
}

func openRouterModel(endpoint *types.ManagedEndpoint, metadata *types.OpenRouterMetadata) map[string]any {
	spec := &endpoint.Spec
	input := map[string]any{"type": "text"}
	if spec.Catalog.ContextLength > 0 {
		input["supported_inputs"] = map[string]any{
			"max_context_length": map[string]any{"value": spec.Catalog.ContextLength, "unit": "token"},
		}
	}
	outputKind := "text"
	switch spec.Kind {
	case types.EndpointKindEmbedding:
		outputKind = "embeddings"
	case types.EndpointKindImage:
		outputKind = "image"
	}
	output := metadata.Output(outputKind)
	var inputPrices, outputPrices, requestPrices []map[string]any
	price := func(kind, unit, cost string) map[string]any {
		return map[string]any{"type": kind, "unit": unit, "cost_usd": cost}
	}
	// Omitted charges are absent SKUs. Explicit zero remains a real free SKU.
	if spec.Pricing.PromptTokens != "" {
		inputPrices = append(inputPrices, price("prompt", "token", spec.Pricing.PromptTokens))
	}
	if spec.Pricing.CachedPromptTokens != "" {
		cached := price("cached_prompt", "token", spec.Pricing.CachedPromptTokens)
		// Beam charges observed engine cache hits; callers do not create or write
		// a cache SKU through this API.
		cached["implicit"] = true
		inputPrices = append(inputPrices, cached)
	}
	if spec.Pricing.CompletionTokens != "" {
		outputPrices = append(outputPrices, price("completion", "token", spec.Pricing.CompletionTokens))
	}
	if spec.Pricing.Image != "" {
		outputPrices = append(outputPrices, price("completion", "image", spec.Pricing.Image))
	}
	if spec.Pricing.Request != "" {
		requestPrices = append(requestPrices, price("request", "request", spec.Pricing.Request))
	}
	if len(inputPrices) > 0 {
		input["pricing"] = inputPrices
	}
	if len(outputPrices) > 0 {
		output["pricing"] = outputPrices
	}
	document := map[string]any{
		"schema_version": "2.4", "id": spec.ID,
		"name": cmp.Or(spec.Catalog.Name, spec.ID), "created": endpoint.CreatedAt.Unix(),
		"description":      spec.Catalog.Description,
		"input_modalities": []any{input}, "output_modalities": []any{output},
	}
	if len(requestPrices) > 0 {
		document["pricing"] = requestPrices
	}
	if metadata.HuggingFaceID != "" {
		document["hugging_face_id"] = metadata.HuggingFaceID
	}
	if metadata.Quantization != "" {
		document["quantization"] = metadata.Quantization
	}
	if len(metadata.Datacenters) > 0 {
		document["datacenters"] = metadata.Datacenters
	}
	// is_ready is launch control in OpenRouter: false hides a live model.
	// Replica availability belongs in fast admission/429 responses, never here.
	return document
}
