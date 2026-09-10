package managedendpoint

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testOpenRouterMetadata() *types.OpenRouterMetadata {
	streaming := true
	return &types.OpenRouterMetadata{
		HuggingFaceID: "Qwen/Qwen3-8B", Quantization: "bf16", MaxOutputTokens: 32768, Streaming: &streaming,
		SupportedParameters: map[string]any{"max_tokens": map[string]any{"type": "integer", "min": 1, "max": 32768, "unit": "token"}},
	}
}

func TestOpenRouterCatalogSchemaAndPricing(t *testing.T) {
	metadata := testOpenRouterMetadata()
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{
		ID: "qwen/qwen3-8b", Kind: types.EndpointKindLLM,
		Catalog: types.Catalog{Name: "Qwen3 8B", Description: "Qwen", ContextLength: 32768},
		Pricing: types.Pricing{PromptTokens: "0.0000001", CompletionTokens: "0.0000003", CachedPromptTokens: "0.000000025"},
	}}
	require.NoError(t, metadata.Validate())
	require.NoError(t, metadata.ValidateFor(&endpoint.Spec))
	doc := openRouterModel(endpoint, metadata)
	require.NoError(t, types.ValidateOpenRouterDocument(doc))
	assert.Equal(t, "Qwen/Qwen3-8B", doc["hugging_face_id"])
	assert.Equal(t, "bf16", doc["quantization"])
	input := doc["input_modalities"].([]any)[0].(map[string]any)
	assert.Equal(t, []map[string]any{
		{"type": "prompt", "unit": "token", "cost_usd": "0.0000001"},
		{"type": "cached_prompt", "unit": "token", "cost_usd": "0.000000025", "implicit": true},
	}, input["pricing"])
	output := doc["output_modalities"].([]any)[0].(map[string]any)
	assert.Equal(t, []map[string]any{{"type": "completion", "unit": "token", "cost_usd": "0.0000003"}}, output["pricing"])
	for _, field := range []string{"is_ready", "is_free", "datacenters", "deployment_region", "compliance", "capacity", "pricing", "kind", "serverless", "owned_by", "endpoints"} {
		assert.NotContains(t, doc, field)
	}
	endpoint.Spec.Pricing = types.Pricing{PromptTokens: "0", CompletionTokens: "0"}
	doc = openRouterModel(endpoint, metadata)
	require.NoError(t, types.ValidateOpenRouterDocument(doc))
	input = doc["input_modalities"].([]any)[0].(map[string]any)
	assert.Equal(t, []map[string]any{{"type": "prompt", "unit": "token", "cost_usd": "0"}}, input["pricing"], "explicit zero is preserved; missing cache has no SKU")
}

func TestOpenRouterCatalogImageAndEmbedding(t *testing.T) {
	for _, tc := range []struct {
		kind    types.EndpointKind
		pricing types.Pricing
		output  string
	}{
		{types.EndpointKindImage, types.Pricing{Image: "0.03", Request: "0.001"}, "image"},
		{types.EndpointKindEmbedding, types.Pricing{PromptTokens: "0.0000001"}, "embeddings"},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			metadata := &types.OpenRouterMetadata{Datacenters: []types.OpenRouterDatacenter{{CountryCode: "US"}}}
			endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model", Kind: tc.kind, Pricing: tc.pricing}}
			require.NoError(t, metadata.Validate())
			require.NoError(t, metadata.ValidateFor(&endpoint.Spec))
			doc := openRouterModel(endpoint, metadata)
			require.NoError(t, types.ValidateOpenRouterDocument(doc))
			output := doc["output_modalities"].([]any)[0].(map[string]any)
			assert.Equal(t, tc.output, output["type"])
			assert.Equal(t, metadata.Datacenters, doc["datacenters"], "only explicitly configured countries are exposed")
			if tc.kind == types.EndpointKindImage {
				assert.Equal(t, []map[string]any{{"type": "request", "unit": "request", "cost_usd": "0.001"}}, doc["pricing"])
				assert.Equal(t, []map[string]any{{"type": "completion", "unit": "image", "cost_usd": "0.03"}}, output["pricing"])
			}
		})
	}
}

func TestOpenRouterCatalogPublicationDoesNotFollowReplicaReadiness(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Spec.Catalog.ContextLength = 32768
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	entry := fleet.Endpoints[endpoint.Spec.ID]
	entry.OpenRouter = testOpenRouterMetadata()
	fleet.Endpoints[endpoint.Spec.ID] = entry
	require.NoError(t, s.repo.SaveFleet(context.Background(), fleet))
	get := func() []any {
		t.Helper()
		rec := httptest.NewRecorder()
		ctx := &auth.HttpAuthContext{Context: echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/v1/models/openrouter", nil), rec),
			AuthInfo: &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "buyer"}, Token: &types.Token{}},
		}
		require.NoError(t, newRouter(s).handleListOpenRouterModels(ctx))
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		return body["data"].([]any)
	}
	cold := get()
	require.Len(t, cold, 1, "configured published model remains listed with zero replicas")
	assert.NotContains(t, cold[0], "is_ready")
	replica := seedReplica(t, s, endpoint)
	replica.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	assert.Equal(t, cold, get(), "ready replicas do not change provider publication")
	endpoint.Spec.Public = false
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	assert.Empty(t, get(), "workspace access still controls discovery")
	endpoint.Spec.Public = true
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	entry.Enabled = false
	fleet.Endpoints[endpoint.Spec.ID] = entry
	require.NoError(t, s.repo.SaveFleet(context.Background(), fleet))
	assert.Empty(t, get(), "disabled config is absent")
	entry.Enabled, entry.OpenRouter = true, nil
	fleet.Endpoints[endpoint.Spec.ID] = entry
	require.NoError(t, s.repo.SaveFleet(context.Background(), fleet))
	assert.Empty(t, get(), "provider publication is opt-in")
}

func TestOpenRouterConfigMetadataChangesPreserveReplicaAndPlacement(t *testing.T) {
	s := newServiceForTest(t)
	endpoint := seedEndpoint(t, s)
	endpoint.Spec.Catalog.ContextLength = 32768
	require.NoError(t, s.repo.SaveEndpoint(context.Background(), endpoint))
	replica := seedReplica(t, s, endpoint)
	replica.Status = types.ReplicaStatusReady
	require.NoError(t, s.repo.SaveReplica(context.Background(), replica))
	fleet, err := s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	before := fleet.Placements(endpoint.Spec.ID)
	beforeEndpoint, err := s.repo.GetEndpoint(context.Background(), endpoint.Spec.ID)
	require.NoError(t, err)
	beforeReplica, err := s.repo.GetReplica(context.Background(), replica.ID)
	require.NoError(t, err)
	g := &gitops{s: s}
	state := &types.GitOpsState{}
	report := &types.GitOpsReport{SHA: "metadata-only", FleetYAML: `acme/model:
  enabled: true
  gpus:
    H100: {priority: 1, maxReplicas: 2}
  openrouter:
    hugging_face_id: Qwen/Qwen3-8B
    quantization: bf16
    max_output_tokens: 32768
    streaming: true
    supported_parameters:
      max_tokens: {type: integer, min: 1, max: 32768, unit: token}
`}
	require.True(t, g.applyFleet(context.Background(), state, report), state.FleetError)
	fleet, err = s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, before, fleet.Placements(endpoint.Spec.ID))
	stored, err := s.repo.GetEndpoint(context.Background(), endpoint.Spec.ID)
	require.NoError(t, err)
	assert.Equal(t, beforeEndpoint, stored)
	storedReplica, err := s.repo.GetReplica(context.Background(), replica.ID)
	require.NoError(t, err)
	assert.Equal(t, beforeReplica, storedReplica)
	report.SHA, report.FleetYAML = "invalid-metadata", report.FleetYAML+"    unexpected: true\n"
	assert.False(t, g.applyFleet(context.Background(), state, report))
	fleet, err = s.repo.GetFleet(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "metadata-only", fleet.GitSHA, "invalid metadata preserves the applied config")
}

func TestOpenRouterConfigStrictMetadata(t *testing.T) {
	for _, metadata := range []string{
		"{quantization: guessed}",
		"{hugging_face_id: true}",
		"{max_output_tokens: 1.5}",
		"{max_output_tokens: true}",
		"{max_output_tokens: '32768'}",
		"{max_output_tokens: -1}",
		"{streaming: 'true'}",
		"{streaming: null}",
		"{supported_parameters: null}",
		"{capacity: [{type: concurrency, unit: request, value: 1}]}",
		"{compliance: {zdr: true}}",
		"{datacenters: [{country_code: us}]}",
		"{datacenters: [{country_code: US, region: unknown}]}",
		"{supported_parameters: {tools: true}}",
		"{supported_parameters: {temperature: {type: guessed}}}",
		"{supported_parameters: {max_tokens: {type: integer, max: .nan}}}",
		"{supported_parameters: {max_tokens: {type: integer, max: '32'}}}",
		"{supported_parameters: {tools: {type: array, max_items: 1.5}}}",
		"{supported_parameters: {tools: {type: array, unknown: true}}}",
		"{supported_parameters: {tools: {type: array, type: boolean}}}",
	} {
		t.Run(metadata, func(t *testing.T) {
			_, err := parseFleet("acme/model:\n  enabled: true\n  gpus: {H100: {priority: 1}}\n  openrouter: " + metadata)
			require.Error(t, err)
		})
	}
	for _, metadata := range []string{
		"{}",
		"{hugging_face_id: Qwen/Qwen3-8B, quantization: bf16, max_output_tokens: 32768, streaming: true}",
		"{datacenters: [{country_code: US}]}",
		"{supported_parameters: {tools: {type: array, max_items: 128, items: {type: object, properties: {}}}}}",
	} {
		t.Run(metadata, func(t *testing.T) {
			fleet, err := parseFleet("acme/model:\n  enabled: true\n  gpus: {H100: {priority: 1}}\n  openrouter: " + metadata)
			require.NoError(t, err)
			require.NotNil(t, fleet.Endpoints["acme/model"].OpenRouter)
		})
	}
}

func TestOpenRouterMetadataChecksActualModelLimits(t *testing.T) {
	for _, tc := range []struct {
		kind            types.EndpointKind
		context, output uint32
		pricing         types.Pricing
	}{
		{kind: types.EndpointKindLLM, context: 32768, output: 65536},
		{kind: types.EndpointKindLLM, context: 0, output: 32768},
		{kind: types.EndpointKindLLM, context: 32768, output: 0},
		{kind: types.EndpointKindLLM, context: 32768, output: 32768, pricing: types.Pricing{Image: "0.01"}},
		{kind: types.EndpointKindImage, output: 32},
		{kind: types.EndpointKindImage, pricing: types.Pricing{CompletionTokens: "0.01"}},
		{kind: types.EndpointKindEmbedding, pricing: types.Pricing{CompletionTokens: "0.01"}},
		{kind: types.EndpointKindCustom},
	} {
		metadata := &types.OpenRouterMetadata{MaxOutputTokens: tc.output}
		spec := &types.ManagedEndpointSpec{Kind: tc.kind, Catalog: types.Catalog{ContextLength: tc.context}, Pricing: tc.pricing}
		require.Error(t, metadata.ValidateFor(spec), "%+v", tc)
	}
	metadata := testOpenRouterMetadata()
	for _, routes := range [][]types.EndpointRoute{{types.EndpointRouteCompletions}, {types.EndpointRouteImageEdits}} {
		spec := &types.ManagedEndpointSpec{Kind: types.EndpointKindLLM, Catalog: types.Catalog{ContextLength: 32768}, Routes: routes}
		require.ErrorContains(t, metadata.ValidateFor(spec), "chat/completions")
	}
}

func TestOpenRouterRenderedSchemaRejectsInvalidModalityFields(t *testing.T) {
	metadata := testOpenRouterMetadata()
	endpoint := &types.ManagedEndpoint{Spec: types.ManagedEndpointSpec{ID: "acme/model", Kind: types.EndpointKindEmbedding}}
	// The descriptor itself is valid as text, but embeddings cannot expose
	// streaming or a text output length. Validate the actual generated document.
	require.NoError(t, metadata.Validate())
	require.Error(t, types.ValidateOpenRouterDocument(openRouterModel(endpoint, metadata)))
	endpoint.Spec.Kind = types.EndpointKindImage
	require.Error(t, types.ValidateOpenRouterDocument(openRouterModel(endpoint, metadata)))
	metadata.MaxOutputTokens = 0
	require.NoError(t, types.ValidateOpenRouterDocument(openRouterModel(endpoint, metadata)))
	metadata.SupportedParameters["resolution"] = map[string]any{"type": "enum", "values": []string{"1K", "2K"}, "unknown": true}
	require.Error(t, types.ValidateOpenRouterDocument(openRouterModel(endpoint, metadata)))
}
