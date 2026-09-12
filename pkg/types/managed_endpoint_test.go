package types

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"math"
	"testing"
	"time"
)

func TestPublicationAllows(t *testing.T) {
	private := Publication{AllowedWorkspaces: []string{"ws-allowed", "partner"}}
	assert.True(t, private.Allows("ws-allowed", "x"))
	assert.True(t, private.Allows("x", "partner"), "workspaces may be listed by name")
	assert.False(t, private.Allows("ws-other", "other"))
	assert.False(t, (&Publication{}).Allows("ws-other", "other"), "unpublished defaults to nobody")
	assert.True(t, (&Publication{Public: true}).Allows("anyone", ""))
}

func TestHostedSpecValidate(t *testing.T) {
	runner := ManagedEndpointSpec{ID: "Acme/Video "}
	runner.Normalize(false)
	require.NoError(t, runner.Validate(false))
	assert.Equal(t, "acme/video", runner.ID)
	assert.Empty(t, runner.Kind, "ordinary deployments carry only an id")

	for _, id := range []string{"", "acme/", "/model", "acme/model/extra", "acme model", "Acme/Model!"} {
		spec := ManagedEndpointSpec{ID: id}
		spec.Normalize(false)
		assert.Error(t, spec.Validate(false), id)
	}

	model := ManagedEndpointSpec{ID: "acme/model", Entrypoint: []string{"vllm", "serve"}, Gpu: map[string]GpuSpec{"h100": {}}}
	model.Normalize(true)
	require.NoError(t, model.Validate(true))
	assert.Equal(t, EndpointKindCustom, model.Kind)
	assert.EqualValues(t, 8000, model.Port)
	assert.Equal(t, "/health", model.Health)
	assert.Contains(t, model.Gpu, "H100", "GPU keys are canonical")

	for name, broken := range map[string]func(*ManagedEndpointSpec){
		"unknown kind":      func(s *ManagedEndpointSpec) { s.Kind = "video" },
		"no entrypoint":     func(s *ManagedEndpointSpec) { s.Entrypoint = nil },
		"unknown gpu":       func(s *ManagedEndpointSpec) { s.Gpu = map[string]GpuSpec{"TPU": {}} },
		"too many gpus":     func(s *ManagedEndpointSpec) { s.Gpu = map[string]GpuSpec{"H100": {Count: 9}} },
		"bad port":          func(s *ManagedEndpointSpec) { s.Port = 70000 },
		"bad rollout":       func(s *ManagedEndpointSpec) { s.Rollout = "yolo" },
		"publication field": nil,
	} {
		t.Run(name, func(t *testing.T) {
			if broken == nil {
				var spec ManagedEndpointSpec
				assert.NoError(t, yaml.Unmarshal([]byte("id: acme/model\n"), &spec))
				return
			}
			spec := model
			broken(&spec)
			spec.Normalize(true)
			assert.Error(t, spec.Validate(true))
		})
	}
}

func TestFleetParsesStrictlyAndValidates(t *testing.T) {
	const doc = `
Acme/Model:
  enabled: true
  catalog: {name: Model, description: A model, context_length: 32768}
  public: true
  pricing: {prompt_tokens: "0.000001", completion_tokens: "0.000002"}
  gpus:
    h100: {priority: 1, minReplicas: 1, maxReplicas: 4, preemption: false}
    A100-80: {priority: 2, serverless: true, maxReplicas: 2}
acme/free:
  enabled: false
  pricing: {}
  gpus:
    cpu: {priority: 1, maxReplicas: 1}
`
	var fleet Fleet
	require.NoError(t, yaml.UnmarshalStrict([]byte(doc), &fleet.Endpoints))
	fleet.Normalize()
	require.NoError(t, fleet.Validate())

	model := fleet.Endpoints["acme/model"]
	assert.True(t, model.Enabled)
	assert.True(t, model.Public)
	assert.Equal(t, "0.000001", model.Pricing.PromptTokens)
	assert.EqualValues(t, 32768, model.Catalog.ContextLength)
	assert.True(t, model.GPUs["H100"].ProtectsMinimum(), "preemption: false protects the minimum")
	assert.False(t, model.GPUs["A100-80"].ProtectsMinimum(), "serverless placements are never protected")
	assert.True(t, fleet.Serverless("acme/model"))
	assert.Equal(t, []string{"A100-80", "H100"}, fleet.GPUs(), "disabled apps place nothing")
	entries := fleet.Entries("H100")
	require.Len(t, entries, 1)
	assert.Equal(t, FleetEntry{EndpointID: "acme/model", Priority: 1, MinReplicas: 1, MaxReplicas: 4, ProtectMinimum: true}, entries[0])
	assert.Equal(t, "acme/free", fleet.Endpoints["acme/free"].Catalog.Name, "catalog name defaults to the id")
	assert.False(t, fleet.Endpoints["acme/free"].Enabled, "a disabled app needs no pricing")

	for name, tc := range map[string]string{
		"missing pricing":        "acme/m: {enabled: true, gpus: {H100: {priority: 1}}}",
		"mixed pricing":          `acme/m: {enabled: true, pricing: {request: "1", prompt_tokens: "1"}, gpus: {H100: {priority: 1}}}`,
		"image pricing":          `acme/m: {enabled: true, pricing: {image: "0.01"}, gpus: {H100: {priority: 1}}}`,
		"negative price":         `acme/m: {enabled: true, pricing: {request: "-1"}, gpus: {H100: {priority: 1}}}`,
		"unknown gpu":            `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {TPU: {priority: 1}}}`,
		"no priority":            `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {}}}`,
		"serverless minimum":     `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {priority: 1, serverless: true, minReplicas: 1}}}`,
		"serverless protected":   `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {priority: 1, serverless: true, preemption: false}}}`,
		"min above max":          `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {priority: 1, minReplicas: 3, maxReplicas: 2}}}`,
		"above fleet cap":        `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {priority: 1, maxReplicas: 65}}}`,
		"cpu uncapped":           `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {cpu: {priority: 1}}}`,
		"fractional replicas":    `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {priority: 1, maxReplicas: 1.5}}}`,
		"string boolean":         `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {priority: 1, preemption: "false"}}}`,
		"unknown placement key":  `acme/m: {enabled: true, pricing: {request: "0"}, gpus: {H100: {priority: 1, replicas: 2}}}`,
		"unknown endpoint key":   `acme/m: {enabled: true, pricing: {request: "0"}, harness: true, gpus: {H100: {priority: 1}}}`,
		"openrouter bad country": `acme/m: {enabled: true, pricing: {request: "0"}, openrouter: {datacenters: [{country_code: USA}]}, gpus: {H100: {priority: 1}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var fleet Fleet
			err := yaml.UnmarshalStrict([]byte(tc), &fleet.Endpoints)
			if err == nil {
				fleet.Normalize()
				err = fleet.Validate()
			}
			assert.Error(t, err)
		})
	}
}

func TestFleetPruneKeepsOnlyDeployedPlacements(t *testing.T) {
	fleet := Fleet{Endpoints: map[string]FleetEndpoint{
		"acme/model": {Enabled: true, GPUs: map[string]FleetPlacement{"H100": {Priority: 1}, "A100-80": {Priority: 2}}},
		"acme/gone":  {Enabled: true, GPUs: map[string]FleetPlacement{"H100": {Priority: 1}}},
	}}
	apps := map[string]*ManagedEndpoint{"acme/model": {Status: EndpointStatusActive, Spec: ManagedEndpointSpec{ID: "acme/model", Gpu: map[string]GpuSpec{"H100": {}}}}}
	dropped := fleet.Prune(apps)
	assert.NotEmpty(t, dropped)
	assert.NotContains(t, fleet.Endpoints, "acme/gone", "an app that is not deployed is not placed")
	assert.NotContains(t, fleet.Endpoints["acme/model"].GPUs, "A100-80", "a GPU the app does not declare is not placed")
	assert.Contains(t, fleet.Endpoints["acme/model"].GPUs, "H100")
}

func TestOpenRouterMetadataValidateFor(t *testing.T) {
	tokens := Pricing{PromptTokens: "0.000001", CompletionTokens: "0.000002"}
	catalog := Catalog{ContextLength: 8192}
	llm := &OpenRouterMetadata{MaxOutputTokens: 4096}
	require.NoError(t, llm.ValidateFor("llm", catalog, tokens))
	assert.Error(t, (&OpenRouterMetadata{MaxOutputTokens: 9000}).ValidateFor("llm", catalog, tokens), "output cannot exceed context")
	assert.Error(t, (&OpenRouterMetadata{}).ValidateFor("llm", catalog, tokens), "LLMs must declare an output limit")
	assert.Error(t, llm.ValidateFor("embedding", catalog, tokens), "embeddings have no output tokens")
	assert.Error(t, (&OpenRouterMetadata{}).ValidateFor("embedding", catalog, tokens), "embeddings price only their input")
	require.NoError(t, (&OpenRouterMetadata{}).ValidateFor("embedding", catalog, Pricing{PromptTokens: "0.000001"}))
	assert.Error(t, (&OpenRouterMetadata{}).ValidateFor("image", catalog, tokens), "images are priced per request")
	require.NoError(t, (&OpenRouterMetadata{}).ValidateFor("image", catalog, Pricing{Request: "0.01"}))
	assert.Error(t, (&OpenRouterMetadata{}).ValidateFor(EndpointKindCustom, catalog, tokens))
}

func TestPricingHasExactlyTwoForms(t *testing.T) {
	require.NoError(t, Pricing{Request: "0.05"}.Validate())
	require.NoError(t, Pricing{PromptTokens: "0.0000001", CompletionTokens: "0.0000003"}.Validate())
	require.NoError(t, Pricing{PromptTokens: "0", CompletionTokens: "0", CachedPromptTokens: "0"}.Validate())
	for name, p := range map[string]Pricing{
		"empty":                   {},
		"mixed":                   {Request: "0.05", PromptTokens: "0.0000001", CompletionTokens: "0.0000003"},
		"request with cache":      {Request: "0.05", CachedPromptTokens: "0"},
		"prompt without output":   {PromptTokens: "0.0000001"},
		"negative":                {Request: "-1"},
		"not a decimal":           {Request: "five cents"},
		"scientific notation":     {Request: "5e-2"},
		"legacy per-image (json)": {Request: ""},
	} {
		require.Error(t, p.Validate(), name)
	}
	free := Pricing{Request: "0"}
	require.True(t, free.Free() && free.PerRequest() && !free.PerToken())
	tokens := Pricing{PromptTokens: "0", CompletionTokens: "0.0000003"}
	require.False(t, tokens.Free(), "any non-zero rate is billable")
	require.True(t, tokens.PerToken())
}

func TestPricePriceIsExactAndItemized(t *testing.T) {
	p := Pricing{PromptTokens: "0.0000001", CompletionTokens: "0.0000003", CachedPromptTokens: "0.000000025"}
	cost, err := p.Price(Work{Requests: 1, PromptTokens: 1000, CachedTokens: 800, CompletionTokens: 100})
	require.NoError(t, err)
	require.Equal(t, Cost{MicroUSD: 70, PromptMicroUSD: 20, CachedMicroUSD: 20, CompletionMicroUSD: 30}, cost)

	p.CachedPromptTokens = ""
	cost, err = p.Price(Work{Requests: 1, PromptTokens: 1000, CachedTokens: 800, CompletionTokens: 100})
	require.NoError(t, err)
	require.EqualValues(t, 80, cost.CachedMicroUSD, "without a discount, cached input is still itemized at the prompt rate")
	require.EqualValues(t, 130, cost.MicroUSD)

	flat, err := Pricing{Request: "0.05"}.Price(Work{Requests: 1, PromptTokens: 99})
	require.NoError(t, err)
	require.Equal(t, Cost{MicroUSD: 50_000, RequestMicroUSD: 50_000}, flat, "flat pricing ignores tokens")
}

// Rounding happens once on the exact total; the components are then
// reconciled so they always sum to it.
func TestPriceRoundingReconcilesComponents(t *testing.T) {
	p := Pricing{PromptTokens: "0.0000001", CompletionTokens: "0.0000003", CachedPromptTokens: "0.000000025"}
	for prompt := int64(0); prompt < 50; prompt++ {
		for cached := int64(0); cached <= prompt; cached++ {
			cost, err := p.Price(Work{Requests: 1, PromptTokens: prompt, CachedTokens: cached, CompletionTokens: 7})
			require.NoError(t, err)
			require.Equal(t, cost.MicroUSD, cost.PromptMicroUSD+cost.CachedMicroUSD+cost.CompletionMicroUSD+cost.RequestMicroUSD)
			// Rates in units of 1/40 micro-USD make the independently rounded
			// total an exact integer calculation, including half-way cases.
			units := (prompt-cached)*4 + cached + 7*12
			require.Equal(t, (units+20)/40, cost.MicroUSD)
		}
	}
}

func TestPriceRejectsInvalidWorkAndOverflow(t *testing.T) {
	for _, w := range []Work{{PromptTokens: -1}, {CompletionTokens: -1}, {PromptTokens: 1, CachedTokens: 2}, {CachedTokens: -1}, {PromptTokens: MaxUsageCounter + 1}} {
		_, err := Pricing{PromptTokens: "0", CompletionTokens: "0"}.Price(w)
		require.Error(t, err)
	}
	_, err := Pricing{PromptTokens: "1", CompletionTokens: "0"}.Price(Work{PromptTokens: math.MaxInt64})
	require.Error(t, err)
	_, err = Pricing{Request: "9007199255"}.Price(Work{Requests: 1})
	require.Error(t, err, "a charge must fit the usage counters before it can be journaled")
}

func TestChargeSettlesOnceFromReportedWork(t *testing.T) {
	now := time.Now()
	c := Charge{ID: "gen-1", Status: ChargeOpen, Pricing: Pricing{PromptTokens: "0.000001", CompletionTokens: "0.000002"}}
	require.NoError(t, c.Settle(Work{PromptTokens: 10, CompletionTokens: 5}, now))
	require.Equal(t, ChargeSettled, c.Status)
	require.Equal(t, Work{Requests: 1, PromptTokens: 10, CompletionTokens: 5}, c.Work)
	require.EqualValues(t, 20, c.Cost.MicroUSD)
	require.Equal(t, Usage{Work: c.Work, Cost: c.Cost}, c.Usage())

	task := Charge{ID: "task-1", Status: ChargeOpen, Pricing: Pricing{Request: "0.05"}}
	require.NoError(t, task.Settle(Work{PromptTokens: 123}, now))
	require.Equal(t, Work{Requests: 1}, task.Work, "flat work is one request regardless of what the app reported")
	require.EqualValues(t, 50_000, task.Cost.MicroUSD)

	failed := Charge{ID: "gen-2", Status: ChargeOpen, Pricing: Pricing{Request: "0.05"}}
	failed.Void("upstream 502", now)
	require.Equal(t, ChargeVoid, failed.Status)
	require.Zero(t, failed.Cost.MicroUSD)
	require.Zero(t, failed.Work.Requests, "unsuccessful work is never counted")
}

func TestUsageAddsFieldwise(t *testing.T) {
	var total Usage
	total.Add(Usage{Work: Work{Requests: 1, PromptTokens: 5}, Cost: Cost{MicroUSD: 7, PromptMicroUSD: 7}})
	total.Add(Usage{Work: Work{Requests: 1, CachedTokens: 2}, Cost: Cost{MicroUSD: 3, CachedMicroUSD: 3}})
	require.Equal(t, Usage{Work: Work{Requests: 2, PromptTokens: 5, CachedTokens: 2}, Cost: Cost{MicroUSD: 10, PromptMicroUSD: 7, CachedMicroUSD: 3}}, total)
	require.Len(t, UsageFieldNames, len(total.Fields()), "wire names and counters stay in step")
}
