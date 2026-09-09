package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func validEndpointSpec() ManagedEndpointSpec {
	return ManagedEndpointSpec{
		ID:         "zai-org/glm-4.5-air",
		Kind:       EndpointKindLLM,
		Engine:     "vllm",
		Entrypoint: []string{"python", "-m", "vllm.entrypoints.openai.api_server"},
		Gpu: map[string]GpuSpec{
			"h100":    {EngineArgs: []string{"--tp", "2"}, Harness: map[string]any{"max_num_seqs": 64}},
			"A100-80": {},
		},
		Pricing: Pricing{PromptTokens: "0.0000002", CompletionTokens: "0.0000011"},
		Catalog: Catalog{Public: true, ContextLength: 131072},
		Harness: true,
	}
}

func TestManagedEndpointSpecNormalizeDefaults(t *testing.T) {
	spec := validEndpointSpec()
	spec.Normalize()

	require.Equal(t, uint32(8000), spec.Port)
	require.Equal(t, "/health", spec.Health)
	require.Equal(t, []EndpointRoute{EndpointRouteChatCompletions, EndpointRouteCompletions}, spec.Routes)
	require.Equal(t, uint32(5), spec.DrainSeconds)
	require.Equal(t, "zai-org/glm-4.5-air", spec.Catalog.Name)

	// GPU keys are canonicalized; the per-GPU spec survives.
	require.Len(t, spec.Gpu, 2)
	require.Contains(t, spec.Gpu, "H100")
	require.Contains(t, spec.Gpu, "A100-80")
	require.Equal(t, []string{"--tp", "2"}, spec.Gpu["H100"].EngineArgs)
	require.NoError(t, spec.Validate(ManagedEndpointValidation{}))
}

func TestManagedEndpointSpecCPUDefault(t *testing.T) {
	spec := ManagedEndpointSpec{ID: "acme/echo", Kind: EndpointKindCustom, Entrypoint: []string{"python", "app.py"}}
	spec.Normalize()
	require.Equal(t, map[string]GpuSpec{CPUInventoryKey: {}}, spec.Gpu)
	require.Equal(t, []EndpointRoute{EndpointRouteInvoke}, spec.Routes)
	require.NoError(t, spec.Validate(ManagedEndpointValidation{}))

	// An explicit "cpu" key (or an empty one) is the same as declaring no GPUs.
	explicit := ManagedEndpointSpec{ID: "acme/echo", Kind: EndpointKindLLM, Engine: "fake", Entrypoint: []string{"python", "app.py"},
		Gpu: map[string]GpuSpec{"CPU": {EngineArgs: []string{"--fake"}}}}
	explicit.Normalize()
	require.Len(t, explicit.Gpu, 1)
	require.Equal(t, []string{"--fake"}, explicit.Gpu[CPUInventoryKey].EngineArgs)
	require.NoError(t, explicit.Validate(ManagedEndpointValidation{}))

	require.Equal(t, "cpu", GPUKey(""))
	require.Equal(t, "cpu", GPUKey(" Cpu "))
	require.Equal(t, "H100", GPUKey("h100"))
}

func TestManagedEndpointSpecValidateErrors(t *testing.T) {
	spec := validEndpointSpec()
	spec.ID = "Bad ID"
	spec.Engine = "sglang"
	spec.Entrypoint = nil
	spec.Routes = []EndpointRoute{EndpointRouteImageEdits}
	spec.Pricing.Request = "1e-3"
	spec.Gpu["NOTAGPU"] = GpuSpec{}
	spec.Normalize()

	err := spec.Validate(ManagedEndpointValidation{AllowedEngines: []string{"vllm"}})
	require.Error(t, err)
	msg := err.Error()
	for _, want := range []string{
		"id \"bad id\"",
		"engine \"sglang\"",
		"entrypoint is required",
		"route \"images/edits\" is not valid for kind \"llm\"",
		"pricing.request",
		"gpu \"NOTAGPU\" is not a known GPU type",
	} {
		require.Contains(t, msg, want)
	}

	image := ManagedEndpointSpec{ID: "acme/img", Kind: EndpointKindImage, Entrypoint: []string{"x"}}
	image.Normalize()
	require.ErrorContains(t, image.Validate(ManagedEndpointValidation{}), "image endpoints must price")
	image.Catalog.Free = true
	require.NoError(t, image.Validate(ManagedEndpointValidation{}))
}

func TestPricingValidateAndRat(t *testing.T) {
	require.NoError(t, Pricing{}.Validate())
	require.True(t, Pricing{}.IsZero())
	require.NoError(t, Pricing{PromptTokens: "0.00000015", Image: "0.04", Request: "0"}.Validate())
	require.Error(t, Pricing{PromptTokens: "-1"}.Validate())
	require.Error(t, Pricing{PromptTokens: ".5"}.Validate())
	require.Error(t, Pricing{PromptTokens: "abc"}.Validate())

	rat, err := PricingRat("0.0000002")
	require.NoError(t, err)
	require.Equal(t, "1/5000000", rat.String())
	zero, err := PricingRat("")
	require.NoError(t, err)
	require.Zero(t, zero.Sign())
}

func TestFleetNormalizeAndPlacements(t *testing.T) {
	fleet := Fleet{Priority: map[string][]FleetEntry{
		"h100": {{EndpointID: " Acme/Model ", Max: 3}, {EndpointID: "acme/other"}},
		"cpu":  {{EndpointID: "acme/model", Max: 1}},
		"A10G": {{EndpointID: "  "}},
	}}
	fleet.Normalize()

	require.Equal(t, map[string][]FleetEntry{
		"H100":          {{EndpointID: "acme/model", Max: 3}, {EndpointID: "acme/other"}},
		CPUInventoryKey: {{EndpointID: "acme/model", Max: 1}},
	}, fleet.Priority, "ids and GPU keys are canonical; blank entries and empty lists are dropped")
	require.Equal(t, []string{"H100", CPUInventoryKey}, fleet.GPUs())

	placements := fleet.Placements("acme/model")
	require.Equal(t, []FleetTarget{{GPU: "H100", Max: 3}, {GPU: CPUInventoryKey, Max: 1}}, placements, "sorted by GPU key")
	require.False(t, placements[0].IsCPU())
	require.True(t, placements[1].IsCPU())
	require.Equal(t, []FleetTarget{{GPU: "H100"}}, fleet.Placements("acme/other"), "no cap reads as 0")
	require.True(t, fleet.Lists("acme/other", "H100"))
	require.False(t, fleet.Lists("acme/other", CPUInventoryKey))
	require.Empty(t, fleet.Placements("acme/none"))
}

func TestFleetValidate(t *testing.T) {
	model := validEndpointSpec()
	model.Normalize()
	endpoints := map[string]*ManagedEndpointSpec{model.ID: &model}

	good := Fleet{Priority: map[string][]FleetEntry{"H100": {{EndpointID: model.ID, Max: 2}}}}
	good.Normalize()
	require.NoError(t, good.Validate())
	require.Empty(t, good.Prune(endpoints))
	require.Equal(t, []FleetTarget{{GPU: "H100", Max: 2}}, good.Placements(model.ID))

	bad := Fleet{Priority: map[string][]FleetEntry{
		"H100":    {{EndpointID: model.ID, Max: 1}, {EndpointID: "acme/typo"}, {EndpointID: model.ID}},
		"A10G":    {{EndpointID: model.ID}},
		"NOTAGPU": {{EndpointID: model.ID, Max: 65}},
		"cpu":     {{EndpointID: "acme/typo"}},
	}}
	bad.Normalize()
	err := bad.Validate()
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "NOTAGPU is not a known GPU type")
	require.Contains(t, msg, "max 65 exceeds 64")
	require.Contains(t, msg, "H100: zai-org/glm-4.5-air is listed twice")
	require.Contains(t, msg, "cpu: acme/typo needs a replica count", "an uncapped cpu entry would start without limit")
	require.NotContains(t, msg, "acme/typo is not", "structural validation does not know about deployed endpoints")

	dropped := bad.Prune(endpoints)
	require.Equal(t, []string{
		"acme/typo is not a deployed endpoint",
		"zai-org/glm-4.5-air does not declare gpu \"A10G\" in its app",
		"zai-org/glm-4.5-air does not declare gpu \"NOTAGPU\" in its app",
	}, dropped, "one message per dropped entry, deduplicated")
	require.Equal(t, map[string][]FleetEntry{"H100": {{EndpointID: model.ID, Max: 1}, {EndpointID: model.ID}}}, bad.Priority, "valid entries survive in order; emptied lists go")
}

func TestDefaultRoutesUnknownKind(t *testing.T) {
	require.Nil(t, defaultRoutes(EndpointKind("nope")))
	require.Equal(t, []EndpointRoute{EndpointRouteChatCompletions, EndpointRouteCompletions}, defaultRoutes(EndpointKindLLM))
	require.Equal(t, []EndpointRoute{EndpointRouteImageGenerations}, defaultRoutes(EndpointKindImage))

	spec := ManagedEndpointSpec{ID: "acme/x", Kind: EndpointKind("nope"), Entrypoint: []string{"x"}}
	spec.Normalize()
	require.Empty(t, spec.Routes)
	require.Error(t, spec.Validate(ManagedEndpointValidation{}))
}

func TestReplicaConfigAckedAndServing(t *testing.T) {
	require.True(t, ReplicaConfig{}.Acked(), "nothing was ever set")
	require.False(t, ReplicaConfig{Revision: 2, AckedRevision: 1}.Acked())
	require.True(t, ReplicaConfig{Revision: 2, AckedRevision: 2}.Acked())

	var none *EndpointReplica
	require.False(t, none.Serving())
	require.True(t, (&EndpointReplica{Status: ReplicaStatusReady}).Serving())
	require.False(t, (&EndpointReplica{Status: ReplicaStatusDraining}).Serving())
	require.True(t, (&EndpointReplica{Status: ReplicaStatusLoading}).Alive())
	require.False(t, (&EndpointReplica{Status: ReplicaStatusDraining}).Alive())
	require.True(t, ReplicaStatusEvicted.Terminal())
	require.False(t, ReplicaStatusEvicting.Terminal())
}

func TestStubConfigCarriesManagedEndpoint(t *testing.T) {
	spec := validEndpointSpec()
	spec.Normalize()
	config := StubConfigV1{ManagedEndpoint: &ManagedEndpointStubConfig{Endpoint: &spec, GitSHA: "abc123"}}
	raw, err := json.Marshal(config)
	require.NoError(t, err)

	var decoded StubConfigV1
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.NotNil(t, decoded.ManagedEndpoint)
	require.Equal(t, "abc123", decoded.ManagedEndpoint.GitSHA)
	require.Equal(t, spec.ID, decoded.ManagedEndpoint.Endpoint.ID)
	require.Equal(t, spec.Gpu["H100"].EngineArgs, decoded.ManagedEndpoint.Endpoint.Gpu["H100"].EngineArgs)

	require.True(t, StubType(StubTypeManagedEndpointDeployment).IsManagedEndpoint())
	require.False(t, StubType(StubTypePodDeployment).IsManagedEndpoint())
}
