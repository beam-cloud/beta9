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
		Gpu: []GpuTarget{
			{Type: "H100", Count: 2, MinReplicas: 1, MaxReplicas: 4, Share: 0.3},
			{Type: "A100-80", Count: 4, MaxReplicas: 2},
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
	require.Equal(t, uint32(5), spec.Policy.DrainSeconds)
	require.InDelta(t, 0.2, spec.Policy.SpareShare, 1e-9)
	require.Equal(t, "zai-org/glm-4.5-air", spec.Catalog.Name)

	require.Equal(t, "H100x2", spec.Gpu[0].Key())
	require.InDelta(t, 0.3, spec.Gpu[0].Share, 1e-9)
	require.Equal(t, "A100-80x4", spec.Gpu[1].Key())
	require.InDelta(t, 0.2, spec.Gpu[1].Share, 1e-9)
	require.Equal(t, uint32(2), spec.Gpu[1].MaxReplicas)
	require.NoError(t, spec.Validate(ManagedEndpointValidation{}))

	targets := spec.Targets()
	require.Len(t, targets, 2)
	require.Equal(t, "serve:H100x2", targets[0].Key())
}

func TestManagedEndpointSpecCPUDefault(t *testing.T) {
	spec := ManagedEndpointSpec{ID: "acme/echo", Kind: EndpointKindCustom, Entrypoint: []string{"python", "app.py"}}
	spec.Normalize()
	require.Len(t, spec.Gpu, 1)
	require.True(t, spec.Gpu[0].IsCPU())
	require.Equal(t, "cpu", spec.Gpu[0].Key())
	require.Equal(t, []EndpointRoute{EndpointRouteInvoke}, spec.Routes)
	require.NoError(t, spec.Validate(ManagedEndpointValidation{}))

	// An explicit "cpu" target (what the SDK sends for GpuTarget(type="cpu"))
	// is the same as declaring no GPUs.
	explicit := ManagedEndpointSpec{ID: "acme/echo", Kind: EndpointKindLLM, Engine: "fake", Entrypoint: []string{"python", "app.py"},
		Gpu: []GpuTarget{{Type: "cpu", Count: 1, MinReplicas: 1, MaxReplicas: 3, Share: 1}}}
	explicit.Normalize()
	require.Len(t, explicit.Gpu, 1)
	require.True(t, explicit.Gpu[0].IsCPU())
	require.Equal(t, "cpu", explicit.Gpu[0].Key())
	require.Equal(t, uint32(0), explicit.Gpu[0].Count)
	require.Equal(t, uint32(3), explicit.Gpu[0].MaxReplicas)
	require.NoError(t, explicit.Validate(ManagedEndpointValidation{}))

	// A GPU type with the count omitted is a one-GPU target, not CPU.
	omitted := ManagedEndpointSpec{ID: "acme/llm", Kind: EndpointKindLLM, Engine: "vllm", Entrypoint: []string{"python", "app.py"},
		Gpu: []GpuTarget{{Type: "H100"}}}
	omitted.Normalize()
	require.False(t, omitted.Gpu[0].IsCPU())
	require.Equal(t, "H100x1", omitted.Gpu[0].Key())
}

func TestManagedEndpointSpecValidateErrors(t *testing.T) {
	spec := validEndpointSpec()
	spec.ID = "Bad ID"
	spec.Engine = "sglang"
	spec.Entrypoint = nil
	spec.Routes = []EndpointRoute{EndpointRouteImageEdits}
	spec.Pricing.Request = "1e-3"
	spec.Gpu = append(spec.Gpu, GpuTarget{Type: "H100", Count: 2, MinReplicas: 3, MaxReplicas: 1, Share: 2})
	spec.Gpu = append(spec.Gpu, GpuTarget{Type: "NOTAGPU", Count: 9})
	spec.Gpu = append(spec.Gpu, GpuTarget{Type: "any", Count: 1})
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
		"min_replicas 3 > max_replicas 1",
		"share 2 must be in (0, 1]",
		"duplicate target H100x2",
		"unknown gpu type \"NOTAGPU\"",
		"count 9 exceeds 8",
		"gpu type \"any\" is not allowed",
	} {
		require.Contains(t, msg, want)
	}
}

func TestManagedEndpointSpecDisaggregated(t *testing.T) {
	spec := validEndpointSpec()
	spec.Topology = map[string][]GpuTarget{
		ReplicaRolePrefill: {{Type: "H100", Count: 1, MaxReplicas: 2}},
		ReplicaRoleDecode:  {{Type: "H100", Count: 2, MaxReplicas: 4}},
	}
	spec.Normalize()
	require.ErrorContains(t, spec.Validate(ManagedEndpointValidation{}), "requires kv_cache")

	spec.KVCache = &KVCacheSpec{Connector: "mooncake", Service: "mooncake-master"}
	spec.Normalize()
	require.ErrorContains(t, spec.Validate(ManagedEndpointValidation{KnownServices: map[string]struct{}{}}), "service \"mooncake-master\" is not deployed")
	require.NoError(t, spec.Validate(ManagedEndpointValidation{KnownServices: map[string]struct{}{"mooncake-master": {}}}))
	require.Contains(t, spec.Services, "mooncake-master")

	targets := spec.Targets()
	require.Len(t, targets, 2)
	require.Equal(t, "decode:H100x2", targets[0].Key())
	require.Equal(t, "prefill:H100x1", targets[1].Key())
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

func TestManagedServiceSpec(t *testing.T) {
	spec := ManagedServiceSpec{Name: "mooncake-master", Entrypoint: []string{"mooncake_master"}, Replicas: 2}
	spec.Normalize()
	require.Len(t, spec.Gpu, 1)
	require.Equal(t, uint32(2), spec.Gpu[0].MinReplicas)
	require.Equal(t, uint32(2), spec.Gpu[0].MaxReplicas)
	require.NoError(t, spec.Validate())

	spec.Name = "Bad_Name"
	require.ErrorContains(t, spec.Validate(), "service name")
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
	require.Equal(t, spec.Gpu, decoded.ManagedEndpoint.Endpoint.Gpu)

	require.True(t, StubType(StubTypeManagedEndpointDeployment).IsManagedEndpoint())
	require.True(t, StubType(StubTypeManagedServiceDeployment).IsManaged())
	require.False(t, StubType(StubTypePodDeployment).IsManaged())
}
