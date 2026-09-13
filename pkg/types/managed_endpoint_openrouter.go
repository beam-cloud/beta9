package types

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v2"
	sigyaml "sigs.k8s.io/yaml"
)

// OpenRouterMetadata opts an endpoint into the provider catalog. It lives in
// config.yaml, separately from the SDK/runtime spec, so correcting provider
// metadata neither rebuilds the image nor replaces serving replicas.
// Prices and context length remain authoritative in the endpoint's app.
type OpenRouterMetadata struct {
	HuggingFaceID       string                 `json:"hugging_face_id,omitempty"`
	Quantization        string                 `json:"quantization,omitempty"`
	MaxOutputTokens     uint32                 `json:"max_output_tokens,omitempty"`
	SupportedParameters map[string]any         `json:"supported_parameters,omitempty"`
	Streaming           *bool                  `json:"streaming,omitempty"`
	Datacenters         []OpenRouterDatacenter `json:"datacenters,omitempty"`
}

type OpenRouterDatacenter struct {
	CountryCode string `json:"country_code"`
}

// Decode through JSON to preserve strict scalar types and descriptor maps:
// yaml.v2 otherwise truncates floats into integer fields and coerces strings.
func (m *OpenRouterMetadata) UnmarshalYAML(unmarshal func(any) error) error {
	var raw any
	if err := unmarshal(&raw); err != nil {
		return err
	}
	encoded, err := yaml.Marshal(raw)
	if err != nil {
		return err
	}
	encoded, err = sigyaml.YAMLToJSONStrict(encoded)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return err
	}
	for name, value := range fields {
		if bytes.Equal(value, []byte("null")) {
			return fmt.Errorf("%s cannot be null; omit undeclared fields", name)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	type plain OpenRouterMetadata
	return decoder.Decode((*plain)(m))
}

// Source: https://openrouter.ai/docs/assets/provider-monitor-schema-v2.openapi.json
// Retrieved 2026-09-10, version 2.4. Keep the original schema intact so provider
// validation follows its closed fields and capability descriptors exactly.
//
//go:embed schemas/provider-monitor-schema-v2.openapi.json
var openRouterSchemaJSON []byte

var openRouterSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	var document any
	if err := json.Unmarshal(openRouterSchemaJSON, &document); err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	const location = "https://openrouter.ai/docs/assets/provider-monitor-schema-v2.openapi.json"
	if err := compiler.AddResource(location, document); err != nil {
		return nil, err
	}
	return compiler.Compile(location + "#/components/schemas/ModelDocumentV2")
})

// ValidateOpenRouterDocument validates one model entry, not the OpenAI list
// envelope. Compilation happens once; inference and scheduling never call it.
func ValidateOpenRouterDocument(document any) error {
	schema, err := openRouterSchema()
	if err != nil {
		return err
	}
	// Callers may use typed metadata, uint32 limits, and decimal string prices.
	encoded, err := json.Marshal(document)
	if err != nil {
		return err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	return schema.Validate(value)
}

func (m *OpenRouterMetadata) Validate() error {
	document := map[string]any{
		"schema_version": "2.4", "id": "validation", "name": "validation",
		"input_modalities":  []any{map[string]any{"type": "text"}},
		"output_modalities": []any{m.Output("text")},
	}
	if m.Quantization != "" {
		document["quantization"] = m.Quantization
	}
	if m.Datacenters != nil {
		document["datacenters"] = m.Datacenters
	}
	return ValidateOpenRouterDocument(document)
}

// Output supplies the declared features for the endpoint's single output.
// An empty descriptor map makes no claims about optional feature support.
func (m *OpenRouterMetadata) Output(kind string) map[string]any {
	parameters := m.SupportedParameters
	if parameters == nil {
		parameters = map[string]any{}
	}
	output := map[string]any{"type": kind, "supported_parameters": parameters}
	if m.MaxOutputTokens > 0 {
		output["max_length"] = map[string]any{"value": m.MaxOutputTokens, "unit": "token"}
	}
	if m.Streaming != nil {
		output["streaming"] = *m.Streaming
	}
	return output
}

// ValidateFor checks the metadata against the published app: token pricing
// belongs to text models, embeddings price only their input, and LLM output
// limits must fit the declared context length.
func (m *OpenRouterMetadata) ValidateFor(kind EndpointKind, catalog Catalog, pricing Pricing) error {
	switch kind {
	case EndpointKindLLM:
		if catalog.ContextLength == 0 || m.MaxOutputTokens == 0 || m.MaxOutputTokens > catalog.ContextLength {
			return fmt.Errorf("max_output_tokens must be positive and no greater than context_length")
		}
	case EndpointKindEmbedding, EndpointKindImage:
		if m.MaxOutputTokens != 0 {
			return fmt.Errorf("max_output_tokens is only supported for text outputs")
		}
		if kind == EndpointKindEmbedding && m.Streaming != nil {
			return fmt.Errorf("streaming is not supported for embeddings")
		}
		if kind == EndpointKindEmbedding && pricing.CompletionTokens != "" {
			return fmt.Errorf("embedding token pricing belongs to the input")
		}
		if kind == EndpointKindImage && pricing.PerToken() {
			return fmt.Errorf("image outputs require request pricing, not token pricing")
		}
	default:
		return fmt.Errorf("kind %q has no OpenRouter modality adapter", kind)
	}
	return nil
}
