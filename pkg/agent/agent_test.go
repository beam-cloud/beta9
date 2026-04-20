package agent

import (
	"reflect"
	"testing"
)

func TestRedactSensitiveKeys(t *testing.T) {
	in := map[string]any{
		"machine_id":  "abc123",
		"k3s_token":   "K10::plaintextadmin",
		"auth_token":  "bearer-abc",
		"token":       "plain-token",
		"password":    "hunter2",
		"api_key":     "sk-live-xxx",
		"apiKey":      "sk-camel-xxx",
		"API-Key":     "sk-dash-xxx",
		"k3s_secret":  "k3s-secret-val",
		"gateway_url": "https://example.com",
		"nested": map[string]any{
			"inner_token": "deep-secret",
			"benign":      "hello",
		},
		"list": []any{
			map[string]any{"password": "p1", "name": "ok"},
			"leave-me-alone",
		},
	}

	got := redactSensitive(in)

	sensitive := []string{"k3s_token", "auth_token", "token", "password", "api_key", "apiKey", "API-Key", "k3s_secret"}
	for _, k := range sensitive {
		if got[k] != "[REDACTED]" {
			t.Errorf("key %q: expected [REDACTED], got %v", k, got[k])
		}
	}

	// Benign keys are preserved verbatim.
	if got["machine_id"] != "abc123" {
		t.Errorf("machine_id was rewritten: %v", got["machine_id"])
	}
	if got["gateway_url"] != "https://example.com" {
		t.Errorf("gateway_url was rewritten: %v", got["gateway_url"])
	}

	// Nested map: inner_token redacted, benign preserved.
	nested, ok := got["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested key lost its map shape: %T", got["nested"])
	}
	if nested["inner_token"] != "[REDACTED]" {
		t.Errorf("nested.inner_token not redacted: %v", nested["inner_token"])
	}
	if nested["benign"] != "hello" {
		t.Errorf("nested.benign was rewritten: %v", nested["benign"])
	}

	// Slice of maps: password redacted, name preserved.
	list, ok := got["list"].([]any)
	if !ok {
		t.Fatalf("list key lost its slice shape: %T", got["list"])
	}
	inner, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("list[0] lost its map shape: %T", list[0])
	}
	if inner["password"] != "[REDACTED]" {
		t.Errorf("list[0].password not redacted: %v", inner["password"])
	}
	if inner["name"] != "ok" {
		t.Errorf("list[0].name was rewritten: %v", inner["name"])
	}
	if list[1] != "leave-me-alone" {
		t.Errorf("list[1] was rewritten: %v", list[1])
	}

	// Ensure the input was not mutated in place.
	if in["k3s_token"] != "K10::plaintextadmin" {
		t.Errorf("redactSensitive mutated its input")
	}
}

func TestRedactSensitiveNil(t *testing.T) {
	if got := redactSensitive(nil); got != nil {
		t.Errorf("redactSensitive(nil) = %v, want nil", got)
	}
}

func TestRedactSensitiveEmpty(t *testing.T) {
	got := redactSensitive(map[string]any{})
	want := map[string]any{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("redactSensitive({}) = %v, want %v", got, want)
	}
}

func TestValidateConfigRejectsPlainHTTPWithK3sToken(t *testing.T) {
	cfg := &AgentConfig{
		Token:         "t",
		MachineID:     "abcd1234",
		PoolName:      "external",
		GatewayHost:   "gw",
		GatewayPort:   1994,
		GatewayScheme: "http",
		K3sToken:      "K10::admin",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject http+k3s_token, got nil")
	}
}

func TestValidateConfigAcceptsHTTPSWithK3sToken(t *testing.T) {
	cfg := &AgentConfig{
		Token:         "t",
		MachineID:     "abcd1234",
		PoolName:      "external",
		GatewayHost:   "gw",
		GatewayPort:   1994,
		GatewayScheme: "https",
		K3sToken:      "K10::admin",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected https+k3s_token to validate, got %v", err)
	}
}

func TestValidateConfigAcceptsHTTPWithoutK3sToken(t *testing.T) {
	cfg := &AgentConfig{
		Token:         "t",
		MachineID:     "abcd1234",
		PoolName:      "external",
		GatewayHost:   "gw",
		GatewayPort:   1994,
		GatewayScheme: "http",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected http without k3s_token to validate (back-compat), got %v", err)
	}
}

func TestToAgentConfigKeepsHTTPWhenNoK3sToken(t *testing.T) {
	cfg := &ConfigFile{
		Gateway: GatewayConfig{Host: "gw", Port: 1994},
		Machine: MachineConfig{ID: "abcd1234", Token: "t"},
		Pool:    "external",
	}
	if got := cfg.ToAgentConfig().GatewayScheme; got != "http" {
		t.Errorf("expected http without k3s token, got %s", got)
	}
}
