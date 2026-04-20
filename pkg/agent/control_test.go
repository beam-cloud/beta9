package agent

import (
	"testing"
)

func TestModelNameAllowlist(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// valid inputs
		{"simple name", "llama3", false},
		{"name with tag", "llama3:latest", false},
		{"name with version tag", "qwen2.5:7b", false},
		{"dots and dashes", "gemma-2.5b-it", false},
		{"underscores", "my_model:v1", false},
		{"digits only", "123:456", false},

		// invalid inputs — security-relevant
		{"empty string", "", true},
		{"registry path", "registry.example.com/evil/model:latest", true},
		{"just a slash", "foo/bar", true},
		{"parent traversal", "..", true},
		{"parent traversal in tag", "llama:..", true},
		{"embedded traversal", "a..b", true},
		{"uppercase rejected", "Llama3", true},
		{"spaces rejected", "llama 3", true},
		{"colon only", ":", true},
		{"leading colon", ":tag", true},
		{"trailing colon", "model:", true},
		{"multiple colons", "a:b:c", true},
		{"null byte", "llama\x00", true},
		{"newline", "llama\n", true},
		{"query string", "llama?foo=bar", true},
		{"too long", string(make([]byte, 200)), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateOllamaModelName(tc.input)
			if tc.wantErr && err == nil {
				t.Errorf("validateOllamaModelName(%q) = nil, want error", tc.input)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validateOllamaModelName(%q) = %v, want nil", tc.input, err)
			}
		})
	}
}

func TestMaxInferencePullBodyBytesConstant(t *testing.T) {
	// Guard-rail: this test exists so that if someone bumps the cap,
	// they think about it. 4 KiB is plenty for {"model":"<128 chars>"}.
	if maxInferencePullBodyBytes != 4096 {
		t.Errorf("maxInferencePullBodyBytes changed to %d; review the security implications", maxInferencePullBodyBytes)
	}
}
