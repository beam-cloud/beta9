package common

import "testing"

func TestShellQuote(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: "''"},
		{name: "simple", in: "value", want: "'value'"},
		{name: "single quote", in: "token'with'quote", want: `'token'\''with'\''quote'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ShellQuote(tt.in); got != tt.want {
				t.Fatalf("ShellQuote() = %s, want %s", got, tt.want)
			}
		})
	}
}
