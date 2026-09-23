package pod

import "testing"

func TestPodPathPrefix(t *testing.T) {
	tests := []struct{ uri, subPath, want string }{
		{"/pod/app/latest/8080", "/", "/pod/app/latest/8080"},
		{"/pod/app/latest/8080/", "/", "/pod/app/latest/8080"},
		{"/pod/app/latest/8080/login?next=1", "/login", "/pod/app/latest/8080"},
		{"/login", "/login", ""}, // subdomain-routed: raw path already starts at subPath
		{"/", "/", ""},
	}
	for _, tt := range tests {
		if got := podPathPrefix(tt.uri, tt.subPath); got != tt.want {
			t.Errorf("podPathPrefix(%q, %q) = %q, want %q", tt.uri, tt.subPath, got, tt.want)
		}
	}
}

func TestRewriteRedirect(t *testing.T) {
	const prefix, backend = "/pod/app/latest/8080", "10.0.0.5:8080"
	tests := []struct{ location, want string }{
		{"/dashboard", prefix + "/dashboard"},
		{"/dashboard?x=1", prefix + "/dashboard?x=1"},
		{prefix + "/already", prefix + "/already"},
		{"http://10.0.0.5:8080/login", prefix + "/login"},
		{"https://accounts.google.com/o/oauth2", "https://accounts.google.com/o/oauth2"},
		{"//cdn.example.com/x", "//cdn.example.com/x"},
		{"relative", "relative"},
	}
	for _, tt := range tests {
		if got := rewriteRedirect(tt.location, prefix, backend); got != tt.want {
			t.Errorf("rewriteRedirect(%q) = %q, want %q", tt.location, got, tt.want)
		}
	}
	if got := rewriteRedirect("/dashboard", "", backend); got != "/dashboard" {
		t.Errorf("no prefix should leave Location alone, got %q", got)
	}
}
