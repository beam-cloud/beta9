package gatewayservices

import "testing"

func TestDatabaseConnectionString(t *testing.T) {
	tests := []struct {
		kind, user, want string
	}{
		{"postgres", "u", "postgresql://u:p%40ss@h:443/db?sslmode=require"},
		{"mysql", "u", "mysql://u:p%40ss@h:443/db?ssl-mode=REQUIRED"},
		{"mongo", "u", "mongodb://u:p%40ss@h:443/db?tls=true&tlsAllowInvalidCertificates=true&authSource=admin"},
		{"redis", "default", "rediss://:p%40ss@h:443/0?ssl_cert_reqs=none"},
		{"redis", "bob", "rediss://bob:p%40ss@h:443/0?ssl_cert_reqs=none"},
	}
	for _, tt := range tests {
		if got := databaseConnectionString(tt.kind, tt.user, "p@ss", "h:443", "db"); got != tt.want {
			t.Errorf("%s/%s = %q, want %q", tt.kind, tt.user, got, tt.want)
		}
	}
	if got := tcpHostFromURL("https://abc.tcp.example.com"); got != "abc.tcp.example.com:443" {
		t.Errorf("tcp host = %q", got)
	}
}

func TestDatabaseSecretNames(t *testing.T) {
	pg := databaseSecrets(databaseProducts["postgres"], "app-db")
	if pg.URL != "BETA9_POSTGRES_APP_DB_URL" || len(pg.all()) != 4 || len(pg.bound()) != 3 {
		t.Fatalf("postgres names: %+v", pg)
	}
	rd := databaseSecrets(databaseProducts["redis"], "cache")
	if rd.Database != "" || len(rd.all()) != 3 {
		t.Fatalf("redis names: %+v", rd)
	}
	for name, ok := range map[string]bool{"app-db": true, "A": false, "1abc": false, "has_underscore": false} {
		if err := validateDatabaseName(name); (err == nil) != ok {
			t.Errorf("%q: err=%v want ok=%v", name, err, ok)
		}
	}
}
