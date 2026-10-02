package gatewayservices

import (
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// Object-backed volumes are no place for a database's files, backups included.
func TestPostgresStubStoresOnlyOnItsDurableDisk(t *testing.T) {
	product := databaseProducts[types.DatabaseKindPostgres]
	request := databaseStubRequest(product, databaseSecrets(product, "app-db"), types.CreateDatabaseParams{Name: "app-db", Size: "10Gi"}, "image", "app-db-data-abc")

	require.Empty(t, request.Volumes)
	require.Len(t, request.Disks, 1)
	require.Equal(t, "app-db-data-abc", request.Disks[0].Name)
	require.Equal(t, types.PostgresDataMountPath, request.Disks[0].MountPath)
	require.NotContains(t, managedPostgresScript, "/volumes")
	require.NotContains(t, managedPostgresScript, "pgbackrest")
}

func TestRefreshManagedPostgresDropsLegacyBackupVolumes(t *testing.T) {
	database := func(kind string, entrypoint ...string) *types.StubConfigV1 {
		return &types.StubConfigV1{
			EntryPoint: entrypoint,
			Serving:    &types.ServingConfig{AppKind: "database", Database: &types.DatabaseServingConfig{Kind: kind}},
			Volumes: []*pb.Volume{
				{Id: "own", MountPath: legacyBackupVolumeMount},
				{Id: "source", MountPath: legacyRestoreVolumeMount},
				{Id: "user", MountPath: "models"},
			},
			Env: []string{"BEAM_RESTORE_TIME=2026-10-02 13:04:30+00:00", "TZ=UTC"},
		}
	}

	managed := database(types.DatabaseKindPostgres, "sh", "-lc", "printf %s old | base64 -d > "+postgresScriptPath+"; exec sh "+postgresScriptPath)
	refreshManagedPostgres(managed)
	require.Equal(t, postgresEntrypoint(), managed.EntryPoint)
	require.Equal(t, []*pb.Volume{{Id: "user", MountPath: "models"}}, managed.Volumes)
	require.Equal(t, []string{"TZ=UTC"}, managed.Env)

	// The script assumes its own PGDATA layout, so a stub that never ran it is
	// not converted.
	for _, unmanaged := range []*types.StubConfigV1{
		database(types.DatabaseKindPostgres, "docker-entrypoint.sh", "postgres"),
		database(types.DatabaseKindRedis, "sh", "-lc", "exec redis-server /tmp/redis.conf"),
		{EntryPoint: []string{"sh", "-lc", "exec sh " + postgresScriptPath}},
	} {
		before := strings.Join(unmanaged.EntryPoint, " ")
		volumes := len(unmanaged.Volumes)
		refreshManagedPostgres(unmanaged)
		require.Equal(t, before, strings.Join(unmanaged.EntryPoint, " "))
		require.Len(t, unmanaged.Volumes, volumes)
	}
}

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
		if got := databaseConnectionString(tt.kind, tt.user, "p@ss", "h:443", "db", false); got != tt.want {
			t.Errorf("%s/%s = %q, want %q", tt.kind, tt.user, got, tt.want)
		}
	}
	if got := tcpHostFromURL("https://abc.tcp.example.com"); got != "abc.tcp.example.com:443" {
		t.Errorf("tcp host = %q", got)
	}
}

func TestDatabaseSecretNames(t *testing.T) {
	pg := databaseSecrets(databaseProducts["postgres"], "app-db")
	if pg.URL != "BETA9_POSTGRES_APP_DB_URL" || len(pg.all()) != 5 || len(pg.bound()) != 3 {
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
