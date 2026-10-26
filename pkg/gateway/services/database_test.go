package gatewayservices

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// The probe reaches the database through the gateway's own TCP listener, so a
// host that resolves only outside the cluster can still be checked, by name.
func TestRedisProbeDialsTheListener(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"*.localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	serverNames := make(chan string, 4)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			serverNames <- hello.ServerName
			return nil, nil
		},
	})
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveFakeRedis(conn)
		}
	}()

	connection := url.URL{Scheme: "rediss", User: url.UserPassword("default", "pw"), Host: "cache-abc-latest-6379.localhost:1995"}
	product := databaseProducts[types.DatabaseKindRedis]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, product.probe(ctx, connection, false, ln.Addr().String()))
	require.Equal(t, "cache-abc-latest-6379.localhost", <-serverNames)
}

// serveFakeRedis answers PING, accepts AUTH and CLIENT, and refuses HELLO so
// clients fall back to RESP2.
func serveFakeRedis(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	line := func() string {
		text, _ := reader.ReadString('\n')
		return strings.TrimRight(text, "\r\n")
	}
	for {
		var count int
		if _, err := fmt.Sscanf(line(), "*%d", &count); err != nil || count == 0 {
			return
		}
		var args []string
		for range count {
			line()
			args = append(args, strings.ToUpper(line()))
		}
		reply := "+OK\r\n"
		switch args[0] {
		case "HELLO":
			reply = "-ERR unknown command 'HELLO'\r\n"
		case "PING":
			reply = "+PONG\r\n"
		}
		conn.Write([]byte(reply))
	}
}

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

// New Postgres and Redis databases answer commits up to a second before object
// storage has them unless created with zero loss, and say which they do.
func TestDatabaseStubCommitLag(t *testing.T) {
	info := func(request *pb.GetOrCreateStubRequest) types.DatabaseServiceInfo {
		config, err := json.Marshal(types.StubConfigV1{Serving: servingConfigFromProto(request.Serving)})
		require.NoError(t, err)
		product := databaseProducts[request.Serving.Database.Kind]
		return databaseInfo(product, databaseSecrets(product, "app-db"), &types.DeploymentWithRelated{Stub: types.Stub{Config: string(config)}})
	}
	for _, tc := range []struct {
		kind     string
		zeroLoss bool
		lag      uint32
	}{
		{kind: types.DatabaseKindPostgres, lag: databaseCommitLagMs},
		{kind: types.DatabaseKindPostgres, zeroLoss: true},
		{kind: types.DatabaseKindRedis, lag: databaseCommitLagMs},
		{kind: types.DatabaseKindRedis, zeroLoss: true},
		{kind: "mysql"},
	} {
		product := databaseProducts[tc.kind]
		params := types.CreateDatabaseParams{Name: "app-db", ZeroLoss: tc.zeroLoss}
		request := databaseStubRequest(product, databaseSecrets(product, "app-db"), params, "image", "app-db-data-abc")
		require.Equal(t, tc.lag, request.Serving.Database.CommitLagMs, tc.kind)
		require.Equal(t, tc.zeroLoss, info(request).ZeroLoss, tc.kind)
	}
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

// Listing databases shows each one's TCP gateway address without reading its
// credentials.
func TestDatabaseHost(t *testing.T) {
	gws := &GatewayService{}
	gws.appConfig.Abstractions.Pod.TCP = types.PodTCPConfig{Enabled: true, ExternalHost: "tcp.example.com", ExternalPort: 443}
	d := &types.DeploymentWithRelated{
		Deployment: types.Deployment{Subdomain: "app-db-abc1234"},
		Stub:       types.Stub{Config: `{"tcp":true,"ports":[5432,6432]}`},
	}
	require.Equal(t, "app-db-abc1234-latest-5432.tcp.example.com:443", gws.deploymentTCPHost(d, 5432))

	d.Stub.Config = `{"ports":[5432]}`
	require.Empty(t, gws.deploymentTCPHost(d, 5432))
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
