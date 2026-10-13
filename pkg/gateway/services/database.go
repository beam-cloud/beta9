package gatewayservices

import (
	"context"
	"crypto/tls"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

//go:embed templates/postgres.sh
var managedPostgresScript string

// A managed database service is a pod deployment of the upstream image on a
// durable disk, reached over TLS through the TCP gateway, with credentials in
// workspace secrets.

const (
	databaseKeepWarmSeconds = 300
	databaseDefaultCpu      = 1000 // millicores
	databaseDefaultMemory   = 512  // MB
	databasePythonVersion   = "python3.10"
	databaseDiskFilesystem  = "ext4"

	postgresScriptPath   = "/tmp/beam-postgres"
	databaseProbeTimeout = 5 * time.Second
	databaseDiskSuffix   = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// Object-backed volumes older managed Postgres stubs mount for pgBackRest: the
// database's own backups and, after a restore, its source's. Nothing reads
// them any more.
const (
	legacyBackupVolumeMount  = "beam-backups"
	legacyRestoreVolumeMount = "beam-restore"
)

type databaseProduct struct {
	Kind              string
	Image             string
	BuildCommands     []string
	SecretEnv         []string // positional names for username, password and database
	PoolPort          uint32
	Port              uint32
	MountPath         string
	DefaultSize       string
	ReadinessProbe    string
	ConnectionEnvName string
	DurabilityMode    string
	HasDatabase       bool // a named database next to user/password (not Redis)
	Entrypoint        string
}

func (p databaseProduct) probe(ctx context.Context, connection url.URL, verifyTLS bool) error {
	switch p.Kind {
	case types.DatabaseKindPostgres:
		mode := "require"
		if verifyTLS {
			mode = "verify-full"
		}
		// lib/pq uses system roots when sslrootcert is empty.
		connection.RawQuery = url.Values{
			"sslmode": {mode}, "sslrootcert": {""},
			"connect_timeout": {strconv.Itoa(int(databaseProbeTimeout.Seconds()))},
		}.Encode()
		connector, err := pq.NewConnector(connection.String())
		if err != nil {
			return errors.New("invalid Postgres connection configuration")
		}
		client := sql.OpenDB(connector)
		defer client.Close()
		var result int
		return client.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	case types.DatabaseKindRedis:
		password, _ := connection.User.Password()
		client := redis.NewClient(&redis.Options{
			Addr:                  connection.Host,
			Username:              connection.User.Username(),
			Password:              password,
			DialTimeout:           databaseProbeTimeout,
			ReadTimeout:           databaseProbeTimeout,
			WriteTimeout:          databaseProbeTimeout,
			ContextTimeoutEnabled: true,
			MaxRetries:            -1,
			TLSConfig: &tls.Config{
				ServerName:         connection.Hostname(),
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: !verifyTLS, // Only development gateways use self-signed certificates.
			},
		})
		defer client.Close()
		return client.Ping(ctx).Err()
	default:
		return errors.New("readiness supports Postgres and Redis")
	}
}

// CheckDatabaseReadiness authenticates through the public endpoint without creating a task.
func (gws *GatewayService) CheckDatabaseReadiness(ctx context.Context, authInfo *auth.AuthInfo, name, deploymentID string) (*types.DatabaseReadiness, error) {
	ctx, cancel := context.WithTimeout(ctx, databaseProbeTimeout)
	defer cancel()
	deployments, product, err := gws.databaseDeployments(ctx, authInfo.Workspace, name)
	if err != nil {
		return nil, err
	}
	deployment := newestDeployment(deployments)
	if product.Kind != types.DatabaseKindPostgres && product.Kind != types.DatabaseKindRedis {
		return nil, errors.New("readiness supports Postgres and Redis")
	}
	if deploymentID != "" && deployment.ExternalId != deploymentID {
		return nil, errors.New("database revision changed; refresh the deployment before checking readiness")
	}
	result := &types.DatabaseReadiness{DeploymentID: deployment.ExternalId}
	if !deployment.Active {
		result.Error = "database is stopped; start it before checking readiness"
		return result, nil
	}
	value, err := gws.SecretValue(ctx, authInfo.Workspace, databaseSecrets(product, name).URL)
	if err != nil {
		return nil, err
	}
	connection, err := url.Parse(value)
	if err != nil || connection.User == nil {
		return nil, errors.New("invalid stored database connection URL")
	}
	host, err := gws.databasePortHost(ctx, deployment.Stub.ExternalId, deployment.ExternalId, product.Port)
	if err != nil {
		return nil, err
	}
	pinnedHost, err := gws.databasePortHost(ctx, deployment.Stub.ExternalId, "", product.Port)
	if err != nil {
		return nil, err
	}
	// A workspace can edit its secrets; never probe an arbitrary destination.
	if connection.Host != host && connection.Host != pinnedHost {
		return nil, errors.New("stored database URL does not match the deployment endpoint")
	}
	connection.Host = pinnedHost
	verifyTLS := gws.appConfig.Abstractions.Pod.TCP.CertFile != ""
	if err := product.probe(ctx, *connection, verifyTLS); err != nil {
		result.Error = err.Error()
		if password, _ := connection.User.Password(); password != "" {
			result.Error = strings.ReplaceAll(result.Error, password, "[redacted]")
		}
	} else {
		result.Ready = true
		result.TLSVerified = verifyTLS
	}
	return result, nil
}

var databaseProducts = map[string]databaseProduct{
	types.DatabaseKindPostgres: {
		Kind:              types.DatabaseKindPostgres,
		Image:             "docker.io/library/postgres:16",
		BuildCommands:     []string{"apt-get update && apt-get install -y --no-install-recommends pgbouncer && rm -rf /var/lib/apt/lists/*"},
		SecretEnv:         []string{"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"},
		Port:              5432,
		PoolPort:          6432,
		MountPath:         types.PostgresDataMountPath,
		DefaultSize:       "10Gi",
		ReadinessProbe:    "pg_isready",
		ConnectionEnvName: "DATABASE_URL",
		DurabilityMode:    "object-store-flush",
		HasDatabase:       true,
	},
	types.DatabaseKindRedis: {
		Kind:              types.DatabaseKindRedis,
		Image:             "docker.io/library/redis:7",
		Port:              6379,
		MountPath:         "/data",
		DefaultSize:       "5Gi",
		ReadinessProbe:    "PING",
		ConnectionEnvName: "REDIS_URL",
		DurabilityMode:    "object-store-flush",
		Entrypoint:        redisEntrypoint,
	},
	"mysql": {
		Kind:              "mysql",
		Image:             "docker.io/library/mysql:8",
		Port:              3306,
		MountPath:         "/var/lib/mysql",
		DefaultSize:       "10Gi",
		ReadinessProbe:    "mysqladmin ping",
		ConnectionEnvName: "DATABASE_URL",
		DurabilityMode:    "snapshot",
		HasDatabase:       true,
		Entrypoint:        mysqlEntrypoint,
	},
	"mongo": {
		Kind:              "mongo",
		Image:             "docker.io/library/mongo:7",
		Port:              27017,
		MountPath:         "/data/db",
		DefaultSize:       "10Gi",
		ReadinessProbe:    "mongosh --eval db.runCommand('ping')",
		ConnectionEnvName: "DATABASE_URL",
		DurabilityMode:    "snapshot",
		HasDatabase:       true,
		Entrypoint:        mongoEntrypoint,
	},
}

// Entrypoints read bound secrets and apply credential changes on restart.
// Postgres uses the embedded lifecycle script; the other products substitute
// secret names into the upstream image's startup command.
const (
	// Redis 7 replays AOF transactions using the disabled default user's permissions.
	redisEntrypoint = `if [ "${USER_SECRET}" = "default" ]; then
  printf 'appendonly yes\nappendfsync always\ndir /data\nuser default on >%s ~* &* +@all\n' "${PASSWORD_SECRET}" > /tmp/redis.conf;
else
  printf 'appendonly yes\nappendfsync always\ndir /data\nuser default off ~* &* +@all\nuser %s on >%s ~* &* +@all\n' "${USER_SECRET}" "${PASSWORD_SECRET}" > /tmp/redis.conf;
fi;
printf 'maxmemory %s\nmaxmemory-policy noeviction\n' "$BETA9_REDIS_MAXMEMORY_BYTES" >> /tmp/redis.conf;
exec redis-server /tmp/redis.conf`

	mysqlEntrypoint = `export MYSQL_USER="${USER_SECRET}" MYSQL_PASSWORD="${PASSWORD_SECRET}" MYSQL_DATABASE="${DATABASE_SECRET}" MYSQL_ROOT_PASSWORD="${PASSWORD_SECRET}";
if [ -d /var/lib/mysql/mysql ]; then
  (docker-entrypoint.sh mysqld --skip-networking --socket=/tmp/rotate.sock >/dev/null 2>&1 &);
  for i in $(seq 1 60); do mysqladmin --socket=/tmp/rotate.sock -uroot -p"$MYSQL_ROOT_PASSWORD" ping >/dev/null 2>&1 && break; sleep 1; done;
  mysql --socket=/tmp/rotate.sock -uroot -p"$MYSQL_ROOT_PASSWORD" -e "ALTER USER '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';" >/dev/null 2>&1 || true;
  mysqladmin --socket=/tmp/rotate.sock -uroot -p"$MYSQL_ROOT_PASSWORD" shutdown >/dev/null 2>&1 || true;
fi;
exec docker-entrypoint.sh mysqld --require-secure-transport=OFF`

	mongoEntrypoint = `export MONGO_INITDB_ROOT_USERNAME="${USER_SECRET}" MONGO_INITDB_ROOT_PASSWORD="${PASSWORD_SECRET}" MONGO_INITDB_DATABASE="${DATABASE_SECRET}";
if [ -f /data/db/WiredTiger ]; then
  (mongod --dbpath /data/db --bind_ip 127.0.0.1 --port 27099 --fork --logpath /tmp/rotate.log >/dev/null 2>&1);
  for i in $(seq 1 60); do mongosh --quiet --port 27099 --eval 'db.runCommand({ping:1})' >/dev/null 2>&1 && break; sleep 1; done;
  mongosh --quiet --port 27099 admin --eval "db.changeUserPassword('$MONGO_INITDB_ROOT_USERNAME', '$MONGO_INITDB_ROOT_PASSWORD')" >/dev/null 2>&1 || true;
  mongosh --quiet --port 27099 admin --eval 'db.shutdownServer()' >/dev/null 2>&1 || true; sleep 1;
fi;
exec docker-entrypoint.sh mongod --auth --bind_ip_all`
)

// databaseSecretNames: BETA9_<KIND>_<NAME>_{USERNAME,PASSWORD,DATABASE,URL}.
type databaseSecretNames struct {
	Username, Password, Database, URL, PooledURL string
}

func databaseSecrets(product databaseProduct, name string) databaseSecretNames {
	prefix := databaseSecretPrefix(product.Kind, name)
	names := databaseSecretNames{
		Username: prefix + "_USERNAME",
		Password: prefix + "_PASSWORD",
		URL:      prefix + "_URL",
	}
	if product.HasDatabase {
		names.Database = prefix + "_DATABASE"
	}
	if product.PoolPort != 0 {
		names.PooledURL = prefix + "_POOLED_URL"
	}
	return names
}

// bound excludes the URL, written after deploy once the host is known.
func (n databaseSecretNames) all() []string   { return append(n.bound(), compact(n.URL, n.PooledURL)...) }
func (n databaseSecretNames) bound() []string { return compact(n.Username, n.Password, n.Database) }

func (n databaseSecretNames) entrypoint(product databaseProduct) []string {
	if product.Kind == types.DatabaseKindPostgres {
		return postgresEntrypoint()
	}
	script := strings.NewReplacer(
		"USER_SECRET", n.Username,
		"PASSWORD_SECRET", n.Password,
		"DATABASE_SECRET", n.Database,
	).Replace(product.Entrypoint)
	return []string{"sh", "-lc", script}
}

func postgresEntrypoint() []string {
	script := base64.StdEncoding.EncodeToString([]byte(managedPostgresScript))
	command := fmt.Sprintf("printf %%s %s | base64 -d > %s; exec sh %s", script, postgresScriptPath, postgresScriptPath)
	return []string{"sh", "-lc", command}
}

// refreshManagedPostgres moves a stub that runs the managed lifecycle script
// to the current one. The script is embedded in each stub, so a database keeps
// the version it was created with until its stub is rebuilt.
func refreshManagedPostgres(config *types.StubConfigV1) {
	database := config.EffectiveDatabaseConfig()
	if database == nil || types.NormalizeDatabaseKind(database.Kind) != types.DatabaseKindPostgres ||
		!strings.Contains(strings.Join(config.EntryPoint, " "), postgresScriptPath) {
		return
	}
	config.EntryPoint = postgresEntrypoint()
	config.Volumes = slices.DeleteFunc(config.Volumes, func(volume *pb.Volume) bool {
		return volume != nil && (volume.MountPath == legacyBackupVolumeMount || volume.MountPath == legacyRestoreVolumeMount)
	})
	config.Env = slices.DeleteFunc(config.Env, func(entry string) bool {
		return strings.HasPrefix(entry, "BEAM_RESTORE_TIME=")
	})
}

// CreateDatabaseService provisions a database. The password is only returned here.
func (gws *GatewayService) CreateDatabaseService(ctx context.Context, authInfo *auth.AuthInfo, p types.CreateDatabaseParams) (*types.DatabaseServiceInfo, error) {
	product, ok := databaseProducts[types.NormalizeDatabaseKind(p.Kind)]
	if !ok {
		return nil, types.ErrDatabaseKind
	}
	if err := validateDatabaseName(p.Name); err != nil {
		return nil, err
	}
	if err := gws.scheduler.CreditGate().Check(ctx, authInfo.Workspace.ExternalId); err != nil {
		return nil, err
	}
	if existing, err := gws.deploymentsByName(ctx, authInfo.Workspace, p.Name); err != nil {
		return nil, err
	} else if len(existing) > 0 {
		return nil, types.ErrDatabaseExists
	}
	if p.SnapshotID != "" {
		if err := gws.prepareDatabaseRestore(ctx, authInfo.Workspace, product, &p); err != nil {
			return nil, err
		}
	}

	identifier := strings.ReplaceAll(p.Name, "-", "_")
	if p.Username == "" {
		p.Username = identifier
	}
	if product.HasDatabase && p.Database == "" {
		p.Database = identifier
	}
	if !databaseIdentifier.MatchString(p.Username) || (product.HasDatabase && !databaseIdentifier.MatchString(p.Database)) {
		return nil, errors.New("username and database must be identifiers of 1–63 letters, digits or underscores, starting with a letter or underscore")
	}
	if p.Password == "" {
		password, err := randomString(32, defaultSecretAlphabet)
		if err != nil {
			return nil, err
		}
		p.Password = password
	}
	if p.Size == "" {
		p.Size = product.DefaultSize
	}
	if p.Cpu <= 0 {
		p.Cpu = databaseDefaultCpu
	}
	if p.Memory <= 0 {
		p.Memory = databaseDefaultMemory
	}

	names := databaseSecrets(product, p.Name)
	credentials := map[string]string{names.Username: p.Username, names.Password: p.Password, names.Database: p.Database}
	for _, secret := range names.bound() {
		if err := gws.upsertSecret(ctx, authInfo, secret, credentials[secret]); err != nil {
			return nil, err
		}
	}

	imageId, err := gws.ensureRegistryImage(ctx, product.Image, product.BuildCommands)
	if err != nil {
		return nil, err
	}
	// A disk's snapshots, journal and worker caches are keyed by its name, so a
	// database created under a deleted one's name must not reuse its disk name.
	suffix, err := randomString(8, databaseDiskSuffix)
	if err != nil {
		return nil, err
	}
	request := databaseStubRequest(product, names, p, imageId, p.Name+"-data-"+suffix)
	stubRes, err := gws.GetOrCreateStub(ctx, request)
	if err != nil {
		return nil, err
	}
	if !stubRes.Ok {
		return nil, errors.New(stubRes.ErrMsg)
	}

	deployRes, err := gws.DeployStub(ctx, &pb.DeployStubRequest{StubId: stubRes.StubId, Name: p.Name})
	if err != nil {
		return nil, err
	}
	if !deployRes.Ok {
		return nil, errors.New(deployRes.ErrMsg)
	}

	deployment, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, deployRes.DeploymentId)
	if err != nil {
		return nil, fmt.Errorf("read deployment: %w", err)
	}
	info := databaseInfo(product, names, deployment)
	if err := gws.setDatabaseConnections(ctx, authInfo, &info, p.Username, p.Password, p.Database); err != nil {
		return nil, err
	}
	return &info, nil
}

// databaseStubRequest is the pod stub a database runs as: one TCP container on a durable disk.
func databaseStubRequest(product databaseProduct, names databaseSecretNames, p types.CreateDatabaseParams, imageId, diskName string) *pb.GetOrCreateStubRequest {
	minContainers := uint32(0)
	if p.AlwaysOn {
		minContainers = 1
	}
	var secrets []*pb.SecretVar
	var env []string
	for index, name := range names.bound() {
		if len(product.SecretEnv) > 0 {
			env = append(env, fmt.Sprintf("%s=${{secret.%s}}", product.SecretEnv[index], name))
		} else {
			secrets = append(secrets, &pb.SecretVar{Name: name})
		}
	}
	if product.Kind == types.DatabaseKindRedis {
		env = append(env, fmt.Sprintf("BETA9_REDIS_MAXMEMORY_BYTES=%d", p.Memory*(1<<20)/2))
	}

	ports := []uint32{product.Port}
	if product.PoolPort != 0 {
		ports = append(ports, product.PoolPort)
		env = append(env, fmt.Sprintf("BEAM_DATABASE_POOL_PORT=%d", product.PoolPort))
	}
	var pool *pb.PoolConfig
	if p.Pool != "" {
		pool = &pb.PoolConfig{Name: p.Pool}
	}
	return &pb.GetOrCreateStubRequest{
		ImageId:            imageId,
		StubType:           types.StubTypePodDeployment,
		Name:               types.StubTypePodDeployment,
		PythonVersion:      databasePythonVersion,
		Cpu:                p.Cpu,
		Memory:             p.Memory,
		KeepWarmSeconds:    databaseKeepWarmSeconds,
		Workers:            1,
		MaxPendingTasks:    100,
		Secrets:            secrets,
		Env:                env,
		Autoscaler:         &pb.Autoscaler{Type: "queue_depth", MaxContainers: 1, TasksPerContainer: 1, MinContainers: minContainers},
		TaskPolicy:         &pb.TaskPolicy{Timeout: 3600, MaxRetries: 3},
		ConcurrentRequests: 1,
		Entrypoint:         names.entrypoint(product),
		Ports:              ports,
		AppName:            p.Name,
		ForceCreate:        true,
		Tcp:                true,
		Pool:               pool,
		IsService:          true,
		Serving: &pb.ServingConfig{
			AppKind:         "database",
			ServingProtocol: product.Kind,
			Database: &pb.DatabaseServingConfig{
				Kind:                    product.Kind,
				Port:                    product.Port,
				ReadinessProbe:          product.ReadinessProbe,
				ConnectionEnvName:       product.ConnectionEnvName,
				CredentialSecretNames:   names.all(),
				DurabilityMode:          product.DurabilityMode,
				UsernameSecretName:      names.Username,
				PasswordSecretName:      names.Password,
				DatabaseSecretName:      names.Database,
				ConnectionUrlSecretName: names.URL,
			},
		},
		Disks: []*pb.DurableDisk{{
			Name: diskName, Size: p.Size, MountPath: product.MountPath,
			Filesystem: databaseDiskFilesystem, Driver: databaseDiskDriver(product),
			SourceSnapshotId: p.SnapshotID,
		}},
	}
}

var databaseIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

func (gws *GatewayService) prepareDatabaseRestore(ctx context.Context, workspace *types.Workspace, product databaseProduct, p *types.CreateDatabaseParams) error {
	if product.Kind != types.DatabaseKindPostgres && product.Kind != types.DatabaseKindRedis {
		return errors.New("snapshot restore supports Postgres and Redis")
	}
	if product.Kind == types.DatabaseKindPostgres && (p.Username == "" || p.Database == "") {
		return errors.New("Postgres restore requires the original username and database name; a fresh password is generated")
	}
	snapshot, err := gws.backendRepo.GetDiskSnapshot(ctx, workspace.Id, p.SnapshotID)
	if err != nil {
		return err
	}
	if snapshot == nil || snapshot.Status != types.DiskSnapshotStatusAvailable || snapshot.Driver != types.DurableDiskDriverQcow {
		return errors.New("restore requires an available qcow snapshot in this workspace")
	}
	if p.Size == "" {
		p.Size = strconv.FormatInt(snapshot.SizeBytes, 10)
	}
	size, err := resource.ParseQuantity(p.Size)
	if err != nil || size.Value() < snapshot.SizeBytes {
		return errors.New("restore disk cannot be smaller than the source snapshot")
	}
	return nil
}

func databaseDiskDriver(product databaseProduct) string {
	if product.DurabilityMode == "object-store-flush" {
		return types.DurableDiskDriverQcow
	}
	return types.DurableDiskDriverSnapshot
}

// RotateDatabaseCredentials sets a new password and recycles the container.
func (gws *GatewayService) RotateDatabaseCredentials(ctx context.Context, authInfo *auth.AuthInfo, name string) (*types.DatabaseServiceInfo, error) {
	deployments, product, err := gws.databaseDeployments(ctx, authInfo.Workspace, name)
	if err != nil {
		return nil, err
	}
	deployment := newestDeployment(deployments)
	names := databaseSecrets(product, name)

	password, err := randomString(32, defaultSecretAlphabet)
	if err != nil {
		return nil, err
	}
	username, err := gws.SecretValue(ctx, authInfo.Workspace, names.Username)
	if err != nil {
		return nil, err
	}
	database := ""
	if product.HasDatabase {
		if database, err = gws.SecretValue(ctx, authInfo.Workspace, names.Database); err != nil {
			return nil, err
		}
	}
	if err := gws.upsertSecret(ctx, authInfo, names.Password, password); err != nil {
		return nil, err
	}

	info := databaseInfo(product, names, deployment)
	if err := gws.setDatabaseConnections(ctx, authInfo, &info, username, password, database); err != nil {
		return nil, err
	}

	// New containers resolve current secrets through the shared launch helper.
	if err := gws.stopActiveDeploymentContainers(*deployment, true); err != nil {
		return nil, err
	}
	if err := gws.recycleDependents(ctx, authInfo.Workspace, deployment, databaseSecretPrefix(product.Kind, name)); err != nil {
		return nil, err
	}

	return &info, nil
}

// setDatabaseConnections keeps direct and pooled credentials in sync.
func (gws *GatewayService) setDatabaseConnections(ctx context.Context, authInfo *auth.AuthInfo, info *types.DatabaseServiceInfo, username, password, database string) error {
	product := databaseProducts[info.Kind]
	info.Username, info.Database = username, database
	for _, endpoint := range []struct {
		port       uint32
		secret     string
		connection *string
	}{
		{product.Port, info.ConnectionStringSecret, &info.ConnectionString},
		{product.PoolPort, info.PooledConnectionStringSecret, &info.PooledConnectionString},
	} {
		if endpoint.secret == "" {
			continue
		}
		host, err := gws.databasePortHost(ctx, info.StubID, info.DeploymentID, endpoint.port)
		if err != nil {
			return err
		}
		if endpoint.port == product.Port {
			info.Host = host
		}
		*endpoint.connection = databaseConnectionString(info.Kind, username, password, host, database, gws.appConfig.Abstractions.Pod.TCP.CertFile != "")
		if err := gws.upsertSecret(ctx, authInfo, endpoint.secret, *endpoint.connection); err != nil {
			return err
		}
	}
	return nil
}

// recycleDependents restarts every other active deployment bound to a secret with this prefix.
func (gws *GatewayService) recycleDependents(ctx context.Context, workspace *types.Workspace, source *types.DeploymentWithRelated, secretPrefix string) error {
	dependents, err := gws.backendRepo.ListDeploymentsWithRelated(ctx, types.DeploymentFilter{
		WorkspaceID: workspace.Id,
		Active:      ptr.To(true),
		BaseFilter:  types.BaseFilter{Limit: 1000},
	})
	if err != nil {
		return err
	}
	bound := `"` + secretPrefix + `_`
	for i := range dependents {
		d := &dependents[i]
		if d.Stub.ExternalId != source.Stub.ExternalId && strings.Contains(d.Stub.Config, bound) {
			if err := gws.stopActiveDeploymentContainers(*d, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteDatabaseService removes the deployments, secrets, disks and app record.
// Snapshots already taken of the disks stay, as for any deleted disk.
func (gws *GatewayService) DeleteDatabaseService(ctx context.Context, authInfo *auth.AuthInfo, name string) error {
	deployments, product, err := gws.databaseDeployments(ctx, authInfo.Workspace, name)
	if err != nil {
		return err
	}
	var disks []string
	var volumes []string
	for _, d := range deployments {
		config, err := d.Stub.UnmarshalConfig()
		if err != nil {
			return fmt.Errorf("decode stub config: %w", err)
		}
		for _, disk := range config.Disks {
			if disk != nil && disk.Name != "" && !slices.Contains(disks, disk.Name) {
				disks = append(disks, disk.Name)
			}
		}
		for _, volume := range config.Volumes {
			if volume != nil && volume.MountPath == legacyBackupVolumeMount {
				volumes = append(volumes, volume.Id)
			}
		}
	}
	for _, d := range deployments {
		res, err := gws.DeleteDeployment(ctx, &pb.DeleteDeploymentRequest{Id: d.ExternalId})
		if err != nil {
			return err
		}
		if !res.Ok {
			return errors.New(res.ErrMsg)
		}
	}
	for _, secret := range databaseSecrets(product, name).all() {
		if err := gws.backendRepo.DeleteSecret(ctx, authInfo.Workspace, secret); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("delete secret %s: %w", secret, err)
		}
	}
	for _, disk := range disks {
		if err := gws.backendRepo.DeleteDisk(ctx, authInfo.Workspace.Id, disk); err != nil {
			return fmt.Errorf("delete disk %s: %w", disk, err)
		}
	}
	if err := gws.deleteLegacyBackupVolume(ctx, authInfo.Workspace, name+"-backups", volumes); err != nil {
		return err
	}
	return gws.backendRepo.DeleteApp(ctx, deployments[0].App.ExternalId)
}

func (gws *GatewayService) deleteLegacyBackupVolume(ctx context.Context, workspace *types.Workspace, name string, mounted []string) error {
	volume, err := gws.backendRepo.GetVolume(ctx, workspace.Id, name)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("find backup volume: %w", err)
	}
	if !slices.Contains(mounted, volume.ExternalId) {
		return nil
	}
	if workspace.StorageAvailable() {
		storage, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
		if err != nil {
			return err
		}
		if _, err := storage.DeleteWithPrefix(ctx, path.Join(types.DefaultVolumesPrefix, volume.ExternalId)); err != nil {
			return fmt.Errorf("delete backup volume contents: %w", err)
		}
	}
	if err := gws.backendRepo.DeleteVolume(ctx, workspace.Id, volume.Name); err != nil {
		return fmt.Errorf("delete backup volume: %w", err)
	}
	return nil
}

// ListDatabaseServices returns the newest version of each database service.
func (gws *GatewayService) ListDatabaseServices(ctx context.Context, authInfo *auth.AuthInfo) ([]types.DatabaseServiceInfo, error) {
	deployments, err := gws.backendRepo.ListDeploymentsWithRelated(ctx, types.DeploymentFilter{
		WorkspaceID: authInfo.Workspace.Id,
		StubType:    types.StringSlice{types.StubTypePodDeployment},
		BaseFilter:  types.BaseFilter{Limit: 1000},
	})
	if err != nil {
		return nil, err
	}

	byName := map[string][]types.DeploymentWithRelated{}
	for _, d := range deployments {
		if databaseKind(&d.Stub) != "" {
			byName[d.Name] = append(byName[d.Name], d)
		}
	}
	out := make([]types.DatabaseServiceInfo, 0, len(byName))
	for name, versions := range byName {
		d := newestDeployment(versions)
		product := databaseProducts[databaseKind(&d.Stub)]
		info := databaseInfo(product, databaseSecrets(product, name), d)
		info.Host = gws.deploymentTCPHost(d, product.Port)
		out = append(out, info)
	}
	return out, nil
}

// deploymentTCPHost is the TCP gateway address of one port of a loaded deployment.
func (gws *GatewayService) deploymentTCPHost(d *types.DeploymentWithRelated, port uint32) string {
	config, err := d.Stub.UnmarshalConfig()
	if err != nil || !config.TCP {
		return ""
	}
	raw := common.BuildPodDeploymentURL(gws.appConfig.Abstractions.Pod.TCP.GetExternalURL(), common.InvokeUrlTypeHost, &d.Deployment, config)
	return tcpHostFromURL(strings.ReplaceAll(raw, common.PortPlaceholder, strconv.FormatUint(uint64(port), 10)))
}

// databaseInfo is the public view of one database deployment.
func databaseInfo(product databaseProduct, names databaseSecretNames, d *types.DeploymentWithRelated) types.DatabaseServiceInfo {
	info := types.DatabaseServiceInfo{
		Name:                   d.Name,
		Kind:                   product.Kind,
		DeploymentID:           d.ExternalId,
		StubID:                 d.Stub.ExternalId,
		AppID:                  d.App.ExternalId,
		Version:                d.Version,
		Active:                 d.Active,
		ConnectionEnvName:      product.ConnectionEnvName,
		ConnectionStringSecret: names.URL,
		UsernameSecret:         names.Username,
		PasswordSecret:         names.Password,
		DatabaseSecret:         names.Database,
	}
	if config, err := d.Stub.UnmarshalConfig(); err == nil && slices.Contains(config.Ports, product.PoolPort) {
		info.PooledConnectionStringSecret = names.PooledURL
	}
	return info
}

func validateDatabaseName(name string) error {
	if len(name) < 2 || len(name) > 32 {
		return errors.New("name must be 2-32 characters")
	}
	for i, r := range name {
		lower := r >= 'a' && r <= 'z'
		digit := r >= '0' && r <= '9'
		if !(lower || digit || r == '-') || (i == 0 && !lower) {
			return errors.New("name must be lowercase letters, digits and dashes, starting with a letter")
		}
	}
	return nil
}

func compact(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func (gws *GatewayService) upsertSecret(ctx context.Context, authInfo *auth.AuthInfo, name, value string) error {
	if _, err := gws.backendRepo.GetSecretByName(ctx, authInfo.Workspace, name); err == nil {
		_, err = gws.backendRepo.UpdateSecret(ctx, authInfo.Workspace, authInfo.TokenId(), name, value)
		return err
	} else if err != sql.ErrNoRows {
		return err
	}
	_, err := gws.backendRepo.CreateSecret(ctx, authInfo.Workspace, authInfo.TokenId(), name, value, false)
	return err
}

// SecretValue returns a workspace secret's plaintext.
func (gws *GatewayService) SecretValue(ctx context.Context, workspace *types.Workspace, name string) (string, error) {
	secret, err := gws.backendRepo.GetSecretByNameDecrypted(ctx, workspace, name)
	if err != nil {
		return "", fmt.Errorf("read secret %s: %w", name, err)
	}
	return secret.Value, nil
}

// deploymentsByName is every version named exactly name (the repository filter is a substring match).
func (gws *GatewayService) deploymentsByName(ctx context.Context, workspace *types.Workspace, name string) ([]types.DeploymentWithRelated, error) {
	all, err := gws.backendRepo.ListDeploymentsWithRelated(ctx, types.DeploymentFilter{
		WorkspaceID: workspace.Id,
		Name:        name,
		BaseFilter:  types.BaseFilter{Limit: 1000},
	})
	if err != nil {
		return nil, err
	}
	out := make([]types.DeploymentWithRelated, 0, len(all))
	for _, d := range all {
		if d.Name == name {
			out = append(out, d)
		}
	}
	return out, nil
}

// databaseDeployments returns every version of the named service and its product.
func (gws *GatewayService) databaseDeployments(ctx context.Context, workspace *types.Workspace, name string) ([]types.DeploymentWithRelated, databaseProduct, error) {
	deployments, err := gws.deploymentsByName(ctx, workspace, name)
	if err != nil {
		return nil, databaseProduct{}, err
	}
	var product databaseProduct
	out := make([]types.DeploymentWithRelated, 0, len(deployments))
	for _, d := range deployments {
		if kind := databaseKind(&d.Stub); kind != "" {
			product = databaseProducts[kind]
			out = append(out, d)
		}
	}
	if len(out) == 0 {
		return nil, databaseProduct{}, types.ErrDatabaseNotFound
	}
	return out, product, nil
}

// newestDeployment prefers an active version, then the highest version.
func newestDeployment(deployments []types.DeploymentWithRelated) *types.DeploymentWithRelated {
	best := &deployments[0]
	for i := range deployments[1:] {
		d := &deployments[i+1]
		if (d.Active && !best.Active) || (d.Active == best.Active && d.Version > best.Version) {
			best = d
		}
	}
	return best
}

// databaseKind is the product kind of a database service stub, or "".
func databaseKind(stub *types.Stub) string {
	// Skip the decode for non-database pods.
	if !strings.Contains(stub.Config, `"database"`) {
		return ""
	}
	cfg, err := stub.UnmarshalConfig()
	if err != nil {
		return ""
	}
	if db := cfg.EffectiveDatabaseConfig(); db != nil {
		if kind := types.NormalizeDatabaseKind(db.Kind); databaseProducts[kind].Kind == kind {
			return kind
		}
	}
	return ""
}

// databaseHost is the TCP gateway host:port clients connect to.
func (gws *GatewayService) databaseHost(ctx context.Context, stubId, deploymentId string) (string, error) {
	stub, err := gws.backendRepo.GetStubByExternalId(ctx, stubId)
	if err != nil {
		return "", err
	}
	product := databaseProducts[databaseKind(&stub.Stub)]
	return gws.databasePortHost(ctx, stubId, deploymentId, product.Port)
}

func (gws *GatewayService) databasePortHost(ctx context.Context, stubId, deploymentId string, port uint32) (string, error) {
	res, err := gws.GetURL(ctx, &pb.GetURLRequest{StubId: stubId, DeploymentId: deploymentId})
	if err != nil {
		return "", err
	}
	if !res.Ok {
		return "", errors.New(res.ErrMsg)
	}
	return tcpHostFromURL(strings.ReplaceAll(res.Url, "<PORT>", strconv.FormatUint(uint64(port), 10))), nil
}

func tcpHostFromURL(raw string) string {
	host := raw
	if parsed, err := url.Parse(raw); err == nil {
		host = parsed.Host
		if host == "" {
			host = parsed.Path
		}
	}
	host = strings.TrimSuffix(host, "/")
	if !strings.Contains(host, ":") {
		host += ":443"
	}
	return host
}

// Configured gateway certificates require hostname and chain verification.
// Development gateways without a certificate generate an ephemeral self-signed
// certificate; their connection URLs explicitly opt out of verification.
func databaseConnectionString(kind, username, password, host, database string, verifyTLS bool) string {
	user, pass, db := url.QueryEscape(username), url.QueryEscape(password), url.QueryEscape(database)
	postgresTLS, mysqlTLS, mongoTLS, redisTLS := "require", "REQUIRED", "true", "none"
	if verifyTLS {
		postgresTLS = "verify-full"
		mysqlTLS, mongoTLS, redisTLS = "VERIFY_IDENTITY", "false", "required&ssl_check_hostname=true"
	}
	switch kind {
	case types.DatabaseKindPostgres:
		return fmt.Sprintf("postgresql://%s:%s@%s/%s?sslmode=%s", user, pass, host, db, postgresTLS)
	case "mysql":
		return fmt.Sprintf("mysql://%s:%s@%s/%s?ssl-mode=%s", user, pass, host, db, mysqlTLS)
	case "mongo":
		return fmt.Sprintf("mongodb://%s:%s@%s/%s?tls=true&tlsAllowInvalidCertificates=%s&authSource=admin", user, pass, host, db, mongoTLS)
	default:
		if username != "" && username != "default" {
			return fmt.Sprintf("rediss://%s:%s@%s/0?ssl_cert_reqs=%s", user, pass, host, redisTLS)
		}
		return fmt.Sprintf("rediss://:%s@%s/0?ssl_cert_reqs=%s", pass, host, redisTLS)
	}
}

// ensureRegistryImage returns the image id for a registry image, pulling it if needed.
func (gws *GatewayService) ensureRegistryImage(ctx context.Context, imageURI string, commands []string) (string, error) {
	if gws.imageService == nil {
		return "", types.ErrDatabaseImageUnsupported
	}
	verify, err := gws.imageService.VerifyImageBuild(ctx, &pb.VerifyImageBuildRequest{
		PythonVersion:    databasePythonVersion,
		ExistingImageUri: imageURI,
		IgnorePython:     true,
		Commands:         commands,
	})
	if err != nil {
		return "", fmt.Errorf("verify image: %w", err)
	}
	if verify.Exists {
		return verify.ImageId, nil
	}

	stream := &collectBuildStream{ctx: ctx}
	err = gws.imageService.BuildImage(&pb.BuildImageRequest{
		PythonVersion:    databasePythonVersion,
		ExistingImageUri: imageURI,
		IgnorePython:     true,
		Commands:         commands,
	}, stream)
	if err != nil {
		return "", fmt.Errorf("build image: %w", err)
	}
	if stream.final == nil || !stream.final.Success {
		if stream.final != nil && stream.final.Msg != "" {
			return "", errors.New(strings.TrimSpace(stream.final.Msg))
		}
		return "", errors.New("image build did not complete")
	}
	return stream.final.ImageId, nil
}

// collectBuildStream keeps only the terminal BuildImage message.
type collectBuildStream struct {
	grpc.ServerStream
	ctx   context.Context
	final *pb.BuildImageResponse
}

func (s *collectBuildStream) Context() context.Context { return s.ctx }

func (s *collectBuildStream) Send(res *pb.BuildImageResponse) error {
	if res.Done {
		s.final = res
	}
	return nil
}

func (s *collectBuildStream) SendMsg(m any) error {
	if res, ok := m.(*pb.BuildImageResponse); ok {
		return s.Send(res)
	}
	return nil
}

func (s *collectBuildStream) RecvMsg(any) error            { return nil }
func (s *collectBuildStream) SetHeader(metadata.MD) error  { return nil }
func (s *collectBuildStream) SendHeader(metadata.MD) error { return nil }
func (s *collectBuildStream) SetTrailer(metadata.MD)       {}
