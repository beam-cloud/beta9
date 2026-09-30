package gatewayservices

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

//go:embed postgres.sh
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
)

type databaseProduct struct {
	Kind              string
	Image             string
	Port              uint32
	MountPath         string
	DefaultSize       string
	ReadinessProbe    string
	ConnectionEnvName string
	DurabilityMode    string
	HasDatabase       bool // a named database next to user/password (not Redis)
	Entrypoint        string
}

var databaseProducts = map[string]databaseProduct{
	"postgres": {
		Kind:              "postgres",
		Image:             "docker.io/library/postgres:16",
		Port:              5432,
		MountPath:         "/var/lib/postgresql/data",
		DefaultSize:       "10Gi",
		ReadinessProbe:    "pg_isready",
		ConnectionEnvName: "DATABASE_URL",
		DurabilityMode:    "object-store-flush",
		HasDatabase:       true,
	},
	"redis": {
		Kind:              "redis",
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
	if product.Kind == "postgres" {
		names.PooledURL = prefix + "_POOLED_URL"
	}
	return names
}

// bound excludes the URL, written after deploy once the host is known.
func (n databaseSecretNames) all() []string   { return append(n.bound(), compact(n.URL, n.PooledURL)...) }
func (n databaseSecretNames) bound() []string { return compact(n.Username, n.Password, n.Database) }

func (n databaseSecretNames) entrypoint(product databaseProduct) []string {
	if product.Kind == "postgres" {
		script := base64.StdEncoding.EncodeToString([]byte(managedPostgresScript))
		return []string{"sh", "-lc", "printf %s " + script + " | base64 -d > /tmp/beam-postgres; exec sh /tmp/beam-postgres"}
	}
	script := strings.NewReplacer(
		"USER_SECRET", n.Username,
		"PASSWORD_SECRET", n.Password,
		"DATABASE_SECRET", n.Database,
	).Replace(product.Entrypoint)
	return []string{"sh", "-lc", script}
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
	var restoreVolume string
	if p.RestoreFrom != "" || p.RestoreTime != "" {
		var err error
		restoreVolume, err = gws.preparePointInTimeRestore(ctx, authInfo.Workspace, product, &p)
		if err != nil {
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

	var commands []string
	if product.Kind == "postgres" {
		commands = []string{"apt-get update && apt-get install -y --no-install-recommends pgbackrest pgbouncer jq && rm -rf /var/lib/apt/lists/*"}
	}
	imageId, err := gws.ensureRegistryImage(ctx, product.Image, commands)
	if err != nil {
		return nil, err
	}
	request := databaseStubRequest(product, names, p, imageId)
	if product.Kind == "postgres" {
		volume, err := gws.backendRepo.GetOrCreateVolume(ctx, authInfo.Workspace.Id, p.Name+"-backups")
		if err != nil {
			return nil, fmt.Errorf("create backup volume: %w", err)
		}
		request.Volumes = []*pb.Volume{{Id: volume.ExternalId, MountPath: "beam-backups"}}
		request.Ports = append(request.Ports, 6432)
		request.Secrets = nil
		request.Env = append(request.Env,
			"POSTGRES_USER=${{secret."+names.Username+"}}",
			"POSTGRES_PASSWORD=${{secret."+names.Password+"}}",
			"POSTGRES_DB=${{secret."+names.Database+"}}",
		)
		if restoreVolume != "" {
			request.Volumes = append(request.Volumes, &pb.Volume{Id: restoreVolume, MountPath: "beam-restore"})
			request.Env = append(request.Env, "BEAM_RESTORE_TIME="+p.RestoreTime)
		}
	}
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
func databaseStubRequest(product databaseProduct, names databaseSecretNames, p types.CreateDatabaseParams, imageId string) *pb.GetOrCreateStubRequest {
	minContainers := uint32(0)
	if p.AlwaysOn {
		minContainers = 1
	}
	secrets := make([]*pb.SecretVar, 0, 3)
	for _, name := range names.bound() {
		secrets = append(secrets, &pb.SecretVar{Name: name})
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
		Env:                []string{fmt.Sprintf("BETA9_REDIS_MAXMEMORY_BYTES=%d", p.Memory*(1<<20)/2)},
		Autoscaler:         &pb.Autoscaler{Type: "queue_depth", MaxContainers: 1, TasksPerContainer: 1, MinContainers: minContainers},
		TaskPolicy:         &pb.TaskPolicy{Timeout: 3600, MaxRetries: 3},
		ConcurrentRequests: 1,
		Entrypoint:         names.entrypoint(product),
		Ports:              []uint32{product.Port},
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
			Name: p.Name + "-data", Size: p.Size, MountPath: product.MountPath,
			Filesystem: databaseDiskFilesystem, Driver: databaseDiskDriver(product),
			SourceSnapshotId: p.SnapshotID,
		}},
	}
}

var databaseIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

func (gws *GatewayService) DatabaseBackups(ctx context.Context, workspace *types.Workspace, name string) (*types.DatabaseBackupStatus, error) {
	volume, err := gws.backendRepo.GetVolume(ctx, workspace.Id, name+"-backups")
	if err != nil {
		return nil, fmt.Errorf("find backup volume: %w", err)
	}
	storage, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return nil, err
	}
	data, _, err := storage.ReadVersion(ctx, "volumes/"+volume.ExternalId+"/status.json")
	if err != nil {
		return nil, fmt.Errorf("read backup status: %w", err)
	}
	status := &types.DatabaseBackupStatus{Status: "initializing"}
	if len(data) > 0 {
		if err := json.Unmarshal(data, status); err != nil {
			return nil, fmt.Errorf("invalid backup status: %w", err)
		}
	}
	status.VolumeID = volume.ExternalId
	status.Stale = time.Now().Unix()-status.ObservedAt > 180
	if status.Kind == "postgres" && len(status.Repository) > 0 {
		var repositories []struct {
			Backup []struct {
				Error     bool `json:"error"`
				Timestamp struct {
					Stop int64 `json:"stop"`
				} `json:"timestamp"`
			} `json:"backup"`
		}
		if err := json.Unmarshal(status.Repository, &repositories); err != nil {
			return nil, fmt.Errorf("invalid Postgres backup catalog: %w", err)
		}
		var first int64
		for _, repository := range repositories {
			for _, backup := range repository.Backup {
				if !backup.Error && backup.Timestamp.Stop > 0 && (first == 0 || backup.Timestamp.Stop < first) {
					first = backup.Timestamp.Stop
				}
			}
		}
		// pgBackRest chooses a base backup whose stop precedes the target.
		if first == 0 {
			return status, nil
		}
		first = max(first+1, time.Now().Add(-7*24*time.Hour).Unix())
		if status.ArchiveThrough >= first {
			start, end := time.Unix(first, 0).UTC(), time.Unix(status.ArchiveThrough, 0).UTC()
			status.RecoverableFrom, status.RecoverableUntil = &start, &end
		}
	}
	return status, nil
}

func (gws *GatewayService) preparePointInTimeRestore(ctx context.Context, workspace *types.Workspace, product databaseProduct, p *types.CreateDatabaseParams) (string, error) {
	if product.Kind != "postgres" || p.RestoreFrom == "" || p.RestoreTime == "" || p.SnapshotID != "" {
		return "", errors.New("Postgres PITR requires restore_from and restore_time, without snapshot_id")
	}
	if _, err := gws.backendRepo.GetDisk(ctx, workspace.Id, p.Name+"-data"); err == nil {
		return "", errors.New("restore requires a new disk name; the existing disk is retained")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	status, err := gws.DatabaseBackups(ctx, workspace, p.RestoreFrom)
	if err != nil {
		return "", err
	}
	target, err := time.Parse(time.RFC3339Nano, p.RestoreTime)
	if err != nil {
		return "", errors.New("restore_time must be an RFC3339 timestamp including its time zone")
	}
	if status.Kind != "postgres" || status.RecoverableFrom == nil || status.RecoverableUntil == nil || target.Before(*status.RecoverableFrom) || target.After(*status.RecoverableUntil) {
		return "", errors.New("restore_time is outside the verified recovery window; inspect database_backups")
	}
	source, err := gws.backendRepo.GetDisk(ctx, workspace.Id, p.RestoreFrom+"-data")
	if err != nil {
		return "", fmt.Errorf("read retained source disk: %w", err)
	}
	if p.Size == "" {
		p.Size = source.Size
	}
	sourceSize, err := resource.ParseQuantity(source.Size)
	if err != nil {
		return "", fmt.Errorf("invalid source disk size: %w", err)
	}
	size, err := resource.ParseQuantity(p.Size)
	if err != nil || size.Value() < sourceSize.Value() {
		return "", errors.New("restore disk cannot be smaller than the source disk")
	}
	p.Username, p.Database = status.Username, status.Database
	p.RestoreTime = target.UTC().Format("2006-01-02 15:04:05.999999999-07:00")
	return status.VolumeID, nil
}

// BackupDatabase wakes the existing deployment, then runs its native backup.
// A timeout is an uncertain outcome: callers must read database_backups first.
func (gws *GatewayService) BackupDatabase(ctx context.Context, authInfo *auth.AuthInfo, name string) (*types.DatabaseBackupStatus, error) {
	deployments, product, err := gws.databaseDeployments(ctx, authInfo.Workspace, name)
	if err != nil {
		return nil, err
	}
	if product.Kind != "postgres" {
		return nil, errors.New("native backup is currently supported for Postgres")
	}
	deployment := newestDeployment(deployments)
	if !deployment.Active {
		return nil, errors.New("start the database before requesting a backup")
	}
	// Legacy services must be upgraded before a native backup can be requested.
	config, err := deployment.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if !strings.Contains(strings.Join(config.EntryPoint, " "), "/tmp/beam-postgres") {
		return nil, errors.New("this deployment predates native backups; upgrade it before requesting a backup")
	}
	response, err := gws.ScaleDeployment(ctx, &pb.ScaleDeploymentRequest{Id: deployment.ExternalId, Containers: 1})
	if err != nil {
		return nil, err
	}
	if !response.Ok {
		return nil, errors.New(response.ErrMsg)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		containers, err := gws.containerRepo.GetActiveContainersByStubId(deployment.Stub.ExternalId)
		if err != nil {
			return nil, err
		}
		for _, container := range containers {
			if container.Status != types.ContainerStatusRunning {
				continue
			}
			client, _, err := gws.getClient(ctx, container.ContainerId, authInfo.Token.Key, authInfo.Workspace.ExternalId)
			if err != nil {
				return nil, err
			}
			result, err := client.ExecContext(ctx, container.ContainerId, "sh /tmp/beam-postgres backup", nil)
			if err != nil {
				return nil, fmt.Errorf("backup outcome uncertain; inspect database_backups and logs before retrying: %w", err)
			}
			if !result.Ok {
				return nil, errors.New("backup failed or another backup is running; inspect database_backups and database logs")
			}
			return gws.DatabaseBackups(ctx, authInfo.Workspace, name)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (gws *GatewayService) prepareDatabaseRestore(ctx context.Context, workspace *types.Workspace, product databaseProduct, p *types.CreateDatabaseParams) error {
	if product.Kind != "postgres" && product.Kind != "redis" {
		return errors.New("snapshot restore supports Postgres and Redis")
	}
	if product.Kind == "postgres" && (p.Username == "" || p.Database == "") {
		return errors.New("Postgres restore requires the original username and database name; a fresh password is generated")
	}
	if _, err := gws.backendRepo.GetDisk(ctx, workspace.Id, p.Name+"-data"); err == nil {
		return errors.New("restore requires a new disk name; the existing disk is retained")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
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

	// Stub configs and running instances cache secret values. Recycle the
	// database and every active deployment bound to its secrets so nothing
	// keeps serving with the old password.
	if err := gws.recycleWithSecrets(ctx, authInfo.Workspace, deployment, true); err != nil {
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
		{6432, info.PooledConnectionStringSecret, &info.PooledConnectionString},
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
			if err := gws.recycleWithSecrets(ctx, workspace, d, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteDatabaseService removes the deployments, secrets and app record; the disk is kept.
func (gws *GatewayService) DeleteDatabaseService(ctx context.Context, authInfo *auth.AuthInfo, name string) error {
	deployments, product, err := gws.databaseDeployments(ctx, authInfo.Workspace, name)
	if err != nil {
		return err
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
	return gws.backendRepo.DeleteApp(ctx, deployments[0].App.ExternalId)
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
		out = append(out, databaseInfo(product, databaseSecrets(product, name), d))
	}
	return out, nil
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
	if config, err := d.Stub.UnmarshalConfig(); err == nil && product.Kind == "postgres" {
		for _, port := range config.Ports {
			if port == 6432 {
				info.PooledConnectionStringSecret = names.PooledURL
			}
		}
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

// recycleWithSecrets re-reads a deployment's bound secrets into its stub
// config, reloads the instance, and stops its containers so replacements
// start with the new values. Databases stop hard; their entrypoints apply
// the new password on the way up. Apps drain.
func (gws *GatewayService) recycleWithSecrets(ctx context.Context, workspace *types.Workspace, d *types.DeploymentWithRelated, force bool) error {
	if err := gws.refreshStubSecrets(ctx, workspace, &d.Stub); err != nil {
		return err
	}
	gws.reloadInstances(d.Stub.ExternalId, d.StubType)
	return gws.stopActiveDeploymentContainers(*d, force)
}

// refreshStubSecrets re-reads the bound secrets into the stub config.
func (gws *GatewayService) refreshStubSecrets(ctx context.Context, workspace *types.Workspace, stub *types.Stub) error {
	cfg, err := stub.UnmarshalConfig()
	if err != nil {
		return err
	}
	for i := range cfg.Secrets {
		fresh, err := gws.backendRepo.GetSecretByName(ctx, workspace, cfg.Secrets[i].Name)
		if err != nil {
			return fmt.Errorf("refresh secret %s: %w", cfg.Secrets[i].Name, err)
		}
		cfg.Secrets[i].Value = fresh.Value
		cfg.Secrets[i].UpdatedAt = fresh.UpdatedAt
	}
	return gws.backendRepo.UpdateStubConfig(ctx, stub.Id, cfg)
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
		postgresTLS = "verify-full&sslrootcert=system"
		mysqlTLS, mongoTLS, redisTLS = "VERIFY_IDENTITY", "false", "required&ssl_check_hostname=true"
	}
	switch kind {
	case "postgres":
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
