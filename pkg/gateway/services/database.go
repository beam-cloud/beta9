package gatewayservices

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

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
		DurabilityMode:    "snapshot_wal",
		HasDatabase:       true,
		Entrypoint:        postgresEntrypoint,
	},
	"redis": {
		Kind:              "redis",
		Image:             "docker.io/library/redis:7",
		Port:              6379,
		MountPath:         "/data",
		DefaultSize:       "5Gi",
		ReadinessProbe:    "PING",
		ConnectionEnvName: "REDIS_URL",
		DurabilityMode:    "aof_tail",
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

// Entrypoints read credentials from the bound secrets and sync the role
// password on start, so a rotation is a restart. `$USER_SECRET` and friends
// are replaced with the secret names.
const (
	postgresEntrypoint = `export PATH=/usr/lib/postgresql/16/bin:$PATH POSTGRES_USER="${USER_SECRET}" POSTGRES_PASSWORD="${PASSWORD_SECRET}" POSTGRES_DB="${DATABASE_SECRET}" PGDATA=/var/lib/postgresql/data/pgdata;
if [ -s "$PGDATA/PG_VERSION" ]; then
  ESCAPED=$(printf %s "$POSTGRES_PASSWORD" | sed "s/'/''/g");
  printf 'ALTER USER "%s" PASSWORD '"'"'%s'"'"';\n' "$POSTGRES_USER" "$ESCAPED" | gosu postgres postgres --single -D "$PGDATA" postgres >/dev/null 2>&1 || true;
fi;
exec docker-entrypoint.sh postgres -c wal_compression=on`

	redisEntrypoint = `if [ "${USER_SECRET}" = "default" ]; then
  printf 'appendonly yes\nappendfsync always\ndir /data\nuser default on >%s ~* &* +@all\n' "${PASSWORD_SECRET}" > /tmp/redis.conf;
else
  printf 'appendonly yes\nappendfsync always\ndir /data\nuser default off\nuser %s on >%s ~* &* +@all\n' "${USER_SECRET}" "${PASSWORD_SECRET}" > /tmp/redis.conf;
fi;
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
	Username, Password, Database, URL string
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
	return names
}

// bound excludes the URL, which the entrypoint derives.
func (n databaseSecretNames) all() []string   { return append(n.bound(), n.URL) }
func (n databaseSecretNames) bound() []string { return compact(n.Username, n.Password, n.Database) }

func (n databaseSecretNames) entrypoint(product databaseProduct) []string {
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
	if existing, err := gws.deploymentsByName(ctx, authInfo.Workspace, p.Name); err != nil {
		return nil, err
	} else if len(existing) > 0 {
		return nil, types.ErrDatabaseExists
	}

	identifier := strings.ReplaceAll(p.Name, "-", "_")
	if p.Username == "" {
		p.Username = identifier
	}
	if product.HasDatabase && p.Database == "" {
		p.Database = identifier
	}
	if p.Password == "" {
		password, err := randomToken(32)
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

	imageId, err := gws.ensureRegistryImage(ctx, product.Image)
	if err != nil {
		return nil, err
	}
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

	stubRes, err := gws.GetOrCreateStub(ctx, &pb.GetOrCreateStubRequest{
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
		Disks: []*pb.DurableDisk{{Name: p.Name + "-data", Size: p.Size, MountPath: product.MountPath, Filesystem: databaseDiskFilesystem}},
	})
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

	host, err := gws.databaseHost(ctx, stubRes.StubId, deployRes.DeploymentId)
	if err != nil {
		return nil, err
	}
	connection := databaseConnectionString(product.Kind, p.Username, p.Password, host, p.Database)
	if err := gws.upsertSecret(ctx, authInfo, names.URL, connection); err != nil {
		return nil, err
	}

	deployment, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, deployRes.DeploymentId)
	if err != nil {
		return nil, fmt.Errorf("read deployment: %w", err)
	}
	info := databaseInfo(product, names, deployment)
	info.Host, info.Username, info.Database, info.ConnectionString = host, p.Username, p.Database, connection
	return &info, nil
}

// RotateDatabaseCredentials sets a new password and recycles the container.
func (gws *GatewayService) RotateDatabaseCredentials(ctx context.Context, authInfo *auth.AuthInfo, name string) (*types.DatabaseServiceInfo, error) {
	deployments, product, err := gws.databaseDeployments(ctx, authInfo.Workspace, name)
	if err != nil {
		return nil, err
	}
	deployment := newestDeployment(deployments)
	names := databaseSecrets(product, name)

	password, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	username, err := gws.secretValue(ctx, authInfo.Workspace, names.Username)
	if err != nil {
		return nil, err
	}
	database := ""
	if product.HasDatabase {
		if database, err = gws.secretValue(ctx, authInfo.Workspace, names.Database); err != nil {
			return nil, err
		}
	}
	if err := gws.upsertSecret(ctx, authInfo, names.Password, password); err != nil {
		return nil, err
	}

	host, err := gws.databaseHost(ctx, deployment.Stub.ExternalId, deployment.ExternalId)
	if err != nil {
		return nil, err
	}
	connection := databaseConnectionString(product.Kind, username, password, host, database)
	if err := gws.upsertSecret(ctx, authInfo, names.URL, connection); err != nil {
		return nil, err
	}

	// The stub config caches secret values; refresh before recycling.
	if err := gws.refreshStubSecrets(ctx, authInfo.Workspace, &deployment.Stub); err != nil {
		return nil, err
	}
	if containers, err := gws.containerRepo.GetActiveContainersByStubId(deployment.Stub.ExternalId); err == nil {
		for _, c := range containers {
			_ = gws.scheduler.Stop(&types.StopContainerArgs{ContainerId: c.ContainerId, Force: true})
		}
	}

	info := databaseInfo(product, names, deployment)
	info.Host, info.Username, info.Database, info.ConnectionString = host, username, database, connection
	return &info, nil
}

// DeleteDatabaseService removes the deployment and its secrets; the disk is kept.
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
		out = append(out, databaseInfo(product, databaseSecrets(product, name), d))
	}
	return out, nil
}

// databaseInfo is the public view of one database deployment.
func databaseInfo(product databaseProduct, names databaseSecretNames, d *types.DeploymentWithRelated) types.DatabaseServiceInfo {
	return types.DatabaseServiceInfo{
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

func randomToken(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
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

// secretValue returns a workspace secret's plaintext.
func (gws *GatewayService) secretValue(ctx context.Context, workspace *types.Workspace, name string) (string, error) {
	secret, err := gws.backendRepo.GetSecretByName(ctx, workspace, name)
	if err != nil {
		return "", fmt.Errorf("read secret %s: %w", name, err)
	}
	if workspace.SigningKey == nil {
		return "", errors.New("workspace has no signing key")
	}
	key, err := common.ParseSecretKey(*workspace.SigningKey)
	if err != nil {
		return "", err
	}
	value, err := common.Decrypt(key, secret.Value)
	if err != nil {
		return "", fmt.Errorf("decrypt secret %s: %w", name, err)
	}
	return value, nil
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

func (gws *GatewayService) deploymentsByName(ctx context.Context, workspace *types.Workspace, name string) ([]types.DeploymentWithRelated, error) {
	return gws.backendRepo.ListDeploymentsWithRelated(ctx, types.DeploymentFilter{
		WorkspaceID: workspace.Id,
		Name:        name,
		BaseFilter:  types.BaseFilter{Limit: 50},
	})
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
		if kind := types.NormalizeDatabaseKind(db.Kind); databaseProducts[kind].Kind != "" {
			return kind
		}
	}
	return ""
}

// databaseHost is the TCP gateway host:port clients connect to.
func (gws *GatewayService) databaseHost(ctx context.Context, stubId, deploymentId string) (string, error) {
	res, err := gws.GetURL(ctx, &pb.GetURLRequest{StubId: stubId, DeploymentId: deploymentId})
	if err != nil {
		return "", err
	}
	if !res.Ok {
		return "", errors.New(res.ErrMsg)
	}
	return tcpHostFromURL(res.Url), nil
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

func databaseConnectionString(kind, username, password, host, database string) string {
	user, pass, db := url.QueryEscape(username), url.QueryEscape(password), url.QueryEscape(database)
	switch kind {
	case "postgres":
		return fmt.Sprintf("postgresql://%s:%s@%s/%s?sslmode=require", user, pass, host, db)
	case "mysql":
		return fmt.Sprintf("mysql://%s:%s@%s/%s?ssl-mode=REQUIRED", user, pass, host, db)
	case "mongo":
		return fmt.Sprintf("mongodb://%s:%s@%s/%s?tls=true&authSource=admin", user, pass, host, db)
	default:
		if username != "" && username != "default" {
			return fmt.Sprintf("rediss://%s:%s@%s/0", user, pass, host)
		}
		return fmt.Sprintf("rediss://:%s@%s/0", pass, host)
	}
}

// ensureRegistryImage returns the image id for a registry image, pulling it if needed.
func (gws *GatewayService) ensureRegistryImage(ctx context.Context, imageURI string) (string, error) {
	if gws.imageService == nil {
		return "", types.ErrDatabaseImageUnsupported
	}
	verify, err := gws.imageService.VerifyImageBuild(ctx, &pb.VerifyImageBuildRequest{
		PythonVersion:    databasePythonVersion,
		ExistingImageUri: imageURI,
		IgnorePython:     true,
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
