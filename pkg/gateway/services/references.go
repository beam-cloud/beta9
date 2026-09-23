package gatewayservices

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// Reference syntax accepted in stub env values:
//
//	KEY=${{secret.NAME}}            bind workspace secret NAME to env KEY
//	KEY=${{db.NAME.DATABASE_URL}}   bind a database service's credential secret to KEY
//	KEY=${{db.NAME.HOST}}           inline the database's host (PORT likewise); not a secret
//	KEY=${{secret(32)}}             generate a secret, store it as <APP>_<KEY>, bind to KEY
//	KEY=...${{app.NAME.URL}}...     inline the public URL of deployment NAME
//	KEY=...${{randomInt(1,100)}}... inline a random integer
//
// Secret-bearing references must be the whole value; the rest are inlined.
var referenceRe = regexp.MustCompile(`\$\{\{\s*([^}]*?)\s*\}\}`)

// secretBinding maps a workspace secret onto an env var name in the container.
type secretBinding struct {
	Name    string
	EnvName string
}

// expandReferences resolves `${{...}}` tokens in env, returning the rewritten
// env (secret-bearing entries removed) and the secret bindings to attach.
func (gws *GatewayService) expandReferences(ctx context.Context, authInfo *auth.AuthInfo, appName string, env []string) ([]string, []secretBinding, error) {
	scope := &referenceScope{gws: gws, ctx: ctx, workspace: authInfo.Workspace, tokenId: authInfo.TokenId(), appName: appName}
	out := make([]string, 0, len(env))
	bindings := []secretBinding{}

	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || !strings.Contains(value, "${{") {
			out = append(out, entry)
			continue
		}

		if m := referenceRe.FindStringSubmatchIndex(value); m != nil && m[0] == 0 && m[1] == len(value) {
			binding, handled, err := scope.secretBinding(key, strings.TrimSpace(value[m[2]:m[3]]))
			if err != nil {
				return nil, nil, err
			}
			if handled {
				bindings = append(bindings, binding)
				continue
			}
		}

		rewritten, err := scope.inline(value)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, key+"="+rewritten)
	}

	return out, bindings, nil
}

// referenceScope resolves one stub's references; database lookups are memoized.
type referenceScope struct {
	gws       *GatewayService
	ctx       context.Context
	workspace *types.Workspace
	tokenId   uint
	appName   string
	databases map[string]*referencedDatabase
}

type referencedDatabase struct {
	deployment *types.DeploymentWithRelated
	product    databaseProduct
	names      databaseSecretNames
	host, port string // resolved on first use
}

// secretBinding binds a secret to key; handled is false when expr is not secret-bearing.
func (s *referenceScope) secretBinding(key, expr string) (secretBinding, bool, error) {
	switch {
	case strings.HasPrefix(expr, "secret."):
		name := strings.TrimPrefix(expr, "secret.")
		if _, err := s.gws.backendRepo.GetSecretByName(s.ctx, s.workspace, name); err != nil {
			if err == sql.ErrNoRows {
				return secretBinding{}, false, fmt.Errorf("secret %q does not exist in this workspace", name)
			}
			return secretBinding{}, false, fmt.Errorf("resolve secret %q: %w", name, err)
		}
		return secretBinding{Name: name, EnvName: key}, true, nil

	case strings.HasPrefix(expr, "db."):
		dbName, field, ok := strings.Cut(strings.TrimPrefix(expr, "db."), ".")
		if !ok || strings.Contains(field, ".") {
			return secretBinding{}, false, fmt.Errorf("invalid database reference %q; expected db.<name>.<field>", expr)
		}
		if isDatabaseAddressField(field) {
			return secretBinding{}, false, nil // host and port are public; inlined instead
		}
		name, err := s.databaseSecretName(dbName, field)
		if err != nil {
			return secretBinding{}, false, err
		}
		return secretBinding{Name: name, EnvName: key}, true, nil

	case strings.HasPrefix(expr, "secret("):
		length, alphabet, err := parseSecretFunc(expr)
		if err != nil {
			return secretBinding{}, false, err
		}
		name := generatedSecretName(s.appName, key)
		_, err = s.gws.backendRepo.GetSecretByName(s.ctx, s.workspace, name)
		if err == nil {
			return secretBinding{Name: name, EnvName: key}, true, nil // generated on an earlier deploy; keep it stable
		}
		if err != sql.ErrNoRows {
			return secretBinding{}, false, fmt.Errorf("resolve generated secret: %w", err)
		}
		value, err := randomString(length, alphabet)
		if err != nil {
			return secretBinding{}, false, err
		}
		if _, err := s.gws.backendRepo.CreateSecret(s.ctx, s.workspace, s.tokenId, name, value, false); err != nil {
			return secretBinding{}, false, fmt.Errorf("store generated secret %q: %w", name, err)
		}
		return secretBinding{Name: name, EnvName: key}, true, nil
	}
	return secretBinding{}, false, nil
}

// inline substitutes the non-secret references inside value.
func (s *referenceScope) inline(value string) (string, error) {
	var firstErr error
	result := referenceRe.ReplaceAllStringFunc(value, func(token string) string {
		if firstErr != nil {
			return token
		}
		expr := strings.TrimSpace(referenceRe.FindStringSubmatch(token)[1])
		replacement, err := s.inlineOne(expr)
		if err != nil {
			firstErr = err
			return token
		}
		return replacement
	})
	return result, firstErr
}

func (s *referenceScope) inlineOne(expr string) (string, error) {
	switch {
	case strings.HasPrefix(expr, "app."):
		name, field, ok := strings.Cut(strings.TrimPrefix(expr, "app."), ".")
		if !ok || field != "URL" {
			return "", fmt.Errorf("invalid app reference %q; expected app.<name>.URL", expr)
		}
		return s.gws.deploymentURLByName(s.ctx, s.workspace, name)
	case strings.HasPrefix(expr, "db."):
		name, field, ok := strings.Cut(strings.TrimPrefix(expr, "db."), ".")
		if !ok || !isDatabaseAddressField(field) {
			return "", fmt.Errorf("reference %q must be the entire value; database credentials cannot be embedded in a string", expr)
		}
		db, err := s.database(name)
		if err != nil {
			return "", err
		}
		if err := s.resolveAddress(db, name); err != nil {
			return "", err
		}
		if field == "PORT" {
			return db.port, nil
		}
		return db.host, nil
	case strings.HasPrefix(expr, "randomInt("):
		n, err := parseRandomInt(expr)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(n, 10), nil
	case strings.HasPrefix(expr, "secret"):
		return "", fmt.Errorf("reference %q must be the entire value; secrets cannot be embedded in a string", expr)
	default:
		return "", fmt.Errorf("unknown reference %q", expr)
	}
}

func isDatabaseAddressField(field string) bool { return field == "HOST" || field == "PORT" }

// database resolves db.<name> to its newest deployment, once per expansion.
func (s *referenceScope) database(name string) (*referencedDatabase, error) {
	if db, ok := s.databases[name]; ok {
		return db, nil
	}
	deployments, product, err := s.gws.databaseDeployments(s.ctx, s.workspace, name)
	if err != nil {
		if errors.Is(err, types.ErrDatabaseNotFound) {
			return nil, fmt.Errorf("database %q does not exist in this workspace", name)
		}
		return nil, fmt.Errorf("resolve database %q: %w", name, err)
	}
	db := &referencedDatabase{deployment: newestDeployment(deployments), product: product, names: databaseSecrets(product, name)}
	if s.databases == nil {
		s.databases = map[string]*referencedDatabase{}
	}
	s.databases[name] = db
	return db, nil
}

// resolveAddress fills in host and port, once.
func (s *referenceScope) resolveAddress(db *referencedDatabase, name string) error {
	if db.host != "" {
		return nil
	}
	hostPort, err := s.gws.databaseHost(s.ctx, db.deployment.Stub.ExternalId, db.deployment.ExternalId)
	if err != nil {
		return fmt.Errorf("resolve database %q address: %w", name, err)
	}
	if db.host, db.port, err = net.SplitHostPort(hostPort); err != nil {
		db.host, db.port = hostPort, ""
	}
	return nil
}

// databaseSecretName maps db.<name>.<field> to BETA9_<KIND>_<NAME>_<FIELD>.
func (s *referenceScope) databaseSecretName(name, field string) (string, error) {
	db, err := s.database(name)
	if err != nil {
		return "", err
	}
	if !db.deployment.Active {
		return "", fmt.Errorf("database %q is not active", name)
	}
	switch field {
	case "URL", "DATABASE_URL", "REDIS_URL":
		return db.names.URL, nil
	case "USERNAME":
		return db.names.Username, nil
	case "PASSWORD":
		return db.names.Password, nil
	case "DATABASE":
		if db.names.Database == "" {
			return "", fmt.Errorf("%s services have no database name", db.product.Kind)
		}
		return db.names.Database, nil
	}
	return "", fmt.Errorf("unknown database field %q; use DATABASE_URL, USERNAME, PASSWORD, DATABASE, HOST or PORT", field)
}

// sanitizeSecretName mirrors `_env_prefix` in the SDK's database CLI.
func sanitizeSecretName(name string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, strings.ToUpper(name))
	return strings.Trim(safe, "_")
}

// databaseSecretPrefix mirrors `_env_prefix` in the SDK's database CLI.
func databaseSecretPrefix(kind, name string) string {
	safe := sanitizeSecretName(name)
	if safe == "" {
		safe = "SERVICE"
	}
	return fmt.Sprintf("BETA9_%s_%s", strings.ToUpper(kind), safe)
}

// generatedSecretName is where `${{secret()}}` values are stored: <APP>_<KEY>.
func generatedSecretName(appName, key string) string {
	app := sanitizeSecretName(appName)
	if app == "" {
		app = "APP"
	}
	return app + "_" + sanitizeSecretName(key)
}

// deploymentURLByName is the public URL of the newest active deployment named name.
func (gws *GatewayService) deploymentURLByName(ctx context.Context, workspace *types.Workspace, name string) (string, error) {
	deployments, err := gws.deploymentsByName(ctx, workspace, name)
	if err != nil {
		return "", fmt.Errorf("resolve app %q: %w", name, err)
	}
	active := make([]types.DeploymentWithRelated, 0, len(deployments))
	for _, d := range deployments {
		if d.Active {
			active = append(active, d)
		}
	}
	if len(active) == 0 {
		return "", fmt.Errorf("no active deployment named %q in this workspace", name)
	}
	d := newestDeployment(active)

	externalURL := gws.appConfig.GatewayService.HTTP.GetExternalURL()
	urlType := gws.appConfig.GatewayService.InvokeURLType
	if d.Stub.Type.Kind() == types.StubTypePod {
		cfg, err := d.Stub.UnmarshalConfig()
		if err != nil {
			return "", fmt.Errorf("decode stub config: %w", err)
		}
		if cfg.TCP {
			return common.BuildPodDeploymentURL(gws.appConfig.Abstractions.Pod.TCP.GetExternalURL(), common.InvokeUrlTypeHost, &d.Deployment, cfg), nil
		}
		return common.BuildPodDeploymentURL(externalURL, urlType, &d.Deployment, cfg), nil
	}
	return common.BuildDeploymentURL(externalURL, urlType, &types.StubWithRelated{Stub: d.Stub}, &d.Deployment), nil
}

const defaultSecretAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// parseSecretFunc parses secret(), secret(n) or secret(n, "alphabet").
func parseSecretFunc(expr string) (int, string, error) {
	args, err := funcArgs(expr, "secret")
	if err != nil {
		return 0, "", err
	}
	length, alphabet := 32, defaultSecretAlphabet
	if len(args) > 0 && args[0] != "" {
		if length, err = strconv.Atoi(args[0]); err != nil || length < 1 || length > 512 {
			return 0, "", fmt.Errorf("secret(): length must be an integer between 1 and 512, got %q", args[0])
		}
	}
	if len(args) > 1 {
		alphabet = strings.Trim(args[1], `"'`)
		if alphabet == "" {
			return 0, "", errors.New("secret(): alphabet cannot be empty")
		}
	}
	return length, alphabet, nil
}

// parseRandomInt parses randomInt(), randomInt(max) or randomInt(min, max).
func parseRandomInt(expr string) (int64, error) {
	args, err := funcArgs(expr, "randomInt")
	if err != nil {
		return 0, err
	}
	lo, hi := int64(0), int64(100)
	switch len(args) {
	case 0:
	case 1:
		if hi, err = strconv.ParseInt(args[0], 10, 64); err != nil {
			return 0, fmt.Errorf("randomInt(): %q is not an integer", args[0])
		}
	default:
		if lo, err = strconv.ParseInt(args[0], 10, 64); err != nil {
			return 0, fmt.Errorf("randomInt(): %q is not an integer", args[0])
		}
		if hi, err = strconv.ParseInt(args[1], 10, 64); err != nil {
			return 0, fmt.Errorf("randomInt(): %q is not an integer", args[1])
		}
	}
	if hi <= lo {
		return 0, errors.New("randomInt(): max must be greater than min")
	}
	n, err := rand.Int(rand.Reader, big.NewInt(hi-lo))
	if err != nil {
		return 0, err
	}
	return lo + n.Int64(), nil
}

func funcArgs(expr, name string) ([]string, error) {
	inner, ok := strings.CutPrefix(expr, name+"(")
	if !ok || !strings.HasSuffix(inner, ")") {
		return nil, fmt.Errorf("invalid %s() reference %q", name, expr)
	}
	inner = strings.TrimSuffix(inner, ")")
	if strings.TrimSpace(inner) == "" {
		return nil, nil
	}
	parts := strings.Split(inner, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts, nil
}

func randomString(length int, alphabet string) (string, error) {
	out := make([]byte, length)
	max := big.NewInt(int64(len(alphabet)))
	for i := range out {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		out[i] = alphabet[n.Int64()]
	}
	return string(out), nil
}
