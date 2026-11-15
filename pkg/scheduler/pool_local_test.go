package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedbatchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	k8stesting "k8s.io/client-go/testing"
)

func TestLocalWorkerPrometheusConfigurationSupportsHostNetworkBinPacking(t *testing.T) {
	prometheus := string(types.MetricsCollectorPrometheus)
	none := string(types.MetricsCollectorNone)
	openmeter := string(types.MetricsCollectorOpenMeter)
	tests := []struct {
		name, collector            string
		hostNetwork, scrapeWorkers bool
		wantPort, wantConfigScrape bool
		wantCollector              string
	}{
		{"host network", prometheus, true, true, false, false, none},
		{"pod network", prometheus, false, true, true, true, prometheus},
		{"scraping disabled", prometheus, false, false, false, false, prometheus},
		{"openmeter", openmeter, true, true, false, true, openmeter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &LocalKubernetesWorkerPoolController{
				poolIdentity: poolIdentity{name: "test-pool"},
				config: types.AppConfig{
					Monitoring: types.MonitoringConfig{
						MetricsCollector: tt.collector,
						Prometheus:       types.PrometheusConfig{Port: 9090, ScrapeWorkers: tt.scrapeWorkers},
					},
					Worker: types.WorkerConfig{HostNetwork: tt.hostNetwork},
				},
			}

			job, _ := controller.createWorkerJob("worker-1", 0, 0, "", 0, "token")
			container := job.Spec.Template.Spec.Containers[0]
			if got := job.Spec.Template.Labels[PrometheusScrapeKey]; got != strconv.FormatBool(tt.wantPort) {
				t.Fatalf("scrape label = %q", got)
			}
			_, hasPort := job.Spec.Template.Labels[PrometheusPortKey]
			if hasPort != tt.wantPort || len(container.Ports) != 0 {
				t.Fatalf("port label = %t, ports = %+v", hasPort, container.Ports)
			}

			config := workerConfigFromEnvironment(t, container.Env)
			if got := config.Monitoring; got.MetricsCollector != tt.wantCollector || got.Prometheus.ScrapeWorkers != tt.wantConfigScrape {
				t.Fatalf("worker monitoring config = %+v", got)
			}
		})
	}
}

func TestLocalWorkerConfigUsesGatewayServiceHostname(t *testing.T) {
	controller := &LocalKubernetesWorkerPoolController{config: types.AppConfig{
		GatewayService: types.GatewayServiceConfig{Host: "beta9-gateway"},
		Worker:         types.WorkerConfig{UseGatewayServiceHostname: true},
	}}
	controller.config.GatewayService.GRPC = types.GRPCConfig{ExternalHost: "0.tcp.ngrok.io", ExternalPort: 12345, TLS: true, Port: 1993}
	controller.config.GatewayService.HTTP = types.HTTPConfig{ExternalHost: "public.example.com", ExternalPort: 443, TLS: true, Port: 1994}

	job, _ := controller.createWorkerJob("worker-1", 0, 0, "", 0, "token")
	config := workerConfigFromEnvironment(t, job.Spec.Template.Spec.Containers[0].Env)
	grpc := config.GatewayService.GRPC
	if grpc.ExternalHost != "beta9-gateway" || grpc.ExternalPort != 1993 || grpc.TLS {
		t.Fatalf("worker gRPC config = %+v", grpc)
	}
	http := config.GatewayService.HTTP
	if http.ExternalHost != "beta9-gateway" || http.ExternalPort != 1994 || http.TLS {
		t.Fatalf("worker HTTP config = %+v", http)
	}
}

func TestLocalWorkerJobOmitsControlPlaneCredentials(t *testing.T) {
	controller := &LocalKubernetesWorkerPoolController{config: types.AppConfig{
		Database: types.DatabaseConfig{
			Postgres: types.PostgresConfig{Password: "postgres-password", EncryptionKey: "sk_encryption-key"},
			Redis:    types.RedisConfig{Password: "redis-password"},
			S2:       types.S2Config{ApiKey: "s2-key"},
		},
		Storage:   types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{DefaultAccessKey: "storage-access-key", DefaultSecretKey: "storage-secret-key"}},
		Providers: types.ProviderConfig{Hetzner: types.HetznerProviderConfig{ApiToken: "hetzner-token"}},
		Tailscale: types.TailscaleConfig{AuthKey: "tailscale-key", AgentAuthKey: "tailscale-agent-key"},
	}}
	controller.config.GatewayService.CreditGate.AuthToken = "credit-gate-token"
	controller.config.ManagedCompute.Billing.AuthToken = "billing-token"
	controller.config.ManagedCompute.Billing.PoolRoutes = []types.ManagedComputeBillingPoolRouteConfig{{AuthToken: "billing-route-token"}}
	controller.config.ImageService.BuildRegistryCredentials.Credentials = map[string]string{"AWS_SECRET_ACCESS_KEY": "build-registry-secret"}
	controller.config.ImageService.Registries.Docker.Password = "docker-password"
	controller.config.ImageService.Registries.S3.AccessKey = "image-bucket-access-key"
	controller.config.ImageService.Registries.S3.SecretKey = "image-bucket-secret"
	controller.config.Cache.Server.Metadata.RedisPasswd = "cache-redis-password"
	controller.config.Cache.Server.Metadata.ValkeyConfig.Password = "valkey-password"
	controller.config.Cache.Coordinator.Token = "coordinator-token"
	controller.config.Cache.Client.Token = "cache-client-token"
	controller.config.Monitoring.Prometheus.AgentPassword = "prometheus-agent-password"

	job, _ := controller.createWorkerJob("worker-1", 0, 0, "", 0, "token")
	pod := job.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatal("worker pod mounts a service account token")
	}
	var configJSON string
	for _, variable := range pod.Containers[0].Env {
		if variable.Name == "CONFIG_JSON" {
			configJSON = variable.Value
		}
	}
	for _, secret := range []string{
		"postgres-password", "sk_encryption-key", "redis-password", "storage-access-key", "storage-secret-key",
		"hetzner-token", "tailscale-key", "tailscale-agent-key", "credit-gate-token", "build-registry-secret",
		"docker-password", "cache-redis-password", "valkey-password", "coordinator-token", "prometheus-agent-password",
		"billing-token",
	} {
		if strings.Contains(configJSON, secret) {
			t.Errorf("worker config contains %q", secret)
		}
	}
	for _, credential := range []string{"s2-key", "image-bucket-access-key", "image-bucket-secret", "cache-client-token", "billing-route-token"} {
		if !strings.Contains(configJSON, credential) {
			t.Errorf("worker config lacks %q", credential)
		}
	}
	if controller.config.Database.Postgres.Password != "postgres-password" {
		t.Fatal("worker config changed the gateway config")
	}
}

func TestLocalWorkerConfigKeepsBillingRouteTokens(t *testing.T) {
	controller := &LocalKubernetesWorkerPoolController{}
	controller.config.ManagedCompute.Billing.AuthToken = "billing-token"
	controller.config.ManagedCompute.Billing.PoolRoutes = []types.ManagedComputeBillingPoolRouteConfig{
		{PoolNamePrefix: "tama-", AuthToken: "route-token"},
		{PoolNamePrefix: "inherits-"},
	}

	job, _ := controller.createWorkerJob("worker-1", 0, 0, "", 0, "token")
	billing := workerConfigFromEnvironment(t, job.Spec.Template.Spec.Containers[0].Env).ManagedCompute.Billing
	if billing.AuthToken != "" || len(billing.PoolRoutes) != 2 ||
		billing.PoolRoutes[0].AuthToken != "route-token" || billing.PoolRoutes[1].AuthToken != "billing-token" {
		t.Fatalf("worker billing config = %+v", billing)
	}
	if controller.config.ManagedCompute.Billing.PoolRoutes[1].AuthToken != "" {
		t.Fatal("worker config changed the gateway config")
	}
}

func workerConfigFromEnvironment(t *testing.T, env []corev1.EnvVar) types.AppConfig {
	t.Helper()
	for _, variable := range env {
		if variable.Name != "CONFIG_JSON" {
			continue
		}
		var config types.AppConfig
		if err := json.Unmarshal([]byte(variable.Value), &config); err != nil {
			t.Fatalf("unmarshal worker config: %v", err)
		}
		return config
	}
	t.Fatal("CONFIG_JSON is missing")
	return types.AppConfig{}
}

func TestApplyJobResourceOverhead(t *testing.T) {
	schedulable := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4000m"),
		corev1.ResourceMemory: resource.MustParse("8192Mi"),
	}

	t.Run("adds overhead to the pod without touching schedulable capacity", func(t *testing.T) {
		out := applyJobResourceOverhead(schedulable, types.JobResourceOverheadConfig{CPU: "1000m", Memory: "1Gi"})
		if got := out[corev1.ResourceCPU]; got.MilliValue() != 5000 {
			t.Fatalf("cpu = %s, want 5000m", got.String())
		}
		if got := out[corev1.ResourceMemory]; got.Value() != 9<<30 {
			t.Fatalf("memory = %s, want 9Gi", got.String())
		}
		if got := schedulable[corev1.ResourceCPU]; got.MilliValue() != 4000 {
			t.Fatalf("schedulable cpu mutated to %s", got.String())
		}
	})

	t.Run("empty, zero and invalid overhead leave resources unchanged", func(t *testing.T) {
		for _, cfg := range []types.JobResourceOverheadConfig{
			{},
			{CPU: "0", Memory: "0"},
			{CPU: "lots", Memory: "-1Gi"},
			// Quantities from the other resource family parse but are nonsense.
			{CPU: "512Mi", Memory: "500m"},
		} {
			out := applyJobResourceOverhead(schedulable, cfg)
			if got := out[corev1.ResourceCPU]; got.MilliValue() != 4000 {
				t.Fatalf("%+v: cpu = %s, want 4000m", cfg, got.String())
			}
			if got := out[corev1.ResourceMemory]; got.Value() != 8<<30 {
				t.Fatalf("%+v: memory = %s, want 8Gi", cfg, got.String())
			}
		}
	})
}

// A Kubernetes pool adds a worker in three steps: a token for the worker, a
// Job that carries the token, and the worker's record. A worker that is not
// added must leave no token that still works.

// workerTokens is the part of the database that adding a worker uses.
type workerTokens struct {
	repo.BackendRepository
	// workspaceErr is why the admin workspace cannot be found, if set.
	workspaceErr error

	mu      sync.Mutex
	created []string
	revoked []string
}

func (b *workerTokens) GetAdminWorkspace(context.Context) (*types.Workspace, error) {
	if b.workspaceErr != nil {
		return nil, b.workspaceErr
	}
	return &types.Workspace{Id: 1, ExternalId: "admin"}, nil
}

func (b *workerTokens) CreateWorkerToken(_ context.Context, _ uint, _ string, workerId string) (types.Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := fmt.Sprintf("token-%d", len(b.created)+1)
	b.created = append(b.created, id)
	return types.Token{ExternalId: id, Key: "key-of-" + id, WorkerID: workerId}, nil
}

func (b *workerTokens) RevokeTokenByExternalId(ctx context.Context, externalId string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.revoked = append(b.revoked, externalId)
	return "key-of-" + externalId, nil
}

// unreliableRecords is a worker repository whose AddWorker reports an error,
// after writing the record if written is set.
type unreliableRecords struct {
	repo.WorkerRepository
	err     error
	written bool
}

func (r *unreliableRecords) AddWorker(worker *types.Worker) error {
	if r.written {
		if err := r.WorkerRepository.AddWorker(worker); err != nil {
			return err
		}
	}
	return r.err
}

// localPoolFixture is a Kubernetes pool, with the cluster, the worker records
// and the tokens it adds workers to.
type localPoolFixture struct {
	controller *LocalKubernetesWorkerPoolController
	kube       *fake.Clientset
	tokens     *workerTokens
	workers    repo.WorkerRepository
	stop       context.CancelFunc
}

func newLocalPoolFixture(t *testing.T) *localPoolFixture {
	t.Helper()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	f := &localPoolFixture{
		kube:    fake.NewSimpleClientset(),
		tokens:  &workerTokens{},
		workers: repo.NewWorkerRedisRepositoryForTest(rdb),
		stop:    cancel,
	}
	config := types.AppConfig{}
	config.Worker.Namespace = "beta9"
	f.controller = &LocalKubernetesWorkerPoolController{
		poolIdentity: poolIdentity{ctx: ctx, name: "cpu"},
		config:       config,
		kubeClient:   f.kube,
		workerRepo:   f.workers,
		backendRepo:  f.tokens,
	}
	return f
}

func (f *localPoolFixture) jobs(t *testing.T) []batchv1.Job {
	t.Helper()
	jobs, err := f.kube.BatchV1().Jobs("beta9").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return jobs.Items
}

func (f *localPoolFixture) workersOnRecord(t *testing.T) []*types.Worker {
	t.Helper()
	workers, err := f.workers.GetAllWorkersInPool("cpu")
	require.NoError(t, err)
	return workers
}

func TestAWorkerThatIsAddedKeepsItsToken(t *testing.T) {
	f := newLocalPoolFixture(t)

	worker, err := f.controller.AddWorker(1000, 1024, 0)

	require.NoError(t, err)
	require.Equal(t, []string{"token-1"}, f.tokens.created)
	require.Empty(t, f.tokens.revoked)
	require.Len(t, f.jobs(t), 1)
	onRecord := f.workersOnRecord(t)
	require.Len(t, onRecord, 1)
	require.Equal(t, worker.Id, onRecord[0].Id)
}

func TestAWorkerWhoseJobTheClusterRefusesLeavesNoTokenThatWorks(t *testing.T) {
	f := newLocalPoolFixture(t)
	refused := errors.New("exceeded quota")
	f.kube.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, refused
	})

	// The pool's sizer asks again every second for as long as it is short.
	for range 3 {
		_, err := f.controller.AddWorker(1000, 1024, 0)
		require.ErrorIs(t, err, refused)
	}

	require.Equal(t, []string{"token-1", "token-2", "token-3"}, f.tokens.created)
	require.Equal(t, f.tokens.created, f.tokens.revoked)
	require.Empty(t, f.workersOnRecord(t))
}

func TestAWorkerThatCannotBePutOnRecordLeavesNoTokenThatWorks(t *testing.T) {
	f := newLocalPoolFixture(t)
	redisDown := errors.New("redis is down")
	f.controller.workerRepo = &unreliableRecords{WorkerRepository: f.workers, err: redisDown}

	_, err := f.controller.AddWorker(1000, 1024, 0)

	require.ErrorIs(t, err, redisDown)
	require.Equal(t, []string{"token-1"}, f.tokens.revoked)
	require.Empty(t, f.workersOnRecord(t))
	// The Job is left to the cleaner, which removes a Job that has no worker
	// on record.
	require.Len(t, f.jobs(t), 1)
}

// Putting a worker on record can report an error after the record is
// written: when the lock that guards the write cannot be released, say. The
// scheduler then has a worker to wait for, and the worker has to be able to
// start.
func TestAWorkerThatIsOnRecordKeepsItsTokenThoughAddingItReportedAnError(t *testing.T) {
	f := newLocalPoolFixture(t)
	lockLost := errors.New("lock was lost")
	f.controller.workerRepo = &unreliableRecords{WorkerRepository: f.workers, err: lockLost, written: true}

	_, err := f.controller.AddWorker(1000, 1024, 0)

	require.ErrorIs(t, err, lockLost)
	require.Len(t, f.workersOnRecord(t), 1)
	require.Empty(t, f.tokens.revoked)
}

func TestAddingAWorkerSaysWhyTheWorkspaceCouldNotBeFound(t *testing.T) {
	f := newLocalPoolFixture(t)
	f.tokens.workspaceErr = errors.New("connection refused")

	_, err := f.controller.AddWorker(1000, 1024, 0)

	require.ErrorIs(t, err, f.tokens.workspaceErr)
	require.ErrorContains(t, err, "cpu", "the error names the pool")
	require.Empty(t, f.tokens.created)
	require.Empty(t, f.jobs(t))
}

// A pool's sizer and the requests that grow the pool add workers at the same
// time. Run with -race.
func TestWorkersAreAddedAtTheSameTime(t *testing.T) {
	f := newLocalPoolFixture(t)
	const callers = 8

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.controller.AddWorker(1000, 1024, 0)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, f.jobs(t), callers)
	require.Len(t, f.workersOnRecord(t), callers)
}

// silentCluster is a cluster that does not answer when a Job is created,
// until the caller gives up.
type silentCluster struct {
	kubernetes.Interface
	asked chan context.Context
}

func (c *silentCluster) BatchV1() typedbatchv1.BatchV1Interface {
	return &silentBatch{BatchV1Interface: c.Interface.BatchV1(), cluster: c}
}

type silentBatch struct {
	typedbatchv1.BatchV1Interface
	cluster *silentCluster
}

func (b *silentBatch) Jobs(namespace string) typedbatchv1.JobInterface {
	return &silentJobs{JobInterface: b.BatchV1Interface.Jobs(namespace), cluster: b.cluster}
}

type silentJobs struct {
	typedbatchv1.JobInterface
	cluster *silentCluster
}

func (j *silentJobs) Create(ctx context.Context, _ *batchv1.Job, _ metav1.CreateOptions) (*batchv1.Job, error) {
	j.cluster.asked <- ctx
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestAddingAWorkerDoesNotWaitForeverForTheCluster(t *testing.T) {
	f := newLocalPoolFixture(t)
	cluster := &silentCluster{Interface: f.kube, asked: make(chan context.Context, 1)}
	f.controller.kubeClient = cluster

	result := make(chan error, 1)
	go func() {
		_, err := f.controller.AddWorker(1000, 1024, 0)
		result <- err
	}()

	// The cluster is given a limited time to answer.
	var asked context.Context
	select {
	case asked = <-cluster.asked:
	case <-time.After(5 * time.Second):
		t.Fatal("the cluster was not asked for a Job")
	}
	deadline, limited := asked.Deadline()
	require.True(t, limited)
	require.WithinDuration(t, time.Now().Add(workerJobCreateTimeout), deadline, 5*time.Second)

	// And the wait ends with the pool, however long is left.
	f.stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("adding a worker outlived its pool")
	}
	require.Equal(t, []string{"token-1"}, f.tokens.revoked, "the token is revoked though the pool's context has ended")
}
