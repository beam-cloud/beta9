package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestWorkerResourceCleanerRemovesWorkerStateWithoutJobAndRequeuesRequests(t *testing.T) {
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)
	worker := cleanerTestWorker("stale-worker", "default", "")
	assert.Nil(t, workerRepo.AddWorker(worker))
	scheduleCleanerRequest(t, rdb, workerRepo, worker, cleanerTestRequest("container-1"))

	cleaner := WorkerResourceCleaner{
		PoolName:   "default",
		Config:     types.WorkerConfig{Namespace: "beta9"},
		KubeClient: fake.NewSimpleClientset(),
		WorkerRepo: workerRepo,
	}
	cleaner.Clean(ctx)

	_, err = workerRepo.GetWorkerById(worker.Id)
	_, workerNotFound := err.(*types.ErrWorkerNotFound)
	assert.True(t, workerNotFound)
	assertBacklogContainsContainer(t, rdb, "container-1")
}

// A worker whose Job appears after the cleaner listed Jobs is not an orphan.
func TestWorkerResourceCleanerPreservesWorkerCreatedDuringJobScan(t *testing.T) {
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)
	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)
	worker := cleanerTestWorker("new-worker", "default", "")
	assert.Nil(t, workerRepo.AddWorker(worker))
	kubeClient := fake.NewSimpleClientset()

	cleaner := WorkerResourceCleaner{PoolName: "default", Config: types.WorkerConfig{Namespace: "beta9"}, KubeClient: kubeClient}
	cleaner.WorkerRepo = &workerRepoWithHook{WorkerRepository: workerRepo, beforePoolList: func() {
		job := cleanerTestWorkerJob("worker-default-new-worker", worker.Id, "default", "")
		_, err := kubeClient.BatchV1().Jobs("beta9").Create(ctx, job, metav1.CreateOptions{})
		assert.Nil(t, err)
	}}
	cleaner.Clean(ctx)

	preserved, err := workerRepo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, worker.Id, preserved.Id)
}

// workerRepoWithHook runs beforePoolList once, before the first pool listing.
type workerRepoWithHook struct {
	repo.WorkerRepository
	beforePoolList func()
}

func (r *workerRepoWithHook) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	if hook := r.beforePoolList; hook != nil {
		r.beforePoolList = nil
		hook()
	}
	return r.WorkerRepository.GetAllWorkersInPool(poolName)
}

func TestWorkerResourceCleanerDeletesCompletedWorkerJob(t *testing.T) {
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)
	worker := cleanerTestWorker("completed-worker", "default", "machine-a")
	assert.Nil(t, workerRepo.AddWorker(worker))
	scheduleCleanerRequest(t, rdb, workerRepo, worker, cleanerTestRequest("container-1"))

	job := cleanerTestWorkerJob("worker-default-completed-worker", worker.Id, "default", "machine-a")
	pod := cleanerTestWorkerPod("worker-default-completed-worker-pod", job.Name, worker.Id, "default", "machine-a", corev1.PodSucceeded)
	kubeClient := fake.NewSimpleClientset(job, pod)
	cleaner := WorkerResourceCleaner{
		PoolName:   "default",
		Config:     types.WorkerConfig{Namespace: "beta9"},
		KubeClient: kubeClient,
		WorkerRepo: workerRepo,
	}
	cleaner.Clean(ctx)

	_, err = workerRepo.GetWorkerById(worker.Id)
	_, workerNotFound := err.(*types.ErrWorkerNotFound)
	assert.True(t, workerNotFound)

	_, err = kubeClient.BatchV1().Jobs("beta9").Get(ctx, job.Name, metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err))
	assertBacklogContainsContainer(t, rdb, "container-1")
}

type tokenBindingBackendRepo struct {
	repo.BackendRepository
	bound map[string]string
}

func (r *tokenBindingBackendRepo) BindWorkerToken(_ context.Context, token, workerId string) (bool, error) {
	r.bound[token] = workerId
	return true, nil
}

func TestWorkerResourceCleanerBindsTokensOfPoolJobs(t *testing.T) {
	withEnv := func(job *batchv1.Job, env ...corev1.EnvVar) *batchv1.Job {
		job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "worker", Env: env}}
		return job
	}
	kubeClient := fake.NewSimpleClientset(
		withEnv(cleanerTestWorkerJob("worker-a", "worker-a", "default", ""),
			corev1.EnvVar{Name: "WORKER_ID", Value: "worker-a"}, corev1.EnvVar{Name: "WORKER_TOKEN", Value: "token-a"}),
		withEnv(cleanerTestWorkerJob("worker-b", "worker-b", "other", ""),
			corev1.EnvVar{Name: "WORKER_ID", Value: "worker-b"}, corev1.EnvVar{Name: "WORKER_TOKEN", Value: "token-b"}),
		withEnv(cleanerTestWorkerJob("worker-c", "worker-c", "default", ""),
			corev1.EnvVar{Name: "WORKER_ID", Value: "worker-c"}),
	)
	backendRepo := &tokenBindingBackendRepo{bound: map[string]string{}}
	cleaner := WorkerResourceCleaner{PoolName: "default", Config: types.WorkerConfig{Namespace: "beta9"}, KubeClient: kubeClient, BackendRepo: backendRepo}
	cleaner.BindWorkerTokens(context.Background())
	assert.Equal(t, map[string]string{"token-a": "worker-a"}, backendRepo.bound)
}

func cleanerTestWorker(workerId, poolName, machineId string) *types.Worker {
	return &types.Worker{
		Id:          workerId,
		Status:      types.WorkerStatusAvailable,
		PoolName:    poolName,
		MachineId:   machineId,
		FreeCpu:     10_000,
		FreeMemory:  10_000,
		TotalCpu:    10_000,
		TotalMemory: 10_000,
	}
}

func cleanerTestRequest(containerId string) *types.ContainerRequest {
	return &types.ContainerRequest{
		ContainerId: containerId,
		WorkspaceId: "workspace",
		StubId:      "stub",
		Cpu:         100,
		Memory:      100,
	}
}

func scheduleCleanerRequest(t *testing.T, rdb *common.RedisClient, workerRepo repo.WorkerRepository, worker *types.Worker, request *types.ContainerRequest) {
	t.Helper()
	containerRepo := repo.NewContainerRedisRepositoryForTest(rdb)
	assert.NoError(t, containerRepo.SetContainerState(request.ContainerId, &types.ContainerState{
		ContainerId: request.ContainerId,
		Status:      types.ContainerStatusPending,
	}))
	assert.NoError(t, workerRepo.ScheduleContainerRequest(worker, request))
}

func cleanerTestWorkerJob(name, workerId, poolName, machineId string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "beta9",
			Labels:    cleanerTestWorkerLabels(workerId, poolName, machineId),
		},
	}
}

func cleanerTestWorkerPod(name, jobName, workerId, poolName, machineId string, phase corev1.PodPhase) *corev1.Pod {
	labels := cleanerTestWorkerLabels(workerId, poolName, machineId)
	labels["job-name"] = jobName

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "beta9",
			Labels:    labels,
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func cleanerTestWorkerLabels(workerId, poolName, machineId string) map[string]string {
	return map[string]string{
		Beta9WorkerLabelKey:         Beta9WorkerLabelValue,
		Beta9WorkerLabelPoolNameKey: poolName,
		Beta9WorkerLabelIDKey:       workerId,
		Beta9MachineLabelIDKey:      machineId,
	}
}

func assertBacklogContainsContainer(t *testing.T, rdb *common.RedisClient, containerId string) {
	t.Helper()

	backlog, err := rdb.ZRange(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 0, -1).Result()
	assert.Nil(t, err)
	for _, raw := range backlog {
		var request types.ContainerRequest
		assert.Nil(t, json.Unmarshal([]byte(raw), &request))
		if request.ContainerId == containerId {
			return
		}
	}

	t.Fatalf("container %s was not requeued into scheduler backlog", containerId)
}

// A cleanup pass that cannot do its work leaves it for the next pass. That is
// safe, and it is also how a pool comes to hold Jobs and worker records that
// nobody removes: every failure has to be in the log.

// failingWorkerRepo is a worker repository whose calls fail with the errors
// that are set.
type failingWorkerRepo struct {
	repo.WorkerRepository
	getWorker, listPool, removeWorker error
}

func (r *failingWorkerRepo) GetWorkerById(workerId string) (*types.Worker, error) {
	if r.getWorker != nil {
		return nil, r.getWorker
	}
	return r.WorkerRepository.GetWorkerById(workerId)
}

func (r *failingWorkerRepo) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	if r.listPool != nil {
		return nil, r.listPool
	}
	return r.WorkerRepository.GetAllWorkersInPool(poolName)
}

func (r *failingWorkerRepo) RemoveWorker(workerId string) error {
	if r.removeWorker != nil {
		return r.removeWorker
	}
	return r.WorkerRepository.RemoveWorker(workerId)
}

// cleanerFixture is a cleaner for a pool with one worker Job and its pod.
type cleanerFixture struct {
	cleaner *WorkerResourceCleaner
	kube    *fake.Clientset
	workers *failingWorkerRepo
}

func newCleanerFixture(t *testing.T, phase corev1.PodPhase) *cleanerFixture {
	t.Helper()
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	job := cleanerTestWorkerJob("worker-default-w1", "w1", "default", "")
	pod := cleanerTestWorkerPod("worker-default-w1-pod", job.Name, "w1", "default", "", phase)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	f := &cleanerFixture{
		kube:    fake.NewSimpleClientset(job, pod),
		workers: &failingWorkerRepo{WorkerRepository: repo.NewWorkerRedisRepositoryForTest(rdb)},
	}
	f.cleaner = &WorkerResourceCleaner{
		PoolName:   "default",
		Config:     types.WorkerConfig{Namespace: "beta9", CleanupPendingWorkerAgeLimit: 10 * time.Minute},
		KubeClient: f.kube,
		WorkerRepo: f.workers,
	}
	return f
}

// fail makes the cluster refuse a verb on a resource.
func (f *cleanerFixture) fail(verb, resource string, err error) {
	f.kube.PrependReactor(verb, resource, func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, err
	})
}

// addWorker puts a worker of the pool on record.
func (f *cleanerFixture) addWorker(t *testing.T, id string) {
	t.Helper()
	require.NoError(t, f.workers.AddWorker(cleanerTestWorker(id, "default", "")))
}

func TestACleanupPassReportsWhatItCouldNotDo(t *testing.T) {
	refused := errors.New("the cluster said no")
	redisDown := errors.New("redis is down")

	tests := []struct {
		name  string
		phase corev1.PodPhase
		// arrange sets up what goes wrong.
		arrange func(t *testing.T, f *cleanerFixture)
		// reported is the message that has to be logged, with cause.
		reported string
		cause    error
	}{
		{
			name:     "the pool's jobs cannot be listed",
			phase:    corev1.PodRunning,
			arrange:  func(t *testing.T, f *cleanerFixture) { f.fail("list", "jobs", refused) },
			reported: "unable to list worker jobs to clean up",
			cause:    refused,
		},
		{
			name:     "a job's pods cannot be listed",
			phase:    corev1.PodRunning,
			arrange:  func(t *testing.T, f *cleanerFixture) { f.fail("list", "pods", refused) },
			reported: "unable to list the pods of a worker job",
			cause:    refused,
		},
		{
			name:  "a job that has finished cannot be deleted",
			phase: corev1.PodSucceeded,
			arrange: func(t *testing.T, f *cleanerFixture) {
				f.addWorker(t, "w1")
				f.fail("delete", "jobs", refused)
			},
			reported: "unable to delete worker",
			cause:    refused,
		},
		{
			name:     "whether a job's worker is on record cannot be read",
			phase:    corev1.PodRunning,
			arrange:  func(t *testing.T, f *cleanerFixture) { f.workers.getWorker = redisDown },
			reported: "unable to read the worker of a job",
			cause:    redisDown,
		},
		{
			name:     "a job whose worker is not on record cannot be deleted",
			phase:    corev1.PodRunning,
			arrange:  func(t *testing.T, f *cleanerFixture) { f.fail("delete", "jobs", refused) },
			reported: "unable to delete worker",
			cause:    refused,
		},
		{
			name:  "a job that has been pending for too long cannot be deleted",
			phase: corev1.PodPending,
			arrange: func(t *testing.T, f *cleanerFixture) {
				f.addWorker(t, "w1")
				f.fail("delete", "jobs", refused)
			},
			reported: "unable to delete worker",
			cause:    refused,
		},
		{
			name:     "the pool's workers cannot be listed",
			phase:    corev1.PodRunning,
			arrange:  func(t *testing.T, f *cleanerFixture) { f.workers.listPool = redisDown },
			reported: "unable to list the pool's workers",
			cause:    redisDown,
		},
		{
			name:  "whether a worker on record has a job cannot be read",
			phase: corev1.PodRunning,
			arrange: func(t *testing.T, f *cleanerFixture) {
				f.addWorker(t, "w1")
				f.addWorker(t, "no-job")
				f.fail("get", "jobs", refused)
			},
			reported: "unable to read the job of a worker",
			cause:    refused,
		},
		{
			name:  "a worker on record that has no job cannot be removed",
			phase: corev1.PodRunning,
			arrange: func(t *testing.T, f *cleanerFixture) {
				f.addWorker(t, "w1")
				f.addWorker(t, "no-job")
				f.workers.removeWorker = redisDown
			},
			reported: "unable to remove the record of a worker that has no job",
			cause:    redisDown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newCleanerFixture(t, test.phase)
			test.arrange(t, f)
			logged := captureLog(t)

			f.cleaner.Clean(context.Background())

			require.Contains(t, logged.String(), `"level":"error"`)
			require.Contains(t, logged.String(), test.reported)
			require.Contains(t, logged.String(), test.cause.Error())
			require.Contains(t, logged.String(), `"pool_name":"default"`)
		})
	}
}

// What a pass leaves undone, it leaves as it found it.
func TestACleanupPassThatFailsRemovesNothing(t *testing.T) {
	ctx := context.Background()

	t.Run("a job stays when it cannot be told whether its worker is on record", func(t *testing.T) {
		f := newCleanerFixture(t, corev1.PodRunning)
		f.workers.getWorker = errors.New("redis is down")

		f.cleaner.Clean(ctx)

		_, err := f.kube.BatchV1().Jobs("beta9").Get(ctx, "worker-default-w1", metav1.GetOptions{})
		require.NoError(t, err)
	})

	t.Run("a worker stays on record when it cannot be told whether it has a job", func(t *testing.T) {
		f := newCleanerFixture(t, corev1.PodRunning)
		f.addWorker(t, "w1")
		f.addWorker(t, "no-job")
		f.fail("get", "jobs", errors.New("the cluster said no"))

		f.cleaner.Clean(ctx)

		_, err := f.workers.GetWorkerById("no-job")
		require.NoError(t, err)
	})

	t.Run("and goes once it can", func(t *testing.T) {
		f := newCleanerFixture(t, corev1.PodRunning)
		f.addWorker(t, "w1")
		f.addWorker(t, "no-job")
		logged := captureLog(t)

		f.cleaner.Clean(ctx)

		_, err := f.workers.GetWorkerById("no-job")
		var notFound *types.ErrWorkerNotFound
		require.ErrorAs(t, err, &notFound)
		_, err = f.kube.BatchV1().Jobs("beta9").Get(ctx, "worker-default-w1", metav1.GetOptions{})
		require.NoError(t, err, "a job whose worker is on record stays")
		require.NotContains(t, logged.String(), `"level":"error"`, "a pass that does its work reports no failure")
	})
}

// Every scheduler replica runs the cleanup tick for a pool. One cleans up
// after its workers at a time: a replica that finds another at it leaves the
// pass to that one, which is no failure.

// cleanupFixture is a Kubernetes pool with a finished worker Job to clean
// up, and the tick that does it.
type cleanupFixture struct {
	*cleanerFixture
	rdb  *common.RedisClient
	tick func()
}

func newCleanupFixture(t *testing.T) *cleanupFixture {
	t.Helper()
	f := &cleanupFixture{cleanerFixture: newCleanerFixture(t, corev1.PodSucceeded)}
	rdb, err := repo.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	f.rdb = rdb

	controller := &LocalKubernetesWorkerPoolController{
		poolIdentity:   poolIdentity{ctx: context.Background(), name: f.cleaner.PoolName},
		workerPoolRepo: repo.NewWorkerPoolRedisRepositoryForTest(rdb),
	}
	f.tick = func() { controller.cleanupTick(f.cleaner) }
	return f
}

func (f *cleanupFixture) jobCleanedUp(t *testing.T) bool {
	t.Helper()
	jobs, err := f.kube.BatchV1().Jobs("beta9").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return len(jobs.Items) == 0
}

func TestTheCleanupTickLeavesThePassToTheReplicaThatIsAtIt(t *testing.T) {
	f := newCleanupFixture(t)
	other := repo.NewWorkerPoolRedisRepositoryForTest(f.rdb)
	logged := captureLog(t)

	require.NoError(t, other.WithWorkerPoolCleanerLock(f.cleaner.PoolName, f.tick))
	require.False(t, f.jobCleanedUp(t))
	require.Empty(t, logged.String())

	f.tick()
	require.True(t, f.jobCleanedUp(t))
}

func TestTheCleanupTickReportsALockItCouldNotAskFor(t *testing.T) {
	f := newCleanupFixture(t)
	require.NoError(t, f.rdb.HSet(context.Background(), common.RedisKeys.WorkerPoolCleanerLock(f.cleaner.PoolName), "not", "a lock").Err())
	logged := captureLog(t)

	f.tick()

	require.False(t, f.jobCleanedUp(t))
	require.Contains(t, logged.String(), `"level":"error"`)
	require.Contains(t, logged.String(), `"pool_name":"default"`)
}
