package scheduler

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWorkerResourceCleanerRemovesWorkerStateWithoutJobAndRequeuesRequests(t *testing.T) {
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)
	worker := cleanerTestWorker("stale-worker", "default", "")
	assert.Nil(t, workerRepo.AddWorker(worker))
	assert.Nil(t, workerRepo.ScheduleContainerRequest(worker, cleanerTestRequest("container-1")))

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

func TestWorkerResourceCleanerScopesRedisOnlyWorkersToExternalMachine(t *testing.T) {
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)
	machineAWorker := cleanerTestWorker("machine-a-worker", "default", "machine-a")
	machineBWorker := cleanerTestWorker("machine-b-worker", "default", "machine-b")
	assert.Nil(t, workerRepo.AddWorker(machineAWorker))
	assert.Nil(t, workerRepo.AddWorker(machineBWorker))

	cleaner := WorkerResourceCleaner{
		PoolName:   "default",
		MachineId:  "machine-a",
		Config:     types.WorkerConfig{Namespace: "beta9"},
		KubeClient: fake.NewSimpleClientset(),
		WorkerRepo: workerRepo,
	}
	cleaner.Clean(ctx)

	_, err = workerRepo.GetWorkerById(machineAWorker.Id)
	_, workerNotFound := err.(*types.ErrWorkerNotFound)
	assert.True(t, workerNotFound)

	worker, err := workerRepo.GetWorkerById(machineBWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, machineBWorker.Id, worker.Id)
}

func TestWorkerResourceCleanerDeletesCompletedWorkerJob(t *testing.T) {
	ctx := context.Background()
	rdb, err := repo.NewRedisClientForTest()
	assert.Nil(t, err)

	workerRepo := repo.NewWorkerRedisRepositoryForTest(rdb)
	worker := cleanerTestWorker("completed-worker", "default", "machine-a")
	assert.Nil(t, workerRepo.AddWorker(worker))
	assert.Nil(t, workerRepo.ScheduleContainerRequest(worker, cleanerTestRequest("container-1")))

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
