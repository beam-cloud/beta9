package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

type WorkerResourceCleaner struct {
	PoolName    string
	Config      types.WorkerConfig
	KubeClient  kubernetes.Interface
	EventRepo   repository.EventRepository
	WorkerRepo  repository.WorkerRepository
	BackendRepo repository.BackendRepository
	// Now is the clock pod ages are measured on. Nil means the wall clock.
	Now func() time.Time
}

func (c *WorkerResourceCleaner) now() time.Time {
	if c.Now == nil {
		return time.Now()
	}
	return c.Now()
}

func (c *WorkerResourceCleaner) listJobs(ctx context.Context) (*batchv1.JobList, error) {
	return c.KubeClient.BatchV1().Jobs(c.Config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s",
			Beta9WorkerLabelKey, Beta9WorkerLabelValue,
			Beta9WorkerLabelPoolNameKey, c.PoolName,
		),
	})
}

// BindWorkerTokens binds the tokens of running workers to their worker IDs.
// Workers started before tokens were bound cannot reach the gateway otherwise.
func (c *WorkerResourceCleaner) BindWorkerTokens(ctx context.Context) {
	jobList, err := c.listJobs(ctx)
	if err != nil {
		log.Error().Err(err).Str("pool_name", c.PoolName).Msg("unable to list worker jobs to bind tokens")
		return
	}
	for _, job := range jobList.Items {
		c.bindWorkerToken(ctx, job)
	}
}

func (c *WorkerResourceCleaner) bindWorkerToken(ctx context.Context, job batchv1.Job) {
	if c.BackendRepo == nil {
		return
	}
	var workerId, token string
	for _, container := range job.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			switch env.Name {
			case "WORKER_ID":
				workerId = env.Value
			case "WORKER_TOKEN":
				token = env.Value
			}
		}
	}
	if workerId == "" || token == "" {
		return
	}
	if bound, err := c.BackendRepo.BindWorkerToken(ctx, token, workerId); err != nil || !bound {
		log.Error().Err(err).Str("worker_id", workerId).Msg("unable to bind worker token")
	}
}

// Clean is one cleanup pass over the pool. It removes the worker Jobs that
// have finished, that have no worker on record or that have been pending for
// too long, and the workers on record that have no Job. On the way it binds
// the token of each Job's worker (see BindWorkerTokens). What it cannot do it
// reports and leaves for the next pass.
func (c *WorkerResourceCleaner) Clean(ctx context.Context) {
	jobList, err := c.listJobs(ctx)
	if err != nil {
		log.Error().Err(err).Str("pool_name", c.PoolName).Msg("unable to list worker jobs to clean up")
		return
	}

	workerJobIds := map[string]struct{}{}
	for _, job := range jobList.Items {
		c.bindWorkerToken(ctx, job)
		if workerId := job.Labels[Beta9WorkerLabelIDKey]; workerId != "" {
			workerJobIds[workerId] = struct{}{}
		}

		podList, err := c.KubeClient.CoreV1().Pods(c.Config.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", job.Name),
		})
		if err != nil {
			log.Error().Err(err).Str("pool_name", c.PoolName).Str("job_name", job.Name).Msg("unable to list the pods of a worker job")
			continue
		}

		for _, pod := range podList.Items {
			c.deleteCompletedJob(ctx, pod, job)
			c.deleteStaleJob(ctx, pod, job)
			c.deletePendingJob(ctx, pod, job)
		}
	}

	c.deleteWorkerStatesWithoutJobs(ctx, workerJobIds)
}

func (c *WorkerResourceCleaner) deleteCompletedJob(ctx context.Context, pod corev1.Pod, job batchv1.Job) {
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
	default:
		return
	}

	workerId := workerIdForJobPod(job, pod)
	if workerId == "" {
		return
	}

	c.deleteWorker(ctx, workerId, job, pod, types.DeletedWorkerReasonPodCompleted)
}

// deleteStaleJob deletes jobs/pods that have no corresponding state in the repository
func (c *WorkerResourceCleaner) deleteStaleJob(ctx context.Context, pod corev1.Pod, job batchv1.Job) {
	workerId := workerIdForJobPod(job, pod)
	if workerId == "" {
		return
	}

	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return
	}

	_, err := c.WorkerRepo.GetWorkerById(workerId)
	if err == nil {
		return
	}

	if _, ok := err.(*types.ErrWorkerNotFound); !ok {
		// The worker may well be on record, so its Job stays.
		log.Error().Err(err).Str("pool_name", c.PoolName).Str("worker_id", workerId).Str("job_name", job.Name).Msg("unable to read the worker of a job")
		return
	}

	c.deleteWorker(ctx, workerId, job, pod, types.DeletedWorkerReasonPodWithoutState)
}

// deletePendingJob deletes pending jobs/pods that have exceeded the age limit in a "pending" state
func (c *WorkerResourceCleaner) deletePendingJob(ctx context.Context, pod corev1.Pod, job batchv1.Job) {
	workerId := workerIdForJobPod(job, pod)
	if workerId == "" {
		return
	}

	if pod.Status.Phase != corev1.PodPending {
		return
	}

	if c.now().Sub(pod.CreationTimestamp.Time) < c.Config.CleanupPendingWorkerAgeLimit {
		return
	}

	c.deleteWorker(ctx, workerId, job, pod, types.DeletedWorkerReasonPodExceededPendingAgeLimit)
}

// deleteWorker removes a worker's record and its Job, and reports the worker
// deleted for the reason given.
func (c *WorkerResourceCleaner) deleteWorker(ctx context.Context, workerId string, job batchv1.Job, pod corev1.Pod, reason types.DeletedWorkerReason) {
	if err := c.deleteWorkerResources(ctx, workerId, job); err != nil {
		log.Error().Err(err).Str("pool_name", c.PoolName).Str("worker_id", workerId).Str("job_name", job.Name).
			Str("reason", string(reason)).Msg("unable to delete worker")
		return
	}

	c.pushWorkerDeletedEvent(workerId, machineIdForJobPod(job, pod), reason)
}

func (c *WorkerResourceCleaner) deleteWorkerStatesWithoutJobs(ctx context.Context, workerJobIds map[string]struct{}) {
	workers, err := c.WorkerRepo.GetAllWorkersInPool(c.PoolName)
	if err != nil {
		log.Error().Err(err).Str("pool_name", c.PoolName).Msg("unable to list the pool's workers to find those without a job")
		return
	}

	for _, worker := range workers {
		if worker == nil || worker.PoolName != c.PoolName {
			continue
		}
		if _, exists := workerJobIds[worker.Id]; exists {
			continue
		}
		jobName := fmt.Sprintf("%s-%s-%s", Beta9WorkerJobPrefix, c.PoolName, worker.Id)
		_, err := c.KubeClient.BatchV1().Jobs(c.Config.Namespace).Get(ctx, jobName, metav1.GetOptions{})
		if err == nil {
			// The job may have been created after the list at the start of this
			// cleanup pass. Only delete state after a point-in-time recheck.
			continue
		}
		if !apierrors.IsNotFound(err) {
			log.Error().Err(err).Str("pool_name", c.PoolName).Str("worker_id", worker.Id).Str("job_name", jobName).Msg("unable to read the job of a worker")
			continue
		}

		if err := c.deleteWorkerState(ctx, worker.Id); err != nil {
			log.Error().Err(err).Str("pool_name", c.PoolName).Str("worker_id", worker.Id).Msg("unable to remove the record of a worker that has no job")
			continue
		}

		c.pushWorkerDeletedEvent(worker.Id, worker.MachineId, types.DeletedWorkerReasonWorkerStateWithoutJob)
	}
}

func (c *WorkerResourceCleaner) deleteWorkerResources(ctx context.Context, workerId string, job batchv1.Job) error {
	var eg errgroup.Group

	// Remove worker state from Repository
	eg.Go(func() error {
		return c.deleteWorkerState(ctx, workerId)
	})

	// Remove worker job from Kubernetes
	eg.Go(func() error {
		err := c.KubeClient.BatchV1().Jobs(c.Config.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		})
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	})

	return eg.Wait()
}

func (c *WorkerResourceCleaner) deleteWorkerState(_ context.Context, workerId string) error {
	if err := c.WorkerRepo.RemoveWorker(workerId); err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); !ok {
			return err
		}
	}
	return nil
}

func (c *WorkerResourceCleaner) pushWorkerDeletedEvent(workerId, machineId string, reason types.DeletedWorkerReason) {
	if c.EventRepo == nil {
		return
	}

	c.EventRepo.PushWorkerDeletedEvent(workerId, machineId, c.PoolName, reason)
}

func workerIdForJobPod(job batchv1.Job, pod corev1.Pod) string {
	if workerId := pod.Labels[Beta9WorkerLabelIDKey]; workerId != "" {
		return workerId
	}
	return job.Labels[Beta9WorkerLabelIDKey]
}

func machineIdForJobPod(job batchv1.Job, pod corev1.Pod) string {
	if machineId := pod.Labels[Beta9MachineLabelIDKey]; machineId != "" {
		return machineId
	}
	return job.Labels[Beta9MachineLabelIDKey]
}
