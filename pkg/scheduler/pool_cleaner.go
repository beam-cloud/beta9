package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

type WorkerResourceCleaner struct {
	PoolName   string
	MachineId  string
	Config     types.WorkerConfig
	KubeClient kubernetes.Interface
	EventRepo  repository.EventRepository
	WorkerRepo repository.WorkerRepository
}

func (c *WorkerResourceCleaner) Clean(ctx context.Context) {
	jobList, err := c.KubeClient.BatchV1().Jobs(c.Config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s",
			Beta9WorkerLabelKey, Beta9WorkerLabelValue,
			Beta9WorkerLabelPoolNameKey, c.PoolName,
		),
	})
	if err != nil {
		return
	}

	workerJobIds := map[string]struct{}{}
	for _, job := range jobList.Items {
		if workerId := job.Labels[Beta9WorkerLabelIDKey]; workerId != "" {
			workerJobIds[workerId] = struct{}{}
		}

		podList, err := c.KubeClient.CoreV1().Pods(c.Config.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", job.Name),
		})
		if err != nil {
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

	if err := c.deleteWorkerResources(ctx, workerId, job); err != nil {
		return
	}

	c.pushWorkerDeletedEvent(workerId, machineIdForJobPod(job, pod), types.DeletedWorkerReasonPodCompleted)
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
		return
	}

	if err := c.deleteWorkerResources(ctx, workerId, job); err != nil {
		return
	}

	c.pushWorkerDeletedEvent(workerId, machineIdForJobPod(job, pod), types.DeletedWorkerReasonPodWithoutState)
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

	if time.Since(pod.CreationTimestamp.Time) < c.Config.CleanupPendingWorkerAgeLimit {
		return
	}

	if err := c.deleteWorkerResources(ctx, workerId, job); err != nil {
		return
	}

	c.pushWorkerDeletedEvent(workerId, machineIdForJobPod(job, pod), types.DeletedWorkerReasonPodExceededPendingAgeLimit)
}

func (c *WorkerResourceCleaner) deleteWorkerStatesWithoutJobs(ctx context.Context, workerJobIds map[string]struct{}) {
	workers, err := c.workerStateCandidates()
	if err != nil {
		return
	}

	for _, worker := range workers {
		if worker == nil || worker.PoolName != c.PoolName {
			continue
		}
		if _, exists := workerJobIds[worker.Id]; exists {
			continue
		}

		if err := c.deleteWorkerState(ctx, worker.Id); err != nil {
			continue
		}

		c.pushWorkerDeletedEvent(worker.Id, worker.MachineId, types.DeletedWorkerReasonWorkerStateWithoutJob)
	}
}

func (c *WorkerResourceCleaner) workerStateCandidates() ([]*types.Worker, error) {
	if c.MachineId != "" {
		return c.WorkerRepo.GetAllWorkersOnMachine(c.MachineId)
	}

	return c.WorkerRepo.GetAllWorkersInPool(c.PoolName)
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
