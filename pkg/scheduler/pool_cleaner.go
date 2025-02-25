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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

type WorkerResourceCleaner struct {
	PoolName   string
	Config     types.WorkerConfig
	KubeClient *kubernetes.Clientset
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

	for _, job := range jobList.Items {
		podList, err := c.KubeClient.CoreV1().Pods(c.Config.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", job.Name),
		})
		if err != nil {
			continue
		}

		for _, pod := range podList.Items {
			c.deleteStaleJob(ctx, pod, job)
			c.deletePendingJob(ctx, pod, job)
		}
	}
}

// deleteStaleJob deletes jobs/pods that have no corresponding state in the repository
func (c *WorkerResourceCleaner) deleteStaleJob(ctx context.Context, pod corev1.Pod, job batchv1.Job) {
	workerId, ok := pod.Labels[Beta9WorkerLabelIDKey]
	if !ok {
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

	machineId := pod.Labels[Beta9MachineLabelIDKey]
	c.EventRepo.PushWorkerDeletedEvent(workerId, machineId, c.PoolName, types.DeletedWorkerReasonPodWithoutState)
}

// deletePendingJob deletes pending jobs/pods that have exceeded the age limit in a "pending" state
func (c *WorkerResourceCleaner) deletePendingJob(ctx context.Context, pod corev1.Pod, job batchv1.Job) {
	workerId, ok := pod.Labels[Beta9WorkerLabelIDKey]
	if !ok {
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

	machineId := pod.Labels[Beta9MachineLabelIDKey]
	c.EventRepo.PushWorkerDeletedEvent(workerId, machineId, c.PoolName, types.DeletedWorkerReasonPodExceededPendingAgeLimit)
}

func (c *WorkerResourceCleaner) deleteWorkerResources(ctx context.Context, workerId string, job batchv1.Job) error {
	var eg errgroup.Group

	// Remove worker state from Repository
	eg.Go(func() error {
		if err := c.WorkerRepo.RemoveWorker(workerId); err != nil {
			if _, ok := err.(*types.ErrWorkerNotFound); !ok {
				return err
			}
		}
		return nil
	})

	// Remove worker job from Kubernetes
	eg.Go(func() error {
		return c.KubeClient.BatchV1().Jobs(c.Config.Namespace).Delete(ctx, job.Name, metav1.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		})
	})

	return eg.Wait()
}
