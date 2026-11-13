package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestWorkerRegistrationSurvivesOrphanCleanup(t *testing.T) {
	controller, cleaner, workers, kube := registrationFixture(t)
	visible, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	controller.kubeClient = registrationKubeClient(t, kube, func(ctx context.Context) error {
		close(visible) // Job/Pod are visible, but public AddWorker has not been called yet.
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	var createErr error
	go func() { _, createErr = controller.addWorkerWithId("registering", 1000, 1024, "", 0); close(finished) }()
	t.Cleanup(func() { release(); awaitRegistration(t, finished) })
	awaitRegistration(t, visible)
	cleaner.Clean(context.Background())
	_, jobErr := kube.BatchV1().Jobs("beta9").Get(context.Background(), "worker-default-registering", metav1.GetOptions{})
	release()
	awaitRegistration(t, finished)
	if createErr != nil {
		t.Fatalf("create worker: %v", createErr)
	}
	if _, err := workers.GetWorkerById("registering"); err != nil {
		t.Fatalf("registered state: %v", err)
	}
	if jobErr != nil {
		t.Fatalf("cleaner deleted a Job before registration completed: %v", jobErr)
	}
}

func TestWorkerRegistrationCleanerControls(t *testing.T) {
	for _, state := range []bool{false, true} {
		name := "orphan"
		if state {
			name = "registered"
		}
		t.Run(name, func(t *testing.T) {
			_, cleaner, workers, kube := registrationFixture(t)
			job := cleanerTestWorkerJob("worker-default-control", "control", "default", "")
			pod := cleanerTestWorkerPod("control-pod", job.Name, "control", "default", "", corev1.PodRunning)
			_, _ = kube.BatchV1().Jobs("beta9").Create(context.Background(), job, metav1.CreateOptions{})
			_, _ = kube.CoreV1().Pods("beta9").Create(context.Background(), pod, metav1.CreateOptions{})
			if state {
				_ = workers.AddWorker(&types.Worker{Id: "control", PoolName: "default"})
			}
			cleaner.Clean(context.Background())
			_, err := kube.BatchV1().Jobs("beta9").Get(context.Background(), job.Name, metav1.GetOptions{})
			if state && err != nil || !state && !apierrors.IsNotFound(err) {
				t.Fatalf("job lookup = %v, registered=%t", err, state)
			}
		})
	}
	t.Run("job appears after initial list", func(t *testing.T) {
		_, cleaner, workers, kube := registrationFixture(t)
		_ = workers.AddWorker(&types.Worker{Id: "later", PoolName: "default"})
		workers.onPoolList = func() {
			job := cleanerTestWorkerJob("worker-default-later", "later", "default", "")
			_, _ = kube.BatchV1().Jobs("beta9").Create(context.Background(), job, metav1.CreateOptions{})
		}
		cleaner.Clean(context.Background())
		if _, err := workers.GetWorkerById("later"); err != nil {
			t.Fatalf("#1901 late Job state lost: %v", err)
		}
	})
}

func TestWorkerRegistrationFailures(t *testing.T) {
	failure := errors.New("registration failed")
	for _, name := range []string{"acquire", "setup", "persist", "canceled create"} {
		t.Run(name, func(t *testing.T) {
			controller, cleaner, workers, kube := registrationFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			controller.ctx = ctx
			switch name {
			case "acquire":
				workers.acquireErr = failure
			case "persist":
				workers.persistErr = failure
			}
			controller.kubeClient = registrationKubeClient(t, kube, func(requestCtx context.Context) error {
				if name == "setup" {
					return failure
				}
				if name == "canceled create" {
					cancel()
					return requestCtx.Err()
				}
				return nil
			})
			if _, err := controller.addWorkerWithId("failed", 1000, 1024, "", 0); err == nil {
				t.Fatal("failed registration returned success")
			}
			workers.acquireErr = nil
			if _, err := workers.GetWorkerById("failed"); err == nil {
				t.Fatal("failed registration left worker state")
			}
			_, err := kube.BatchV1().Jobs("beta9").Get(context.Background(), "worker-default-failed", metav1.GetOptions{})
			if name == "acquire" && !apierrors.IsNotFound(err) {
				t.Fatal("Job created before acquiring worker lease")
			}
			cleaner.Clean(context.Background())
			_, err = kube.BatchV1().Jobs("beta9").Get(context.Background(), "worker-default-failed", metav1.GetOptions{})
			if !apierrors.IsNotFound(err) {
				t.Fatalf("failed registration's orphan Job survived: %v", err)
			}
		})
	}
}

func awaitRegistration(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker registration did not progress")
	}
}

type registrationWorkers struct {
	repository.WorkerRepository
	lease                  sync.Mutex
	mu                     sync.Mutex
	worker                 *types.Worker
	onPoolList             func()
	acquireErr, persistErr error
}

func (r *registrationWorkers) AddWorker(w *types.Worker) error {
	return r.AddWorkerWithSetup(context.Background(), w, nil)
}
func (r *registrationWorkers) AddWorkerWithSetup(ctx context.Context, w *types.Worker, setup func(context.Context) error) error {
	if r.acquireErr != nil {
		return r.acquireErr
	}
	if !r.lease.TryLock() {
		return errors.New("worker lease busy")
	}
	defer r.lease.Unlock()
	if setup != nil {
		if err := setup(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.persistErr != nil {
		return r.persistErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := *w
	r.worker = &copy
	return nil
}
func (r *registrationWorkers) GetWorkerById(id string) (*types.Worker, error) {
	if !r.lease.TryLock() {
		return nil, errors.New("worker lease busy")
	}
	defer r.lease.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.worker == nil || r.worker.Id != id {
		return nil, &types.ErrWorkerNotFound{WorkerId: id}
	}
	copy := *r.worker
	return &copy, nil
}
func (r *registrationWorkers) GetAllWorkersInPool(_ string) ([]*types.Worker, error) {
	if r.onPoolList != nil {
		hook := r.onPoolList
		r.onPoolList = nil
		hook()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.worker == nil {
		return nil, nil
	}
	copy := *r.worker
	return []*types.Worker{&copy}, nil
}
func (r *registrationWorkers) RemoveWorker(id string) error {
	if !r.lease.TryLock() {
		return errors.New("worker lease busy")
	}
	defer r.lease.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.worker == nil {
		return &types.ErrWorkerNotFound{WorkerId: id}
	}
	r.worker = nil
	return nil
}

type registrationBackend struct{ repository.BackendRepository }

func (registrationBackend) CreateToken(context.Context, uint, string, bool) (types.Token, error) {
	return types.Token{Key: "test-token"}, nil
}

type registrationRoundTripFunc func(*http.Request) (*http.Response, error)

func (f registrationRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func registrationKubeClient(t *testing.T, kube *fake.Clientset, afterVisible func(context.Context) error) *kubernetes.Clientset {
	t.Helper()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: "https://kube.invalid", ContentConfig: rest.ContentConfig{ContentType: "application/json"}, Transport: registrationRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/apis/batch/v1/namespaces/beta9/jobs" {
			return nil, errors.New("unexpected Kubernetes request")
		}
		var job batchv1.Job
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
			return nil, err
		}
		created, err := kube.BatchV1().Jobs("beta9").Create(r.Context(), &job, metav1.CreateOptions{})
		if err != nil {
			return nil, err
		}
		pod := cleanerTestWorkerPod(job.Name+"-pod", job.Name, job.Labels[Beta9WorkerLabelIDKey], "default", "", corev1.PodRunning)
		if _, err := kube.CoreV1().Pods("beta9").Create(r.Context(), pod, metav1.CreateOptions{}); err != nil {
			return nil, err
		}
		if afterVisible != nil {
			if err := afterVisible(r.Context()); err != nil {
				return nil, err
			}
		}
		created.APIVersion, created.Kind = "batch/v1", "Job"
		body, err := json.Marshal(created)
		return &http.Response{StatusCode: http.StatusCreated, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), Request: r}, err
	})})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func registrationFixture(t *testing.T) (*LocalKubernetesWorkerPoolController, *WorkerResourceCleaner, *registrationWorkers, *fake.Clientset) {
	kube := fake.NewSimpleClientset()
	workers := &registrationWorkers{}
	config := types.AppConfig{Worker: types.WorkerConfig{Namespace: "beta9"}}
	controller := &LocalKubernetesWorkerPoolController{ctx: context.Background(), name: "default", config: config, workerRepo: workers, backendRepo: registrationBackend{}, workspace: &types.Workspace{Id: 1}, kubeClient: registrationKubeClient(t, kube, nil)}
	cleaner := &WorkerResourceCleaner{PoolName: "default", Config: config.Worker, WorkerRepo: workers, KubeClient: kube}
	return controller, cleaner, workers, kube
}
