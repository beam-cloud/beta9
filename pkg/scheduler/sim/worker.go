package schedsim

import (
	"context"
	"errors"
	"maps"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Pool controls how the simulated workers of one pool behave.
type Pool struct {
	sim  *Sim
	Name string

	bootDelay   time.Duration
	idleTimeout time.Duration
	stalled     bool
	refusing    bool
	refused     int
}

// BootAfter sets how long a new worker takes to start accepting work.
func (p *Pool) BootAfter(d time.Duration) *Pool { p.bootDelay = d; return p }

// IdleAfter sets how long a worker with nothing to run stays up.
func (p *Pool) IdleAfter(d time.Duration) *Pool { p.idleTimeout = d; return p }

// Stall makes the pool's new workers never start: the cluster accepts each
// Job and its pod stays Pending, as when no node has room for it.
func (p *Pool) Stall() *Pool { p.stalled = true; return p }

// Unstall lets pending and new workers start again.
func (p *Pool) Unstall() *Pool { p.stalled = false; return p }

// RefuseWorkers makes the cluster reject the pool's new worker Jobs, so
// adding a worker fails at once.
func (p *Pool) RefuseWorkers() *Pool { p.refusing = true; return p }

// AcceptWorkers undoes RefuseWorkers.
func (p *Pool) AcceptWorkers() *Pool { p.refusing = false; return p }

// Workers returns the pool's simulated workers that have not exited, oldest
// first.
func (p *Pool) Workers() []*Worker {
	p.sim.mu.Lock()
	defer p.sim.mu.Unlock()
	var out []*Worker
	for _, worker := range p.sim.workers {
		if worker.Pool == p.Name && worker.state != workerExited {
			out = append(out, worker)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].createdAt.Equal(out[j].createdAt) {
			return out[i].createdAt.Before(out[j].createdAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Refused counts the worker Jobs the cluster rejected while the pool was
// refusing workers: each is one failed attempt to add a worker.
func (p *Pool) Refused() int {
	p.sim.mu.Lock()
	defer p.sim.mu.Unlock()
	return p.refused
}

// Started counts every worker the cluster accepted for the pool, whether or
// not it went on to start.
func (p *Pool) Started() int {
	p.sim.mu.Lock()
	defer p.sim.mu.Unlock()
	n := 0
	for _, worker := range p.sim.workers {
		if worker.Pool == p.Name {
			n++
		}
	}
	return n
}

type workerState int

const (
	workerBooting workerState = iota
	workerRunning
	workerVanished
	workerExited
)

// Worker is a simulated worker: a pod in a Kubernetes pool, or the worker of
// a machine that was bought for a pool.
type Worker struct {
	ID   string
	Pool string

	sim     *Sim
	jobName string
	// machine is the bought machine the worker runs on, or nil for a pod.
	// Such a worker lives as long as its machine: it has no Job, and it does
	// not exit when it is idle.
	machine    *machine
	state      workerState
	createdAt  time.Time
	lastWorkAt time.Time
	running    map[string]*run
	// dieOnDelivery makes the worker vanish holding the next requests it
	// takes off its queue.
	dieOnDelivery bool
	// dieOnClaim makes the worker vanish once it has claimed the next
	// request it takes, before the container has started.
	dieOnClaim bool
}

type run struct {
	container *Container
	request   *types.ContainerRequest
	endsAt    time.Time
}

// Vanish stops the worker dead: no more heartbeats, nothing it was running
// finishes, and its pod stays Running. This is a node that stopped answering.
func (w *Worker) Vanish() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	if w.state != workerExited {
		w.state = workerVanished
	}
}

// LoseRecord drops what the scheduler has on record for the worker, as Redis
// does when it evicts the key. The worker itself is untouched: it finds out
// at its next heartbeat.
func (w *Worker) LoseRecord() {
	w.sim.t.Helper()
	if err := w.sim.rdb.Del(w.sim.ctx, common.RedisKeys.SchedulerWorkerState(w.ID)).Err(); err != nil {
		w.sim.t.Fatalf("schedsim: lose record of worker %s: %v", w.ID, err)
	}
}

// VanishAfterTakingWork makes the worker vanish the moment it has taken its
// next requests off its queue, before it claims them.
func (w *Worker) VanishAfterTakingWork() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.dieOnDelivery = true
}

// VanishAfterClaimingWork makes the worker vanish the moment it has claimed
// its next request, while the container is still starting: a node that is
// lost in the middle of an image pull.
func (w *Worker) VanishAfterClaimingWork() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.dieOnClaim = true
}

// Running reports whether the worker is up and taking work.
func (w *Worker) Running() bool {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	return w.state == workerRunning
}

// Container is a submitted request and what became of it.
type Container struct {
	ID          string
	SubmittedAt time.Time
	// Runs lists each time a worker started the container, oldest first.
	Runs []Run

	spec Request
}

// Run is one start of a container on a worker.
type Run struct {
	Pool      string
	WorkerID  string
	StartedAt time.Time
	// Asked counts, by pool, the workers the scheduler had asked for on the
	// request's behalf by the time it started.
	Asked map[string]int
}

// Pool returns the pool the container last started on, or "".
func (c *Container) Pool() string {
	if len(c.Runs) == 0 {
		return ""
	}
	return c.Runs[len(c.Runs)-1].Pool
}

// tickWorkers is one pass of everything workers, the cluster and bought
// machines do: new Jobs get a pod, pods and machines boot, workers heartbeat,
// take work, finish it and exit when idle.
func (s *Sim) tickWorkers() {
	s.adoptJobs()
	s.tickMachines()

	s.mu.Lock()
	workers := make([]*Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		workers = append(workers, worker)
	}
	s.mu.Unlock()
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })

	for _, worker := range workers {
		worker.tick()
	}
}

// adoptJobs gives every worker Job a pod, as the Job controller would, and a
// simulated worker to run in it.
func (s *Sim) adoptJobs() {
	namespace := s.Config.Worker.Namespace
	jobs, err := s.kube.BatchV1().Jobs(namespace).List(s.ctx, metav1.ListOptions{})
	if err != nil {
		s.t.Fatalf("schedsim: list jobs: %v", err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		workerID := job.Labels[scheduler.Beta9WorkerLabelIDKey]
		poolName := job.Labels[scheduler.Beta9WorkerLabelPoolNameKey]

		s.mu.Lock()
		_, known := s.workers[workerID]
		s.mu.Unlock()
		if known || workerID == "" {
			continue
		}

		now := s.Clock.Now()
		labels := map[string]string{"job-name": job.Name}
		for key, value := range job.Labels {
			labels[key] = value
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: namespace, Labels: labels, CreationTimestamp: metav1.NewTime(now)},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		}
		if _, err := s.kube.CoreV1().Pods(namespace).Create(s.ctx, pod, metav1.CreateOptions{}); err != nil {
			s.t.Fatalf("schedsim: create pod for %s: %v", job.Name, err)
		}

		s.mu.Lock()
		s.workers[workerID] = &Worker{ID: workerID, Pool: poolName, sim: s, jobName: job.Name,
			createdAt: now, lastWorkAt: now, running: map[string]*run{}}
		s.mu.Unlock()
	}
}

func (w *Worker) tick() {
	s := w.sim
	pool := s.pools[w.Pool]
	now := s.Clock.Now()

	s.mu.Lock()
	state := w.state
	s.mu.Unlock()
	if state == workerExited || state == workerVanished {
		return
	}

	onMachine := w.machine != nil
	if !onMachine {
		// The cleaner deletes the Job of a worker it gave up on; the pod
		// goes with it.
		if _, err := s.kube.BatchV1().Jobs(s.Config.Worker.Namespace).Get(s.ctx, w.jobName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			w.exit(false)
			return
		}
	}

	if state == workerBooting {
		// A machine has booted by the time it runs a worker.
		if !onMachine && (pool == nil || pool.stalled || now.Sub(w.createdAt) < pool.bootDelay) {
			return
		}
		// A worker becomes available when it opens its request stream.
		if err := s.workerRepo.RecoverPendingContainerRequests(w.ID); err != nil {
			s.t.Fatalf("schedsim: worker %s recover requests: %v", w.ID, err)
		}
		if err := s.workerRepo.ToggleWorkerAvailable(w.ID, ""); err != nil {
			var notFound *types.ErrWorkerNotFound
			if errors.As(err, &notFound) {
				// A pod without a worker record is done for. A machine
				// waits for its pool to give it one.
				if !onMachine {
					w.exit(false)
				}
				return
			}
			s.t.Fatalf("schedsim: worker %s start: %v", w.ID, err)
		}
		w.setPodPhase(corev1.PodRunning)
		s.mu.Lock()
		w.state = workerRunning
		w.lastWorkAt = now
		s.mu.Unlock()
	}

	if err := s.workerRepo.SetWorkerKeepAlive(w.ID, types.WorkerKeepAlive{RequestStreamOpen: true, CPUTierProtocol: types.CPUTierProtocolVersion}); err != nil {
		// The scheduler removed this worker. A real worker cannot heartbeat
		// either and shuts down; on a machine, the agent starts another.
		if onMachine {
			w.restart()
		} else {
			w.exit(false)
		}
		return
	}

	w.finishContainers(now)
	w.startContainers(now)
	if !w.Running() {
		return
	}

	if len(w.running) > 0 {
		s.mu.Lock()
		w.lastWorkAt = now
		s.mu.Unlock()
		return
	}
	if !onMachine && pool != nil && now.Sub(w.lastWorkAt) > pool.idleTimeout && !w.holdsHeadroom() {
		w.exit(true)
	}
}

// restart is a worker process that died and is started again on its
// machine. What it was running is gone.
func (w *Worker) restart() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.state = workerBooting
	w.running = map[string]*run{}
}

// die ends the worker with its machine: nothing is cleaned up, and nothing
// it was running finishes.
func (w *Worker) die() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.state = workerExited
	w.running = map[string]*run{}
}

func (w *Worker) holdsHeadroom() bool {
	state, err := w.sim.workerRepo.GetWorkerById(w.ID)
	if err != nil {
		return true
	}
	return w.sim.schedulers[0].WorkerHoldsPoolHeadroom(state)
}

func (w *Worker) startContainers(now time.Time) {
	s := w.sim
	requests, err := s.workerRepo.GetNextContainerRequests(w.ID, 64)
	if err != nil {
		s.t.Fatalf("schedsim: worker %s take requests: %v", w.ID, err)
	}
	if w.dieOnDelivery && len(requests) > 0 {
		w.Vanish()
		return
	}
	for _, request := range requests {
		if err := s.workerRepo.AddContainerToWorker(w.ID, request.ContainerId, request.DeliveryToken); err != nil {
			// The request was cancelled or moved while it was on its way.
			continue
		}
		if w.dieOnClaim {
			// The container is this worker's now, and starting.
			w.Vanish()
			return
		}
		if err := s.containerRepo.UpdateContainerStatus(request.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS); err != nil {
			s.t.Fatalf("schedsim: worker %s start %s: %v", w.ID, request.ContainerId, err)
		}

		s.mu.Lock()
		container, ok := s.containers[request.ContainerId]
		if !ok {
			container = &Container{ID: request.ContainerId, SubmittedAt: now, spec: Request{runFor: defaultRunFor}}
			s.containers[request.ContainerId] = container
			s.order = append(s.order, request.ContainerId)
		}
		container.Runs = append(container.Runs, Run{Pool: w.Pool, WorkerID: w.ID, StartedAt: now, Asked: maps.Clone(request.GrowAttempts)})
		w.running[request.ContainerId] = &run{container: container, request: request, endsAt: now.Add(container.spec.runFor)}
		s.mu.Unlock()
	}
}

func (w *Worker) finishContainers(now time.Time) {
	s := w.sim
	ids := make([]string, 0, len(w.running))
	for id := range w.running {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		current := w.running[id]
		if now.Before(current.endsAt) {
			// Keep the container's lease, as a running worker does.
			_ = s.containerRepo.UpdateContainerStatus(id, types.ContainerStatusRunning, types.ContainerStateTtlS)
			continue
		}
		delete(w.running, id)
		_ = s.containerRepo.DeleteContainerState(id)
		_ = s.workerRepo.RemoveContainerFromWorker(w.ID, id)
		_ = s.workerRepo.UpdateWorkerCapacity(&types.Worker{Id: w.ID}, current.request, types.AddCapacity)
	}
}

// exit ends the worker. A clean exit removes its own state and completes its
// pod, which the cleaner then collects; otherwise the pod is simply gone.
func (w *Worker) exit(clean bool) {
	s := w.sim
	if clean {
		_ = s.workerRepo.RemoveWorker(w.ID)
		w.setPodPhase(corev1.PodSucceeded)
	} else {
		_ = s.kube.CoreV1().Pods(s.Config.Worker.Namespace).Delete(s.ctx, w.jobName, metav1.DeleteOptions{})
	}
	s.mu.Lock()
	w.state = workerExited
	w.running = map[string]*run{}
	s.mu.Unlock()
}

func (w *Worker) setPodPhase(phase corev1.PodPhase) {
	if w.machine != nil {
		return
	}
	s := w.sim
	pods := s.kube.CoreV1().Pods(s.Config.Worker.Namespace)
	pod, err := pods.Get(s.ctx, w.jobName, metav1.GetOptions{})
	if err != nil {
		return
	}
	pod.Status.Phase = phase
	if _, err := pods.UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		s.t.Fatalf("schedsim: update pod %s: %v", w.jobName, err)
	}
}

// Jobs returns the worker Jobs that exist in the simulated cluster.
func (s *Sim) Jobs() []batchv1.Job {
	jobs, err := s.kube.BatchV1().Jobs(s.Config.Worker.Namespace).List(s.ctx, metav1.ListOptions{})
	if err != nil {
		s.t.Fatalf("schedsim: list jobs: %v", err)
	}
	return jobs.Items
}
