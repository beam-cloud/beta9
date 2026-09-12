package managedendpoint

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seen is one request the hosted layer made to an execution service.
type seen struct {
	method, path, authorization, body string
	headers                           http.Header
}

// fakeGateway stands in for the gateway's HTTP handler: the existing
// execution services (task queue, ASGI, task API) the hosted layer dispatches to.
type fakeGateway struct {
	mu       sync.Mutex
	requests []seen
	respond  func(w http.ResponseWriter, r *http.Request, body string)
}

func (g *fakeGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	g.mu.Lock()
	g.requests = append(g.requests, seen{r.Method, r.URL.RequestURI(), r.Header.Get("Authorization"), string(body), r.Header.Clone()})
	g.mu.Unlock()
	g.respond(w, r, string(body))
}

func (g *fakeGateway) calls() []seen {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]seen(nil), g.requests...)
}

// taskRecord makes the backend report one task row for GetTaskWithRelated.
func taskRecord(mock sqlmock.Sqlmock, id string, status types.TaskStatus, containerID string) {
	mock.ExpectQuery("FROM task t").WithArgs(id).WillReturnRows(
		sqlmock.NewRows([]string{"external_id", "status", "container_id"}).AddRow(id, string(status), containerID))
}

func newDispatchService(t *testing.T) (*Service, *fakeGateway, sqlmock.Sqlmock) {
	t.Helper()
	s := newServiceForTest(t)
	gateway := &fakeGateway{}
	backend, mock := repository.NewBackendPostgresRepositoryForTest()
	s.gateway, s.backend = gateway, backend
	require.NoError(t, s.repo.SetChargeSchema(s.ctx, repository.ChargeSchema))
	return s, gateway, mock
}

// A queued task is submitted as the platform, its charge is opened under the
// task id, only the caller can read it, and a successful completion is
// billed exactly once however often it is read.
func TestQueuedTaskIsBilledOnceOnCompletion(t *testing.T) {
	s, gateway, mock := newDispatchService(t)
	app := seedRunner(t, s, "acme/video", types.StubTypeTaskQueue, types.Pricing{Request: "0.05"})
	gateway.respond = func(w http.ResponseWriter, r *http.Request, _ string) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/taskqueue/id/"+app.StubID:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"task_id":"task-1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/task/admin-ws/task-1":
			_, _ = w.Write([]byte(`{"external_id":"task-1","status":"COMPLETE","container_id":"c-1","workspace_id":7,"stub":{"external_id":"` + app.StubID + `"},"result":{"url":"s3://out"},"outputs":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}

	code, accepted := callJSON(t, s, userInfo, http.MethodPost, "/v1/models/acme/video/tasks", `{"prompt":"a cat"}`)
	require.Equal(t, http.StatusAccepted, code, accepted)
	assert.Equal(t, "task-1", accepted["id"])
	assert.Equal(t, "PENDING", accepted["status"])
	assert.Equal(t, "open", accepted["charge"].(map[string]any)["status"])
	submit := gateway.calls()[0]
	assert.Equal(t, "/taskqueue/id/"+app.StubID, submit.path, "queued work goes through the existing task queue")
	assert.Equal(t, "Bearer admin-token", submit.authorization, "executed as the platform workspace")
	assert.JSONEq(t, `{"prompt":"a cat"}`, submit.body)

	opened := charge(t, s, "task-1")
	assert.Equal(t, types.ChargeOpen, opened.Status)
	assert.Equal(t, "user-ws", opened.WorkspaceID, "the caller stays on the charge")
	assert.Equal(t, types.Pricing{Request: "0.05"}, opened.Pricing, "price is snapshotted on accept")
	assert.Zero(t, opened.Cost.MicroUSD, "submission moves no money")
	assert.Zero(t, spend(t, s, "user-ws").Cost.MicroUSD)

	code, _ = callJSON(t, s, otherInfo, http.MethodGet, "/v1/tasks/task-1", nil)
	assert.Equal(t, http.StatusNotFound, code, "another workspace cannot see the task")
	code, _ = callJSON(t, s, otherInfo, http.MethodDelete, "/v1/tasks/task-1", nil)
	assert.Equal(t, http.StatusNotFound, code, "nor cancel it")
	require.Len(t, gateway.calls(), 1, "unauthorized reads never reach the task API")

	taskRecord(mock, "task-1", types.TaskStatusComplete, "c-1")
	for i := range 3 {
		code, view := callJSON(t, s, userInfo, http.MethodGet, "/v1/tasks/task-1", nil)
		require.Equal(t, http.StatusOK, code, view)
		assert.Equal(t, "COMPLETE", view["status"])
		assert.Equal(t, map[string]any{"url": "s3://out"}, view["result"])
		for _, hidden := range []string{"container_id", "workspace_id", "stub", "external_id"} {
			assert.NotContains(t, view, hidden, "read %d leaks platform execution detail", i)
		}
		assert.Equal(t, "settled", view["charge"].(map[string]any)["status"])
	}
	settled := charge(t, s, "task-1")
	assert.Equal(t, types.ChargeSettled, settled.Status)
	assert.EqualValues(t, 50_000, settled.Cost.MicroUSD)
	assert.EqualValues(t, 50_000, settled.Cost.RequestMicroUSD)
	assert.EqualValues(t, 1, settled.Work.Requests)
	usage := spend(t, s, "user-ws")
	assert.EqualValues(t, 50_000, usage.Cost.MicroUSD, "three reads of one completion charge once")
	assert.EqualValues(t, 1, usage.Work.Requests)
	require.NoError(t, mock.ExpectationsWereMet(), "a settled charge is not re-derived from the task record")
	pending, err := s.repo.ListPendingCharges(s.ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Empty(t, pending, "accounting closed the journal entry")
}

// Failed and cancelled tasks are never billed; the task API is invoked as the
// platform but only for the caller who owns the charge.
func TestUnsuccessfulTasksAreNotBilled(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		status types.TaskStatus
	}{{"failed", http.MethodGet, types.TaskStatusError}, {"cancelled", http.MethodDelete, types.TaskStatusCancelled}} {
		t.Run(tc.name, func(t *testing.T) {
			s, gateway, mock := newDispatchService(t)
			app := seedRunner(t, s, "acme/video", types.StubTypeTaskQueue, types.Pricing{Request: "0.05"})
			gateway.respond = func(w http.ResponseWriter, r *http.Request, _ string) {
				switch {
				case r.URL.Path == "/taskqueue/id/"+app.StubID:
					_, _ = w.Write([]byte(`{"task_id":"task-9"}`))
				case r.Method == http.MethodGet:
					_, _ = w.Write([]byte(`{"status":"` + string(tc.status) + `","failure_reason":"oom"}`))
				default:
					_, _ = w.Write([]byte(`{}`))
				}
			}
			code, _ := callJSON(t, s, userInfo, http.MethodPost, "/v1/models/acme/video/tasks", `{}`)
			require.Equal(t, http.StatusAccepted, code)

			taskRecord(mock, "task-9", tc.status, "")
			code, view := callJSON(t, s, userInfo, tc.method, "/v1/tasks/task-9", nil)
			require.Equal(t, http.StatusOK, code, view)
			assert.Equal(t, string(tc.status), view["status"])
			assert.Equal(t, "void", view["charge"].(map[string]any)["status"])
			if tc.method == http.MethodDelete {
				cancel := gateway.calls()[1]
				assert.Equal(t, "/api/v1/task/admin-ws", cancel.path)
				assert.Equal(t, "Bearer admin-token", cancel.authorization)
				assert.JSONEq(t, `{"task_ids":["task-9"]}`, cancel.body)
			}
			voided := charge(t, s, "task-9")
			assert.Equal(t, types.ChargeVoid, voided.Status)
			assert.Zero(t, voided.Cost.MicroUSD)
			assert.Zero(t, spend(t, s, "user-ws").Cost.MicroUSD)
			assert.Zero(t, spend(t, s, "user-ws").Work.Requests)
		})
	}
}

// A rejected submission leaves nothing to bill or to poll.
func TestRejectedTaskSubmissionOpensNoCharge(t *testing.T) {
	s, gateway, _ := newDispatchService(t)
	seedRunner(t, s, "acme/video", types.StubTypeTaskQueue, types.Pricing{Request: "0.05"})
	gateway.respond = func(w http.ResponseWriter, _ *http.Request, _ string) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"queue is full"}`))
	}
	code, body := callJSON(t, s, userInfo, http.MethodPost, "/v1/models/acme/video/tasks", `{}`)
	assert.Equal(t, http.StatusTooManyRequests, code, body)
	pending, err := s.repo.ListPendingCharges(s.ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Empty(t, pending)

	code, body = callJSON(t, s, userInfo, http.MethodPost, "/v1/models/acme/video/invoke", `{}`)
	assert.Equal(t, http.StatusNotFound, code, "a hosted task queue only queues work: %v", body)
}

// A synchronous deployment route is relayed through the existing execution
// service as the platform, its binary response streamed back, and the flat
// price charged once the whole body was sent. Failures are not billed.
func TestSyncDeploymentIsRelayedAsPlatformAndBilledFlat(t *testing.T) {
	s, gateway, _ := newDispatchService(t)
	app := seedRunner(t, s, "acme/tts", types.StubTypeASGI, types.Pricing{Request: "0.01"})
	audio := strings.Repeat("\x00\x01mp3", 1024)
	fail := false
	gateway.respond = func(w http.ResponseWriter, r *http.Request, _ string) {
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"synthesizer crashed"}`))
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write([]byte(audio))
	}

	rec := call(t, s, userInfo, http.MethodPost, "/v1/audio/speech", `{"model":"acme/tts","input":"hello"}`, "Authorization", "Bearer user-secret")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "audio/mpeg", rec.Header().Get("Content-Type"))
	assert.Equal(t, audio, rec.Body.String(), "binary bodies are streamed unmodified")
	relayed := gateway.calls()[0]
	assert.Equal(t, "/asgi/id/"+app.StubID+"/audio-speech", relayed.path, "an ASGI app takes the OpenAI route as one path segment")
	assert.Equal(t, "Bearer admin-token", relayed.authorization, "the caller's credential never reaches the executor")
	assert.Equal(t, rec.Header().Get(headerRequestID), relayed.headers.Get(headerRequestID))
	requestID := rec.Header().Get(headerRequestID)
	settled := charge(t, s, requestID)
	assert.Equal(t, types.ChargeSettled, settled.Status)
	assert.EqualValues(t, 10_000, settled.Cost.MicroUSD)
	assert.EqualValues(t, 1, settled.Work.Requests)
	assert.Equal(t, types.EndpointRouteAudioSpeech, settled.Route)

	fail = true
	rec = call(t, s, userInfo, http.MethodPost, "/v1/audio/speech", `{"model":"acme/tts","input":"hello"}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	voided := charge(t, s, rec.Header().Get(headerRequestID))
	assert.Equal(t, types.ChargeVoid, voided.Status)
	assert.EqualValues(t, 10_000, spend(t, s, "user-ws").Cost.MicroUSD, "a failed request is not billed")

	code, body := callJSON(t, s, otherInfo, http.MethodGet, "/v1/generation?id="+requestID, nil)
	assert.Equal(t, http.StatusNotFound, code, "%v", body)
	code, body = callJSON(t, s, userInfo, http.MethodGet, "/v1/generation?id="+requestID, nil)
	require.Equal(t, http.StatusOK, code, body)
	data, _ := json.Marshal(body)
	for _, hidden := range []string{"stub", "container", "admin-ws", "replica"} {
		assert.NotContains(t, strings.ToLower(string(data)), hidden, "generation view keeps placement internal")
	}
}

// A task whose completion nobody reads is still billed: the flush loop
// settles due open charges from the task record. Once settled, later reads
// and flushes leave it alone, and a redeploy in between does not change the
// price the caller accepted.
func TestFlushSettlesUnreadTaskAtAcceptedPrice(t *testing.T) {
	s, gateway, mock := newDispatchService(t)
	s.usage = &recordingUsageMetrics{}
	app := seedRunner(t, s, "acme/video", types.StubTypeTaskQueue, types.Pricing{Request: "0.05"})
	gateway.respond = func(w http.ResponseWriter, r *http.Request, _ string) {
		if r.URL.Path == "/taskqueue/id/"+app.StubID {
			_, _ = w.Write([]byte(`{"task_id":"task-unread"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"COMPLETE"}`))
	}
	code, _ := callJSON(t, s, userInfo, http.MethodPost, "/v1/models/acme/video/tasks", `{}`)
	require.Equal(t, http.StatusAccepted, code)

	// The endpoint is redeployed at a new price while the task runs.
	app.Version, app.Pricing = 2, types.Pricing{Request: "0.50"}
	require.NoError(t, s.repo.SaveEndpoint(s.ctx, app))

	taskRecord(mock, "task-unread", types.TaskStatusRunning, "")
	require.NoError(t, s.repo.DeferAccounting(s.ctx, "task-unread", time.Now().Add(-time.Second)), "make the poll due")
	require.NoError(t, s.billing.flush(s.ctx))
	assert.Equal(t, types.ChargeOpen, charge(t, s, "task-unread").Status, "a running task stays open and unbilled")
	assert.Zero(t, spend(t, s, "user-ws").Cost.MicroUSD)

	taskRecord(mock, "task-unread", types.TaskStatusComplete, "")
	require.NoError(t, s.repo.DeferAccounting(s.ctx, "task-unread", time.Now().Add(-time.Second)))
	require.NoError(t, s.billing.flush(s.ctx))
	require.NoError(t, s.billing.flush(s.ctx))
	code, view := callJSON(t, s, userInfo, http.MethodGet, "/v1/tasks/task-unread", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "settled", view["charge"].(map[string]any)["status"])

	settled := charge(t, s, "task-unread")
	assert.Equal(t, types.ChargeSettled, settled.Status)
	assert.EqualValues(t, 1, settled.Version, "billed against the version that accepted the task")
	assert.EqualValues(t, 50_000, settled.Cost.MicroUSD, "at the price accepted, not the redeployed one")
	usage := spend(t, s, "user-ws")
	assert.EqualValues(t, 50_000, usage.Cost.MicroUSD, "flush, flush and read charge once")
	assert.EqualValues(t, 1, usage.Work.Requests)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Runner containers carry the prefix their execution service watches, so
// that service's queue, buffer and task bookkeeping adopt them as its own.
func TestRunnerPrefixMatchesExecutionService(t *testing.T) {
	for stubType, prefix := range map[string]string{
		types.StubTypeManagedEndpointDeployment: containerPrefix,
		types.StubTypeTaskQueueDeployment:       "taskqueue",
		types.StubTypeEndpointDeployment:        "endpoint",
		types.StubTypeASGIDeployment:            "endpoint",
	} {
		assert.Equal(t, prefix, runnerPrefix(&types.ManagedEndpoint{StubType: types.StubType(stubType)}), stubType)
	}
}
