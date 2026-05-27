package apiv1

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestMetricEventTypesIncludeContainerEventsForRealtimeCounts(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/?event_types=container.metrics,container.event,task.created,task.updated,container.log", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	got := metricEventTypesFromContext(ctx)
	want := []string{
		types.EventContainerMetrics,
		types.EventContainerEvent,
		types.EventTaskCreated,
		types.EventTaskUpdated,
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected metric event types: got %#v want %#v", got, want)
	}
}
