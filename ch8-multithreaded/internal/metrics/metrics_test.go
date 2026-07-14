package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCounterIncrements(t *testing.T) {
	before := testutil.ToFloat64(EventsProcessed)
	EventsProcessed.Inc()
	if got, want := testutil.ToFloat64(EventsProcessed), before+1; got != want {
		t.Fatalf("EventsProcessed: got %v, want %v", got, want)
	}
}

func TestMetricsEndpointExposesAllCounters(t *testing.T) {
	// Touch each counter so it appears in the exposition output.
	StreamEventsConsumed.Inc()
	RedpandaEventsProduced.Inc()
	RedpandaEventsConsumed.Inc()
	EventsProcessed.Inc()
	EventsFailed.Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, name := range []string{
		"wiki_stream_events_consumed_total",
		"wiki_redpanda_events_produced_total",
		"wiki_redpanda_events_consumed_total",
		"wiki_events_processed_total",
		"wiki_events_failed_total",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics missing %s", name)
		}
	}
}
