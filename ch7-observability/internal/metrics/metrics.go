package metrics

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Producer side
	StreamEventsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wiki_stream_events_consumed_total",
		Help: "Events consumed from the Wikimedia SSE stream.",
	})
	RedpandaEventsProduced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wiki_redpanda_events_produced_total",
		Help: "Events successfully persisted (acked) to Redpanda.",
	})

	// Consumer side
	RedpandaEventsConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wiki_redpanda_events_consumed_total",
		Help: "Events consumed (fetched) from Redpanda.",
	})
	EventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wiki_events_processed_total",
		Help: "Events processed successfully.",
	})
	EventsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wiki_events_failed_total",
		Help: "Events that failed to be processed.",
	})
)

func Serve(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("metrics server shutdown: %v", err)
		}
	}()

	log.Printf("metrics server listening on %s/metrics", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("metrics server error: %v", err)
	}
}
