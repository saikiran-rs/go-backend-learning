package main

import (
	"testing"

	"ch8-multithreaded/internal/metrics"
	"ch8-multithreaded/internal/stats"
	wikiv1 "ch8-multithreaded/internal/wiki/v1"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"
)

func TestProcessRecordSuccess(t *testing.T) {
	store := stats.NewInMemoryStore()
	before := testutil.ToFloat64(metrics.EventsProcessed)

	value, err := proto.Marshal(&wikiv1.WikiEvent{
		User:      "Alice",
		Bot:       false,
		ServerUrl: "https://en.wikipedia.org",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if _, err := processRecord(store, value); err != nil {
		t.Fatalf("processRecord returned error: %v", err)
	}
	if got, want := testutil.ToFloat64(metrics.EventsProcessed), before+1; got != want {
		t.Fatalf("EventsProcessed: got %v, want %v", got, want)
	}
	if s := store.Snapshot(); s.TotalMessages != 1 {
		t.Fatalf("store.TotalMessages: got %d, want 1", s.TotalMessages)
	}
}

func TestProcessRecordFailure(t *testing.T) {
	store := stats.NewInMemoryStore()
	before := testutil.ToFloat64(metrics.EventsFailed)

	// 0x0f = field 1 with wire type 7, which is illegal — proto.Unmarshal errors.
	if _, err := processRecord(store, []byte{0x0f}); err == nil {
		t.Fatalf("expected error for invalid payload, got nil")
	}
	if got, want := testutil.ToFloat64(metrics.EventsFailed), before+1; got != want {
		t.Fatalf("EventsFailed: got %v, want %v", got, want)
	}
}
