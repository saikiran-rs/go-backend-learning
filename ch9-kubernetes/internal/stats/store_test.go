package stats

import (
	"fmt"
	"sync"
	"testing"
)

func TestInMemoryStoreConcurrentRecord(t *testing.T) {
	store := NewInMemoryStore()

	const workers, perWorker = 50, 1000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				store.Record(fmt.Sprintf("user-%d", w), "https://en.wikipedia.org", i%2 == 0)
			}
		}(w)
	}
	wg.Wait()

	s := store.Snapshot()
	if want := int64(workers * perWorker); s.TotalMessages != want {
		t.Fatalf("TotalMessages: got %d, want %d", s.TotalMessages, want)
	}
	if s.DistinctUsers != int64(workers) {
		t.Fatalf("DistinctUsers: got %d, want %d", s.DistinctUsers, workers)
	}
}
