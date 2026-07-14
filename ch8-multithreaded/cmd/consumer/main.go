package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"ch8-multithreaded/internal/metrics"
	"ch8-multithreaded/internal/stats"
	wikiv1 "ch8-multithreaded/internal/wiki/v1"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func main() {
	brokers := getenv("REDPANDA_BROKERS", "localhost:9092")
	topic := getenv("WIKI_TOPIC", "wiki-events-proto")
	group := getenv("CONSUMER_GROUP", "wiki-stats")
	scyllaHost := getenv("SCYLLA_HOST", "localhost")
	scyllaPort := atoi(getenv("SCYLLA_PORT", "9042"), 9042)
	numConsumers := atoi(getenv("NUM_CONSUMERS", "3"), 3)
	batchSize := atoi(getenv("BATCH_SIZE", "100"), 100)
	batchTimeout := durenv("BATCH_TIMEOUT", 5*time.Second)

	db, err := stats.NewScyllaStore(scyllaHost, scyllaPort)
	if err != nil {
		log.Fatal("scylla: ", err)
	}
	defer db.Close()

	store := stats.NewInMemoryStore()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go metrics.Serve(ctx, getenv("METRICS_ADDR", ":2112"))
	go reportStats(ctx, store, 5*time.Second)

	log.Printf("consumer starting: brokers=%s topic=%s group=%s consumers=%d batch=%d",
		brokers, topic, group, numConsumers, batchSize)

	var wg sync.WaitGroup
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runConsumer(ctx, id, brokers, topic, group, store, db, batchSize, batchTimeout)
		}(i)
	}

	wg.Wait()
	log.Println("consumer shutdown done.")
}

func runConsumer(ctx context.Context, id int, brokers, topic, group string,
	store stats.Store, db *stats.ScyllaStore, batchSize int, batchTimeout time.Duration) {

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		log.Printf("worker %d: kafka client: %v", id, err)
		return
	}
	defer client.Close()

	buf := make([]stats.Event, 0, batchSize)
	pending := make([]*kgo.Record, 0, batchSize)

	flush := func(fctx context.Context) {
		if len(buf) == 0 {
			return
		}
		if err := db.SaveEvents(fctx, buf); err != nil {
			log.Printf("worker %d: save batch: %v", id, err)
			return // keep buffer don't commit safe re-read
		}
		if err := client.CommitRecords(fctx, pending...); err != nil {
			log.Printf("worker %d: commit: %v", id, err)
			return
		}
		buf = buf[:0]
		pending = pending[:0]
	}

	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			// Graceful shutdown
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			flush(fctx)
			cancel()
			log.Printf("worker %d: stopped", id)
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("worker %d: fetch error: %v", id, e.Err)
			}
			continue
		}

		it := fetches.RecordIter()
		for !it.Done() {
			rec := it.Next()
			metrics.RedpandaEventsConsumed.Inc()

			event, err := processRecord(store, rec.Value)
			if err != nil {
				log.Printf("worker %d: process error: %v", id, err)
				continue
			}

			buf = append(buf, stats.Event{
				Partition: rec.Partition,
				Offset:    rec.Offset,
				User:      event.User,
				ServerURL: event.ServerUrl,
				Bot:       event.Bot,
			})
			pending = append(pending, rec)

			if len(buf) >= batchSize {
				flush(ctx)
			}
		}

		select {
		case <-ticker.C:
			flush(ctx)
		default:
		}
	}
}

func reportStats(ctx context.Context, store stats.Store, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s := store.Snapshot()
			log.Printf("stats: messages=%d distinct_users=%d bots=%d non_bots=%d servers=%d",
				s.TotalMessages, s.DistinctUsers, s.BotCount, s.NonBotCount, len(s.ServerURLCounts))
		}
	}
}

func processRecord(store stats.Store, value []byte) (*wikiv1.WikiEvent, error) {
	var event wikiv1.WikiEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		metrics.EventsFailed.Inc()
		return nil, err
	}
	store.Record(event.User, event.ServerUrl, event.Bot)
	metrics.EventsProcessed.Inc()
	return &event, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func atoi(s string, fallback int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return fallback
}

func durenv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
