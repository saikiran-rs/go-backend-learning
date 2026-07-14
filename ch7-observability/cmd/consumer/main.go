package main

import (
	"ch7-observability/internal/metrics"
	"ch7-observability/internal/stats"
	wikiv1 "ch7-observability/internal/wiki/v1"
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	brokers := getenv("REDPANDA_BROKERS", "localhost:9092")
	topic := getenv("WIKI_TOPIC", "wiki-events-proto")
	group := getenv("CONSUMER_GROUP", "wiki-stats")

	store := stats.NewInMemoryStore()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(), // so we consume and push to in memory then commit manually
	)
	if err != nil {
		log.Fatal("kafka client: ", err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go metrics.Serve(ctx, getenv("METRICS_ADDR", ":2112"))

	log.Printf("consumer starting: brokers=%s topic=%s group=%s", brokers, topic, group)
	go reportStats(ctx, store, 5*time.Second)
	consume(ctx, client, store)
	log.Println("consumer shutdown done.")
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

func consume(ctx context.Context, client *kgo.Client, store stats.Store) {
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("fetch error: topic=%s partition=%d err=%v", e.Topic, e.Partition, e.Err)
			}
			continue
		}

		it := fetches.RecordIter()
		for !it.Done() {
			record := it.Next()
			metrics.RedpandaEventsConsumed.Inc()

			if err := processRecord(store, record.Value); err != nil {
				log.Printf("process error: %v", err)
			}
		}

		if err := client.CommitRecords(ctx, fetches.Records()...); err != nil {
			log.Printf("error committing records %v", err)
		}
	}
}

func processRecord(store stats.Store, value []byte) error {
	var event wikiv1.WikiEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		metrics.EventsFailed.Inc()
		return err
	}
	store.Record(event.User, event.ServerUrl, event.Bot)
	metrics.EventsProcessed.Inc()
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
