package main

import (
	"bufio"
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	brokers := getenv("REDPANDA_BROKERS", "localhost:9092")
	topic := getenv("WIKI_TOPIC", "wiki-events")
	streamURL := getenv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		log.Fatal("kafka client: ", err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("producer starting: brokers=%s topic=%s", brokers, topic)
	for ctx.Err() == nil {
		if err := produceStream(ctx, client, streamURL); err != nil && ctx.Err() == nil {
			log.Printf("stream ended (%v); reconnecting in 2s", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}

	if err := client.Flush(context.Background()); err != nil {
		log.Printf("flush error: %v", err)
	}
	log.Println("producer shut down cleanly")
}

func produceStream(ctx context.Context, client *kgo.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Go-Wikimedia-SSE-Leraning-Client/1.0 (gobackendlearningsai@gmail.com)")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("closing stream body: %v", err)
		}
	}()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0), 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		rawData := strings.TrimPrefix(line, "data: ")

		record := &kgo.Record{Value: []byte(rawData)}
		client.Produce(ctx, record, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Printf("produce error: %v", err)
			}
		})
	}
	return scanner.Err()
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
