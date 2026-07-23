package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {

	var store StatsStore = NewInMemoryStore()
	go consumeWikiStream(getenv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"), store)

	scyllaHost := getenv("SCYLLA_HOST", "localhost")
	scyllaPort := getenvInt("SCYLLA_PORT", 9042)
	persister, err := NewScyllaStore(scyllaHost, scyllaPort)
	if err != nil {
		log.Fatal("Error creating Scylla store: ", err)
	}
	go runSnapshotTicker(persister, store, time.Minute)

	port := getenv("PORT", "7001")
	mux := http.NewServeMux()
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		statsHandler(w, store)
	})

	log.Println("Starting server on port http://localhost:" + port)

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal("Error starting server: ", err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func statusHandler(w http.ResponseWriter, r *http.Request) {

	version := r.URL.Query().Get("version")
	responce := map[string]string{"status": "ok", "version": version}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(responce)

}

type data struct {
	ID        int    `json:"id"`
	User      string `json:"user"`
	Bot       bool   `json:"bot"`
	ServerURL string `json:"server_url"`
}

var rwmData sync.RWMutex

func consumeWikiStream(url string, store StatsStore) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("Error creating request: ", err)
	}

	req.Header.Set("User-Agent", "Go-Wikimedia-SSE-Leraning-Client/1.0 (gobackendlearningsai@gmail.com)")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("Error connecting to stream: ", err)
	} else if resp.StatusCode != http.StatusOK {
		log.Fatalf("Error connecting to stream: received status code %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		rawData := strings.TrimPrefix(line, "data: ")
		var wikiData data
		err := json.Unmarshal([]byte(rawData), &wikiData)
		if err != nil {
			log.Printf("Error parsing JSON: %v\n", err)
			continue
		}
		store.Record(wikiData.User, wikiData.ServerURL, wikiData.Bot)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("stream read error: %v", err)
	}

}

func statsHandler(w http.ResponseWriter, store StatsStore) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(store.Snapshot())
}

func runSnapshotTicker(p SnapshotSaver, store StatsStore, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := p.SaveSnapshot(time.Now().UTC(), store.Snapshot()); err != nil {
			log.Printf("snapshot save error: %v", err)
		}
	}
}