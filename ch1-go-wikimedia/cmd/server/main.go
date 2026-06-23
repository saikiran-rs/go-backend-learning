package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
)

func main() {
	go consumeWikiStream("https://stream.wikimedia.org/v2/stream/recentchange")
	const port = ":7001"
	mux := http.NewServeMux()
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/stats", statsHandler)

	error := http.ListenAndServe(port, mux)

	if error != nil {
		log.Fatal("Error starting server: ", error)
	}

}

func statusHandler(w http.ResponseWriter, r *http.Request) {

	version := r.URL.Query().Get("version")
	responce := map[string]string{"status": "ok", "version": version}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(responce)

}

// Name staing with capital letter is public and small letter is private to the package
type data struct {
	ID        int    `json:"id"`
	User      string `json:"user"` //struct tags are like annotation tags in kotlin
	Bot       bool   `json:"bot"`
	ServerURL string `json:"server_url"`
}

type statsStore struct {
	mu              sync.RWMutex
	totalMessages   int64
	bots            int64
	nonBots         int64
	distinctUsers   map[string]struct{}
	serverURLCounts map[string]int64
}

var store = &statsStore{
	distinctUsers:   make(map[string]struct{}),
	serverURLCounts: make(map[string]int64),
}

func consumeWikiStream(url string) {

	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		log.Fatal("Error creating request: ", err)
	}

	// looks like wikimedia requires a descriptive app name + contact.
	req.Header.Set("User-Agent", "Go-Wikimedia-SSE-Leraning-Client/1.0 (gobackendlearningsai@gmail.com)")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req) //opens the connetions

	if err != nil {
		log.Fatal("Error connecting to stream: ", err)
	} else if resp.StatusCode != http.StatusOK {
		log.Fatalf("Error connecting to stream: received status code %d", resp.StatusCode)
	}

	defer resp.Body.Close() //like finally block upfront is best practice to avoid resource leaks

	scanner := bufio.NewScanner(resp.Body) //keeps the connection open
	// 0 to 1 mb buffer defult is 64 kb before overflow kind of error.
	// Not sure 1 mb is needed in this case. Just for learning.
	scanner.Buffer(make([]byte, 0), 1024*1024)

	for scanner.Scan() { //reads the stream line by line and gets new line when for continues

		line := scanner.Text() //gets the text of the line

		// println(line)
		// time.Sleep(time.Second * 2)
		//Sample out with sleep each line is like this
		// :ok
		// event: message
		// id: {...}
		// data: {...}
		// /n

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

		store.mu.Lock()
		store.totalMessages++
		store.distinctUsers[wikiData.User] = struct{}{}
		if wikiData.Bot {
			store.bots++
		} else {
			store.nonBots++
		}
		store.serverURLCounts[wikiData.ServerURL]++
		store.mu.Unlock()

	}
	if err := scanner.Err(); err != nil {
		log.Printf("stream read error: %v", err)
	}

}

// We need:
// Number of messages consumed
// Number of distinct users
// Number of bots & Number of non-bots
// Count by distinct server URLs

type stats struct {
	TotalMessages   int64            `json:"total_messages"`
	DistinctUsers   int64            `json:"distinct_users"`
	BotCount        int64            `json:"bot_count"`
	NonBotCount     int64            `json:"non_bot_count"`
	ServerURLCounts map[string]int64 `json:"server_url_counts"`
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	// We could use plain mu.Lock/Unlock but that serializes readers each /stats
	// request would wait for the previous one, even though reads don't conflict.
	// RLock/RUnlock lets readers run concurrently, only a write blocks them.
	// Not a big deal at this scale just using it for learning.
	store.mu.RLock()
	defer store.mu.RUnlock()

	stats := stats{
		TotalMessages:   store.totalMessages,
		DistinctUsers:   int64(len(store.distinctUsers)),
		BotCount:        store.bots,
		NonBotCount:     store.nonBots,
		ServerURLCounts: store.serverURLCounts,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)

}
