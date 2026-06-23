package main

import (
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type ScyllaStore struct {
	session         *gocql.Session
	mu              sync.RWMutex
	totalMessages   int64
	bots            int64
	nonBots         int64
	distinctUsers   map[string]struct{}
	serverURLCounts map[string]int64
}

func NewScyllaStore(host string, port int) (*ScyllaStore, error) {
	cluster := gocql.NewCluster(host)
	cluster.Port = port

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	s := &ScyllaStore{
		session:         session,
		distinctUsers:   make(map[string]struct{}),
		serverURLCounts: make(map[string]int64),
	}

	if err := s.initializeSchema(); err != nil {
		return nil, err
	}

	return s, nil
}

// Schema decisions:
// Approach: timed snapshots. Every minute the ticker writes one row capturing
// the current stats. This builds the time series keeping Grafana in mind. The
// in-memory store only holds "now", the DB holds the whole timeline.
// PRIMARY KEY (bucket, ts):
//   - bucket (the date) groups rows by day. This keeps each partition from
//     growing forever, which Scylla cares about.
//   - ts (timestamp) sorts rows by time within the day, so time-range queries
//     for Grafana will be fast. DESC = newest first.
// server_counts is a map column: all per-server counts sit inside the snapshot row.
func (s *ScyllaStore) initializeSchema() error {
	queries := []string{
		`CREATE KEYSPACE IF NOT EXISTS wiki WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
		`CREATE TABLE IF NOT EXISTS wiki.stats_snapshots(
			bucket date, 
			ts timestamp, 
			total_messages bigint, 
			distinct_users bigint, 
			bot_count bigint, 
			non_bot_count bigint, 
			server_counts map<text, bigint>,
			PRIMARY KEY (bucket, ts)
		) WITH CLUSTERING ORDER BY (ts DESC)`,
	}

	for _, query := range queries {
		if err := s.session.Query(query).Exec(); err != nil {
			return err
		}
	}

	return nil
}

func (s *ScyllaStore) Record(user, serverURL string, bot bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalMessages++
	s.distinctUsers[user] = struct{}{}
	if bot {
		s.bots++
	} else {
		s.nonBots++
	}
	s.serverURLCounts[serverURL]++
}

func (s *ScyllaStore) Snapshot() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	servers := make(map[string]int64, len(s.serverURLCounts))
	for serverURL, count := range s.serverURLCounts {
		servers[serverURL] = count
	}
	return Stats{
		TotalMessages:   s.totalMessages,
		DistinctUsers:   int64(len(s.distinctUsers)),
		BotCount:        s.bots,
		NonBotCount:     s.nonBots,
		ServerURLCounts: servers,
	}
}

func (s *ScyllaStore) SaveSnapshot(ts time.Time, snapshot Stats) error {
	return s.session.Query(`
	INSERT INTO wiki.stats_snapshots 
	(bucket, ts, total_messages, distinct_users, bot_count, non_bot_count, server_counts) 
	VALUES (?, ?, ?, ?, ?, ?, ?)`,
		ts, //bucket
		ts, //actual ts
		snapshot.TotalMessages,
		snapshot.DistinctUsers,
		snapshot.BotCount,
		snapshot.NonBotCount,
		snapshot.ServerURLCounts,
	).Exec()
}

func (s *ScyllaStore) Close() {
	s.session.Close()
}
