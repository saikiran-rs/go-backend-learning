package main

import "time"

type Stats struct {
	TotalMessages   int64            `json:"total_messages"`
	DistinctUsers   int64            `json:"distinct_users"`
	BotCount        int64            `json:"bot_count"`
	NonBotCount     int64            `json:"non_bot_count"`
	ServerURLCounts map[string]int64 `json:"server_url_counts"`
}

type StatsStore interface {
	Record(user, serverURL string, bot bool)
	Snapshot() Stats
}

type SnapshotSaver interface {
	SaveSnapshot(ts time.Time, s Stats) error
}
