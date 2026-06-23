package main

import "sync"

type InMemoryStore struct {
	mu              sync.RWMutex
	totalMessages   int64
	bots            int64
	nonBots         int64
	distinctUsers   map[string]struct{}
	serverURLCounts map[string]int64
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		distinctUsers:   make(map[string]struct{}),
		serverURLCounts: make(map[string]int64),
	}
}

func (s *InMemoryStore) Record(user, serverURL string, bot bool) {
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

func (s *InMemoryStore) Snapshot() Stats {
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
