package main

import (
	"flag"
	"fmt"
	go_cache "go-cache"
	"log"
	"sort"
	"strings"
	"sync"
)

type store struct {
	mu   sync.RWMutex
	data map[string]string
}

func (s *store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	log.Println("[SlowDB] search key", key)
	if v, ok := s.data[key]; ok {
		return []byte(v), nil
	}
	return nil, fmt.Errorf("%w: %s", go_cache.ErrKeyNotFound, key)
}

func (s *store) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = string(value)
	return nil
}

func (s *store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return fmt.Errorf("%w: %s", go_cache.ErrKeyNotFound, key)
	}
	delete(s.data, key)
	return nil
}

func (s *store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func main() {
	addr := flag.String("addr", "localhost:8001", "cache server address")
	peersFlag := flag.String("peers", "localhost:8001,localhost:8002,localhost:8003", "comma-separated peer addresses")
	flag.Parse()

	db := &store{
		data: map[string]string{
			"kk":  "630",
			"acd": "589",
			"123": "567",
		},
	}

	group := go_cache.NewGroup("scores", 2<<10, db)

	peers := go_cache.NewHTTPPool(*addr)
	peerAddrs := splitPeers(*peersFlag)
	if len(peerAddrs) == 0 {
		log.Fatal("at least one peer is required")
	}
	// 把集群中的所有节点注册到当前服务
	peers.Set(peerAddrs...)
	group.RegisterPeers(peers)

	log.Printf("cache server running at %s", *addr)
	log.Printf("cache peers: %v", peerAddrs)
	log.Printf("available keys: %v", db.Keys())
	log.Fatal(peers.Run(*addr))
}

func splitPeers(raw string) []string {
	parts := strings.Split(raw, ",")
	peers := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		peers = append(peers, part)
	}
	return peers
}
