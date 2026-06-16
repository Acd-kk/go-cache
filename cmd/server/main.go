package main

import (
	"flag"
	"fmt"
	go_cache "go-cache"
	"log"
	"sort"
	"strings"
)

var db = map[string]string{
	"kk":  "630",
	"acd": "589",
	"123": "567",
}

func main() {
	addr := flag.String("addr", "localhost:8001", "cache server address")
	peersFlag := flag.String("peers", "localhost:8001,localhost:8002,localhost:8003", "comma-separated peer addresses")
	flag.Parse()

	group := go_cache.NewGroup("scores", 2<<10, go_cache.GetterFunc(
		func(key string) ([]byte, error) {
			// 这里用 map 模拟慢速数据源
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%w: %s", go_cache.ErrKeyNotFound, key)
		}))

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
	log.Printf("available keys: %v", sortedKeys(db))
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

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
