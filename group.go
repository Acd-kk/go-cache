package go_cache

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	pb "go-cache/proto"
	"golang.org/x/sync/singleflight"
)

type Getter interface {
	Get(key string) ([]byte, error)
}

var ErrKeyNotFound = errors.New("key not found")

type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

type Group struct {
	name      string
	getter    Getter
	mainCache cache
	peers     PeerPicker
	loader    *singleflight.Group

	cacheHits       atomic.Uint64
	peerLoads       atomic.Uint64
	peerLoadErrors  atomic.Uint64
	localLoads      atomic.Uint64
	localLoadErrors atomic.Uint64
}

type GroupStats struct {
	Name            string `json:"name"`
	CacheHits       uint64 `json:"cache_hits"`
	PeerLoads       uint64 `json:"peer_loads"`
	PeerLoadErrors  uint64 `json:"peer_load_errors"`
	LocalLoads      uint64 `json:"local_loads"`
	LocalLoadErrors uint64 `json:"local_load_errors"`
}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) load(key string) (value ByteView, err error) {

	viewi, err, _ := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					g.peerLoads.Add(1)
					return value, nil
				}
				g.peerLoadErrors.Add(1)
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}

		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: cloneBytes(res.GetValue())}, nil
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	if _, exists := groups[name]; exists {
		panic("group already exists: " + name)
	}
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}

	if v, ok := g.mainCache.get(key); ok {
		g.cacheHits.Add(1)
		log.Println("[go-cache] hit")
		return v, nil
	}

	return g.load(key)
}

func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		g.localLoadErrors.Add(1)
		return ByteView{}, err

	}
	g.localLoads.Add(1)
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

func (g *Group) Stats() GroupStats {
	return GroupStats{
		Name:            g.name,
		CacheHits:       g.cacheHits.Load(),
		PeerLoads:       g.peerLoads.Load(),
		PeerLoadErrors:  g.peerLoadErrors.Load(),
		LocalLoads:      g.localLoads.Load(),
		LocalLoadErrors: g.localLoadErrors.Load(),
	}
}

func AllGroupStats() []GroupStats {
	mu.RLock()
	defer mu.RUnlock()

	stats := make([]GroupStats, 0, len(groups))
	for _, group := range groups {
		stats = append(stats, group.Stats())
	}
	return stats
}
