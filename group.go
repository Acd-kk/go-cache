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

type Setter interface {
	Set(key string, value []byte) error
}

type Deleter interface {
	Delete(key string) error
}

var ErrKeyNotFound = errors.New("key not found")

type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

type Group struct {
	// name 表示缓存组名称
	name string
	// getter 用于缓存未命中时回源取数据
	getter Getter
	// mainCache 是当前节点的本地缓存
	mainCache cache
	// peers 用于选择远程节点
	peers PeerPicker
	// loader 用于防止并发重复加载同一个 key
	loader *singleflight.Group

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

// RegisterPeers注册一个peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) load(key string) (value ByteView, err error) {
	// singleflight 会让同一个 key 的并发请求只加载一次
	viewi, err, _ := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				// 如果 key 被分配到远程节点 就优先去远程节点取值
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
	// 远程节点之间通过 protobuf 消息传递 group 和 key
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

	// 先查本地缓存 命中就直接返回
	if v, ok := g.mainCache.get(key); ok {
		g.cacheHits.Add(1)
		log.Println("[go-cache] hit")
		return v, nil
	}

	return g.load(key)
}

func (g *Group) getLocally(key string) (ByteView, error) {
	// 本地回源可以理解为查数据库或调用下游服务
	bytes, err := g.getter.Get(key)
	if err != nil {
		g.localLoadErrors.Add(1)
		return ByteView{}, err

	}
	g.localLoads.Add(1)
	value := ByteView{b: cloneBytes(bytes)}
	// 回源成功后写入缓存 方便后续直接命中
	g.populateCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

func (g *Group) Set(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}

	if g.peers != nil {
		if peer, ok := g.peers.PickPeer(key); ok {
			return peer.Set(g.name, key, value)
		}
	}

	return g.setLocally(key, value)
}

func (g *Group) setLocally(key string, value []byte) error {
	setter, ok := g.getter.(Setter)
	if !ok {
		return fmt.Errorf("setter is not supported")
	}
	if err := setter.Set(key, value); err != nil {
		return err
	}
	g.populateCache(key, ByteView{b: cloneBytes(value)})
	return nil
}

func (g *Group) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}

	if g.peers != nil {
		if peer, ok := g.peers.PickPeer(key); ok {
			return peer.Delete(g.name, key)
		}
	}

	return g.deleteLocally(key)
}

func (g *Group) deleteLocally(key string) error {
	deleter, ok := g.getter.(Deleter)
	if !ok {
		return fmt.Errorf("deleter is not supported")
	}
	if err := deleter.Delete(key); err != nil {
		return err
	}
	g.mainCache.remove(key)
	return nil
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

	// 汇总所有缓存组的运行统计
	stats := make([]GroupStats, 0, len(groups))
	for _, group := range groups {
		stats = append(stats, group.Stats())
	}
	return stats
}
