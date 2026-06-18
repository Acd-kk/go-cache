package go_cache

import (
	"strconv"
	"testing"
	"time"
)

type testStore struct {
	data map[string][]byte
}

func (s *testStore) Get(key string) ([]byte, error) {
	if v, ok := s.data[key]; ok {
		return v, nil
	}
	return nil, ErrKeyNotFound
}

func (s *testStore) Set(key string, value []byte) error {
	s.data[key] = append([]byte(nil), value...)
	return nil
}

func (s *testStore) Delete(key string) error {
	if _, ok := s.data[key]; !ok {
		return ErrKeyNotFound
	}
	delete(s.data, key)
	return nil
}

func TestHTTPPoolSetNormalizesPeers(t *testing.T) {
	pool := NewHTTPPool("localhost:8001")
	pool.Set("localhost:8001", "localhost:8002")

	if _, ok := pool.httpGetters["localhost:8002"]; ok {
		t.Fatalf("expected raw peer address to be absent from httpGetters")
	}
	if _, ok := pool.httpGetters["http://localhost:8002"]; !ok {
		t.Fatalf("expected normalized peer address to exist in httpGetters")
	}

	peer := pool.peers.Get("some-key")
	if peer != "http://localhost:8001" && peer != "http://localhost:8002" {
		t.Fatalf("expected normalized peer from hash ring, got %q", peer)
	}
}

func TestGroupStatsTrackLocalLoadAndHit(t *testing.T) {
	groupName := "stats-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	group := NewGroup(groupName, 1<<10, GetterFunc(func(key string) ([]byte, error) {
		return []byte("value"), nil
	}))

	if _, err := group.Get("k"); err != nil {
		t.Fatalf("first get failed: %v", err)
	}
	if _, err := group.Get("k"); err != nil {
		t.Fatalf("second get failed: %v", err)
	}

	stats := group.Stats()
	if stats.LocalLoads != 1 {
		t.Fatalf("expected 1 local load, got %d", stats.LocalLoads)
	}
	if stats.CacheHits != 1 {
		t.Fatalf("expected 1 cache hit, got %d", stats.CacheHits)
	}
}

func TestGroupSetAndDelete(t *testing.T) {
	groupName := "set-delete-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	store := &testStore{data: map[string][]byte{}}
	group := NewGroup(groupName, 1<<10, store)

	if err := group.Set("new-key", []byte("value")); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	view, err := group.Get("new-key")
	if err != nil {
		t.Fatalf("get after set failed: %v", err)
	}
	if view.String() != "value" {
		t.Fatalf("expected value, got %q", view.String())
	}

	if err := group.Delete("new-key"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	if _, err := group.Get("new-key"); err == nil {
		t.Fatalf("expected key to be deleted")
	}
}
