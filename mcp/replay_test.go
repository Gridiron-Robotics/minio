package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func payload(v string) map[string]any { return map[string]any{"tool": "put_object", "result": v} }

// The original cache never evicted anything: every distinct Idempotency-Key
// held its payload for the life of the process. This pins that the store is
// actually bounded.
func TestMemStoreEvictsAtCapacity(t *testing.T) {
	ctx := context.Background()
	m := newMemReplayStore(3, time.Hour)
	for i := 0; i < 10; i++ {
		m.Remember(ctx, fmt.Sprintf("k%d", i), payload(fmt.Sprintf("v%d", i)))
	}
	if got := m.Len(); got != 3 {
		t.Fatalf("after 10 inserts with max=3, Len()=%d, want 3 (unbounded growth regression)", got)
	}
	// The most recent survive; the oldest are gone.
	if _, ok := m.Lookup(ctx, "k9"); !ok {
		t.Error("most recent key should be retained")
	}
	if _, ok := m.Lookup(ctx, "k0"); ok {
		t.Error("oldest key should have been evicted")
	}
}

// Eviction is LRU, not insertion order: a key that keeps being read must
// outlive newer-but-idle keys, otherwise a hot retry loop evicts its own entry.
func TestMemStoreEvictsLeastRecentlyUsed(t *testing.T) {
	ctx := context.Background()
	m := newMemReplayStore(3, time.Hour)
	m.Remember(ctx, "a", payload("a"))
	m.Remember(ctx, "b", payload("b"))
	m.Remember(ctx, "c", payload("c"))
	// Touch "a" so it is the most recently used, then overflow by one.
	if _, ok := m.Lookup(ctx, "a"); !ok {
		t.Fatal("a should be present")
	}
	m.Remember(ctx, "d", payload("d"))

	if _, ok := m.Lookup(ctx, "a"); !ok {
		t.Error("recently-read key 'a' must survive eviction")
	}
	if _, ok := m.Lookup(ctx, "b"); ok {
		t.Error("least recently used key 'b' should have been evicted")
	}
}

// A replayed result must not be replayable forever — the TTL is the second
// eviction axis.
func TestMemStoreExpiresByTTL(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	m := newMemReplayStore(100, 10*time.Minute)
	m.now = func() time.Time { return now }

	m.Remember(ctx, "k", payload("v"))
	if _, ok := m.Lookup(ctx, "k"); !ok {
		t.Fatal("entry should be live immediately after write")
	}
	now = now.Add(9 * time.Minute)
	if _, ok := m.Lookup(ctx, "k"); !ok {
		t.Fatal("entry should still be live before the TTL elapses")
	}
	now = now.Add(2 * time.Minute) // now 11 minutes -> past the 10 minute TTL
	if _, ok := m.Lookup(ctx, "k"); ok {
		t.Fatal("entry should have expired after the TTL")
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("expired entry should not be counted live, Len()=%d", got)
	}
}

// A hit must return the ORIGINAL payload, not merely "some" payload — that is
// the entire promise of idempotent replay.
func TestMemStoreReturnsOriginalPayload(t *testing.T) {
	ctx := context.Background()
	m := newMemReplayStore(10, time.Hour)
	m.Remember(ctx, "k", map[string]any{"tool": "put_object", "result": map[string]any{"etag": "abc123"}})
	got, ok := m.Lookup(ctx, "k")
	if !ok {
		t.Fatal("expected a hit")
	}
	res, _ := got["result"].(map[string]any)
	if res["etag"] != "abc123" {
		t.Fatalf("replay returned %v, want the original etag abc123", got)
	}
}

// Honesty check: the in-memory store must not claim durability. /healthz reads
// this, and a wrong answer here is worse than no answer.
func TestMemStoreIsNotDurable(t *testing.T) {
	if newMemReplayStore(10, time.Hour).Durable() {
		t.Fatal("in-memory replay store must report Durable()=false")
	}
}

// Two concurrent requests carrying the same key must not both execute the
// mutation; the second waits and replays the first's result.
func TestMemStoreSingleFlight(t *testing.T) {
	m := newMemReplayStore(10, time.Hour)
	done, wait := m.begin("k")
	if wait != nil {
		t.Fatal("first caller should acquire the key, not wait")
	}
	secondDone, secondWait := m.begin("k")
	if secondWait == nil {
		t.Fatal("second concurrent caller must be told to wait")
	}
	if secondDone != nil {
		t.Fatal("second caller must not receive a release func")
	}

	released := make(chan struct{})
	go func() { <-secondWait; close(released) }()
	done()
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not released when the first caller finished")
	}
	// Once released the key is free again.
	if _, w := m.begin("k"); w != nil {
		t.Fatal("key should be claimable again after release")
	}
}

func TestMemStoreConcurrentAccessIsSafe(t *testing.T) {
	ctx := context.Background()
	m := newMemReplayStore(50, time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := fmt.Sprintf("k%d", i%20)
			m.Remember(ctx, k, payload("v"))
			m.Lookup(ctx, k)
		}(i)
	}
	wg.Wait()
	if got := m.Len(); got > 50 {
		t.Fatalf("Len()=%d exceeds max 50", got)
	}
}

// ---- durable store ---------------------------------------------------------

// fakeRedis stands in for go-redis so durability semantics are tested without
// a live server.
type fakeRedis struct {
	mu     sync.Mutex
	data   map[string]string
	setErr error
	getErr error
	sets   int
}

func newFakeRedis() *fakeRedis { return &fakeRedis{data: map[string]string{}} }

func (f *fakeRedis) Get(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.data[key]
	if !ok {
		return "", errors.New("redis: nil")
	}
	return v, nil
}

func (f *fakeRedis) SetEX(_ context.Context, key, value string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setErr != nil {
		return f.setErr
	}
	f.sets++
	f.data[key] = value
	return nil
}

func (f *fakeRedis) Close() error { return nil }

// The point of the durable store: a payload written before a restart is still
// replayable after one. Simulated by building a fresh store over the same
// backing data, which is exactly what a restarted process sees.
func TestRedisStoreSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	shared := newFakeRedis()

	before := newRedisReplayStore(shared, time.Hour)
	before.Remember(ctx, "tenant\x00key-1", map[string]any{"tool": "put_object", "result": "etag-xyz"})

	// ---- process restarts here: new store, same external Redis ----
	after := newRedisReplayStore(shared, time.Hour)
	got, ok := after.Lookup(ctx, "tenant\x00key-1")
	if !ok {
		t.Fatal("a retry after a restart must replay, not re-execute")
	}
	if got["result"] != "etag-xyz" {
		t.Fatalf("replayed payload = %v, want the original result", got)
	}
	if !after.Durable() {
		t.Fatal("redis-backed store must report Durable()=true")
	}
}

// Keys are namespaced so the replay cache cannot collide with anything else
// sharing the Redis instance.
func TestRedisStoreNamespacesKeys(t *testing.T) {
	ctx := context.Background()
	f := newFakeRedis()
	newRedisReplayStore(f, time.Hour).Remember(ctx, "k", payload("v"))
	f.mu.Lock()
	defer f.mu.Unlock()
	for k := range f.data {
		if k != "minio-mcp:replay:k" {
			t.Fatalf("stored under %q, want the namespaced minio-mcp:replay: prefix", k)
		}
	}
}

// A replay-cache outage must cost a replay, never the caller's request. If
// Redis is down the tool still runs; it just re-executes on retry.
func TestRedisStoreFailuresDoNotBreakCallers(t *testing.T) {
	ctx := context.Background()
	f := newFakeRedis()
	f.setErr = errors.New("connection refused")
	r := newRedisReplayStore(f, time.Hour)
	r.Remember(ctx, "k", payload("v")) // must not panic
	if _, ok := r.Lookup(ctx, "k"); ok {
		t.Fatal("a failed write should simply be a miss")
	}

	f.setErr = nil
	f.getErr = errors.New("connection refused")
	r.Remember(ctx, "k2", payload("v"))
	if _, ok := r.Lookup(ctx, "k2"); ok {
		t.Fatal("a failed read should be a miss, not a bogus hit")
	}
}

// Corrupt data in the cache must be treated as a miss (re-execute), never
// surfaced as a result.
func TestRedisStoreIgnoresCorruptPayload(t *testing.T) {
	ctx := context.Background()
	f := newFakeRedis()
	f.data["minio-mcp:replay:k"] = "{not json"
	if _, ok := newRedisReplayStore(f, time.Hour).Lookup(ctx, "k"); ok {
		t.Fatal("undecodable payload must be a miss")
	}
}

// newReplayStore is the wiring decision; it must not silently pick the
// non-durable store when durability was configured, nor blow up on a bad URL.
func TestNewReplayStoreSelection(t *testing.T) {
	if s := newReplayStore(config{}); s.Durable() {
		t.Error("no Redis URL configured -> in-memory (not durable)")
	}
	// An unusable URL degrades to in-memory rather than refusing to serve
	// storage, and must report itself as not durable.
	s := newReplayStore(config{replayRedis: "://nonsense"})
	if s.Durable() {
		t.Error("an unusable Redis URL must not report durability")
	}
}

// Zero/negative bounds must fall back to the defaults rather than producing a
// store that evicts everything (max=0) or expires instantly (ttl=0).
func TestMemStoreRejectsDegenerateBounds(t *testing.T) {
	ctx := context.Background()
	m := newMemReplayStore(0, 0)
	if m.max != defaultReplayMax || m.ttl != defaultReplayTTL {
		t.Fatalf("degenerate bounds -> max=%d ttl=%v, want defaults", m.max, m.ttl)
	}
	m.Remember(ctx, "k", payload("v"))
	if _, ok := m.Lookup(ctx, "k"); !ok {
		t.Fatal("a store built with degenerate bounds must still work")
	}
}
