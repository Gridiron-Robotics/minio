package main

// Idempotency replay storage.
//
// A caller retries a mutating tool call with the same Idempotency-Key and must
// get the ORIGINAL result back rather than a second execution. That promise has
// two requirements the first implementation did not meet:
//
//  1. It must not grow without bound. The original was a plain map that was
//     only ever written to — every distinct key held its payload for the life
//     of the process, so a long-lived sidecar leaked memory in proportion to
//     traffic.
//  2. It must survive a restart. The original lived only in process memory, so
//     a retry arriving after a deploy or a crash re-executed the mutation. That
//     is precisely the case idempotency exists to cover, and it was the case it
//     did not handle.
//
// So the store is an interface with two implementations: a bounded in-memory
// LRU with a TTL (correct eviction, no durability) and a Redis-backed store
// (durable, and shared across replicas). Which one is live is reported on the
// health endpoint — a silent downgrade to in-memory would otherwise look
// exactly like durability.
//
// Concurrency note: a hit is only recorded AFTER the handler returns, so two
// concurrent requests carrying the same key can both miss and both execute.
// The in-memory store closes that window with a single-flight reservation. The
// Redis store does not coordinate across replicas — two replicas racing the
// same key can still both execute. Every tool this guards is itself idempotent
// at the S3 layer (PutObject overwrites, RemoveObject on a missing key
// succeeds, MakeBucket is guarded by BucketExists), so the consequence is a
// duplicated round-trip, not a corrupted result. A cross-replica reservation
// (SET NX + wait) is the fix if a non-idempotent tool is ever added here.

import (
	"container/list"
	"context"
	"encoding/json"
	"sync"
	"time"
)

// replayStore records the result of a keyed, mutating invocation so a retry
// replays it instead of re-executing.
type replayStore interface {
	// Lookup returns a previously stored payload for key, if one is present
	// and unexpired.
	Lookup(ctx context.Context, key string) (map[string]any, bool)
	// Remember stores payload under key.
	Remember(ctx context.Context, key string, payload map[string]any)
	// Durable reports whether entries survive a process restart. Surfaced on
	// the health endpoint so an in-memory fallback is visible, not assumed.
	Durable() bool
	// Len reports the number of live entries, where cheaply knowable (-1 when
	// not). Used by tests to assert eviction actually happens.
	Len() int
	Close() error
}

// ---- bounded in-memory LRU + TTL -------------------------------------------

type memEntry struct {
	key     string
	payload map[string]any
	expires time.Time
}

// memReplayStore is a bounded LRU with a per-entry TTL. Eviction is by both:
// an entry leaves when it expires OR when the store is at capacity and it is
// the least recently used. Bounded is the point — see the file comment.
type memReplayStore struct {
	mu    sync.Mutex
	max   int
	ttl   time.Duration
	ll    *list.List               // front = most recently used
	items map[string]*list.Element // key -> element holding *memEntry

	// inflight gives single-flight semantics within one process: the second
	// concurrent request for a key waits for the first to finish rather than
	// executing a duplicate mutation.
	inflight map[string]chan struct{}

	now func() time.Time // injectable for deterministic TTL tests
}

func newMemReplayStore(max int, ttl time.Duration) *memReplayStore {
	if max <= 0 {
		max = defaultReplayMax
	}
	if ttl <= 0 {
		ttl = defaultReplayTTL
	}
	return &memReplayStore{
		max:      max,
		ttl:      ttl,
		ll:       list.New(),
		items:    map[string]*list.Element{},
		inflight: map[string]chan struct{}{},
		now:      time.Now,
	}
}

func (m *memReplayStore) Lookup(_ context.Context, key string) (map[string]any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	el, ok := m.items[key]
	if !ok {
		return nil, false
	}
	ent := el.Value.(*memEntry)
	if m.now().After(ent.expires) {
		m.removeElement(el)
		return nil, false
	}
	m.ll.MoveToFront(el)
	return ent.payload, true
}

func (m *memReplayStore) Remember(_ context.Context, key string, payload map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		ent := el.Value.(*memEntry)
		ent.payload = payload
		ent.expires = m.now().Add(m.ttl)
		m.ll.MoveToFront(el)
		return
	}
	el := m.ll.PushFront(&memEntry{key: key, payload: payload, expires: m.now().Add(m.ttl)})
	m.items[key] = el
	m.evictLocked()
}

// evictLocked drops expired entries first, then the least recently used until
// the store is within capacity. Caller holds the lock.
func (m *memReplayStore) evictLocked() {
	now := m.now()
	for el := m.ll.Back(); el != nil; {
		prev := el.Prev()
		if now.After(el.Value.(*memEntry).expires) {
			m.removeElement(el)
		}
		el = prev
	}
	for m.ll.Len() > m.max {
		if back := m.ll.Back(); back != nil {
			m.removeElement(back)
		}
	}
}

func (m *memReplayStore) removeElement(el *list.Element) {
	m.ll.Remove(el)
	delete(m.items, el.Value.(*memEntry).key)
}

func (m *memReplayStore) Durable() bool { return false }

func (m *memReplayStore) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Report live (unexpired) entries so a TTL test sees the truth rather than
	// a count padded with tombstones.
	n := 0
	now := m.now()
	for el := m.ll.Front(); el != nil; el = el.Next() {
		if !now.After(el.Value.(*memEntry).expires) {
			n++
		}
	}
	return n
}

func (m *memReplayStore) Close() error { return nil }

// begin claims key for this caller. It returns done (to be called when the
// work finishes) and wait — when wait is non-nil another caller already holds
// the key and this caller should block on it, then re-Lookup.
func (m *memReplayStore) begin(key string) (done func(), wait <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, held := m.inflight[key]; held {
		return nil, ch
	}
	ch := make(chan struct{})
	m.inflight[key] = ch
	return func() {
		m.mu.Lock()
		delete(m.inflight, key)
		m.mu.Unlock()
		close(ch)
	}, nil
}

// ---- redis-backed durable store --------------------------------------------

// redisClient is the small slice of go-redis this package uses, as an
// interface so the store is testable without a live Redis.
type redisClient interface {
	Get(ctx context.Context, key string) (string, error)
	SetEX(ctx context.Context, key, value string, ttl time.Duration) error
	Close() error
}

// redisReplayStore persists replay payloads outside the process, so a retry
// that arrives after a restart still replays. Eviction is Redis's own TTL.
type redisReplayStore struct {
	c      redisClient
	ttl    time.Duration
	prefix string
}

func newRedisReplayStore(c redisClient, ttl time.Duration) *redisReplayStore {
	if ttl <= 0 {
		ttl = defaultReplayTTL
	}
	return &redisReplayStore{c: c, ttl: ttl, prefix: "minio-mcp:replay:"}
}

func (r *redisReplayStore) Lookup(ctx context.Context, key string) (map[string]any, bool) {
	raw, err := r.c.Get(ctx, r.prefix+key)
	if err != nil || raw == "" {
		return nil, false
	}
	var out map[string]any
	if json.Unmarshal([]byte(raw), &out) != nil {
		return nil, false
	}
	return out, true
}

func (r *redisReplayStore) Remember(ctx context.Context, key string, payload map[string]any) {
	blob, err := json.Marshal(payload)
	if err != nil {
		return
	}
	// A failed write costs a replay, not correctness: the retry re-executes a
	// tool that is idempotent at the S3 layer. Never fail the caller's request
	// because the replay cache is unavailable.
	_ = r.c.SetEX(ctx, r.prefix+key, string(blob), r.ttl)
}

func (r *redisReplayStore) Durable() bool { return true }
func (r *redisReplayStore) Len() int      { return -1 }
func (r *redisReplayStore) Close() error  { return r.c.Close() }
