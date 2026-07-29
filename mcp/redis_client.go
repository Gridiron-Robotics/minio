package main

// Concrete go-redis binding for the durable replay store. Kept in its own file
// so the rest of the sidecar depends only on the small `redisClient`
// interface in replay.go and stays testable without a live Redis.

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type goRedisClient struct{ rdb *redis.Client }

// dialRedis parses a redis:// URL and returns a client. It does NOT ping —
// connection health is reported by the health endpoint rather than made a boot
// dependency, so a Redis outage degrades replay durability instead of taking
// the storage surface down with it.
func dialRedis(url string) (redisClient, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return &goRedisClient{rdb: redis.NewClient(opt)}, nil
}

func (g *goRedisClient) Get(ctx context.Context, key string) (string, error) {
	return g.rdb.Get(ctx, key).Result()
}

func (g *goRedisClient) SetEX(ctx context.Context, key, value string, ttl time.Duration) error {
	return g.rdb.Set(ctx, key, value, ttl).Err()
}

func (g *goRedisClient) Close() error { return g.rdb.Close() }

// ping reports whether the durable store is actually reachable right now.
func (g *goRedisClient) ping(ctx context.Context) error { return g.rdb.Ping(ctx).Err() }
