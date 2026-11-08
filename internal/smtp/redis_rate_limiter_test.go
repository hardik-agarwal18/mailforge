package smtp

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewRedisAuthAttemptLimiterRequiresClient(t *testing.T) {
	if _, err := NewRedisAuthAttemptLimiter(nil, ""); err == nil {
		t.Fatal("NewRedisAuthAttemptLimiter() accepted a nil Redis client")
	}
}

func TestRedisAuthAttemptLimiterFailsClosedWhenRedisUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve an unused TCP address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved TCP address: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:         address,
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   0,
	})
	defer client.Close()

	limiter, err := NewRedisAuthAttemptLimiter(client, "mailforge:test:unavailable")
	if err != nil {
		t.Fatalf("NewRedisAuthAttemptLimiter() error = %v", err)
	}

	allowed, err := limiter.Allow(context.Background(), "192.0.2.10", "hardik", 5, 3, time.Minute)
	if err == nil {
		t.Fatal("Allow() error = nil, want Redis connection error")
	}
	if allowed {
		t.Fatal("Allow() = true when Redis is unavailable; limiter must fail closed")
	}
}

// This test exercises the real Lua script, including concurrent atomicity.
// Set REDIS_TEST_ADDR to a disposable Redis instance to enable it.
func TestRedisAuthAttemptLimiterIntegration(t *testing.T) {
	address := os.Getenv("REDIS_TEST_ADDR")
	if address == "" {
		t.Skip("set REDIS_TEST_ADDR to run Redis Lua-script integration tests")
	}

	client := redis.NewClient(&redis.Options{
		Addr:         address,
		Password:     os.Getenv("REDIS_TEST_PASSWORD"),
		DB:           15,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		MaxRetries:   0,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping disposable Redis test instance: %v", err)
	}

	prefix := fmt.Sprintf("mailforge:test:auth:%d", time.Now().UnixNano())
	limiter, err := NewRedisAuthAttemptLimiter(client, prefix)
	if err != nil {
		t.Fatalf("NewRedisAuthAttemptLimiter() error = %v", err)
	}

	// Remove only this test's keys, even if an assertion fails.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		keys, err := client.Keys(cleanupCtx, prefix+":*").Result()
		if err == nil && len(keys) > 0 {
			_ = client.Del(cleanupCtx, keys...).Err()
		}
	})

	// A blocked attempt must not partially consume the other dimension.
	allowed, err := limiter.Allow(ctx, "192.0.2.1", "alice", 1, 5, time.Minute)
	if err != nil || !allowed {
		t.Fatalf("first attempt allowed=%v error=%v, want allowed", allowed, err)
	}
	allowed, err = limiter.Allow(ctx, "192.0.2.1", "bob", 1, 5, time.Minute)
	if err != nil || allowed {
		t.Fatalf("second attempt allowed=%v error=%v, want IP-limited", allowed, err)
	}
	allowed, err = limiter.Allow(ctx, "192.0.2.2", "bob", 1, 5, time.Minute)
	if err != nil || !allowed {
		t.Fatalf("third attempt allowed=%v error=%v, want allowed (denied request must not increment username)", allowed, err)
	}

	// Username limits are shared across distinct source IPs.
	allowed, err = limiter.Allow(ctx, "192.0.2.3", "shared-user", 10, 1, time.Minute)
	if err != nil || !allowed {
		t.Fatalf("username baseline allowed=%v error=%v, want allowed", allowed, err)
	}
	allowed, err = limiter.Allow(ctx, "192.0.2.4", "shared-user", 10, 1, time.Minute)
	if err != nil || allowed {
		t.Fatalf("cross-IP username attempt allowed=%v error=%v, want username-limited", allowed, err)
	}

	// Exactly the per-IP limit must pass under contention across server-like callers.
	const workers = 64
	const ipLimit = 9
	var passed atomic.Int64
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			username := fmt.Sprintf("parallel-user-%d", i)
			ok, callErr := limiter.Allow(ctx, "198.51.100.9", username, ipLimit, 100, time.Minute)
			if callErr != nil {
				t.Errorf("parallel Allow() error = %v", callErr)
				return
			}
			if ok {
				passed.Add(1)
			}
		}(i)
	}
	wait.Wait()
	if got := passed.Load(); got != ipLimit {
		t.Fatalf("concurrent accepted attempts = %d, want exactly %d", got, ipLimit)
	}
}
