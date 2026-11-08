package smtp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultAuthRateLimitPrefix = "mailforge:smtp:auth:v1"

// redisAuthAttemptScript checks both dimensions before incrementing either one.
// Running it as a Lua script makes the combined IP+username decision atomic.
const redisAuthAttemptScript = `
local ip_limit = tonumber(ARGV[1])
local username_limit = tonumber(ARGV[2])
local window_ms = tonumber(ARGV[3])

local ip_count = tonumber(redis.call('GET', KEYS[1]) or '0')
local username_count = tonumber(redis.call('GET', KEYS[2]) or '0')

if ip_count >= ip_limit or username_count >= username_limit then
    return 0
end

local ip_next = redis.call('INCR', KEYS[1])
if ip_next == 1 or redis.call('PTTL', KEYS[1]) == -1 then
    redis.call('PEXPIRE', KEYS[1], window_ms)
end

local username_next = redis.call('INCR', KEYS[2])
if username_next == 1 or redis.call('PTTL', KEYS[2]) == -1 then
    redis.call('PEXPIRE', KEYS[2], window_ms)
end

return 1
`

// RedisAuthAttemptLimiter stores authentication attempt counters in Redis so
// every SMTP submission replica enforces the same client-IP and username
// limits. The operation fails closed whenever Redis cannot be reached.
//
// The Lua script touches two keys in one atomic operation. For Redis Cluster,
// both keys are deliberately placed in the same hash slot; this favors atomic
// enforcement over distributing this small authentication workload across
// cluster slots.
type RedisAuthAttemptLimiter struct {
	client redis.UniversalClient
	prefix string
	script *redis.Script
}

// NewRedisAuthAttemptLimiter creates a shared authentication limiter. A blank
// prefix selects the stable default namespace.
func NewRedisAuthAttemptLimiter(client redis.UniversalClient, prefix string) (*RedisAuthAttemptLimiter, error) {
	if client == nil {
		return nil, errors.New("Redis client is required for distributed SMTP authentication rate limiting")
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultAuthRateLimitPrefix
	}
	if strings.ContainsAny(prefix, "{} \t\r\n") {
		return nil, errors.New("Redis authentication rate-limit prefix must not contain braces or whitespace")
	}

	return &RedisAuthAttemptLimiter{
		client: client,
		prefix: prefix,
		script: redis.NewScript(redisAuthAttemptScript),
	}, nil
}

// Allow atomically checks and records one AUTH attempt against both the client
// IP and normalized username. Successful and unsuccessful authentication
// exchanges both consume an attempt, matching the SMTP server's previous
// process-local policy.
func (l *RedisAuthAttemptLimiter) Allow(
	ctx context.Context,
	clientIP string,
	username string,
	ipLimit int,
	usernameLimit int,
	window time.Duration,
) (bool, error) {
	if l == nil || l.client == nil || l.script == nil {
		return false, errors.New("Redis SMTP authentication rate limiter is not configured")
	}
	if ctx == nil {
		return false, errors.New("context is required for Redis SMTP authentication rate limiting")
	}
	if strings.TrimSpace(clientIP) == "" || strings.TrimSpace(username) == "" {
		return false, errors.New("client IP and username are required for SMTP authentication rate limiting")
	}
	if ipLimit < 1 || usernameLimit < 1 || window <= 0 {
		return false, errors.New("SMTP authentication rate limits and window must be positive")
	}
	windowMS := window.Milliseconds()
	if windowMS < 1 {
		windowMS = 1
	}

	ipKey := l.key("ip", normalizeRateLimitIP(clientIP))
	usernameKey := l.key("username", strings.ToLower(strings.TrimSpace(username)))
	allowed, err := l.script.Run(ctx, l.client, []string{ipKey, usernameKey}, ipLimit, usernameLimit, windowMS).Int64()
	if err != nil {
		return false, fmt.Errorf("execute Redis SMTP authentication rate-limit script: %w", err)
	}
	return allowed == 1, nil
}

func (l *RedisAuthAttemptLimiter) key(dimension, value string) string {
	sum := sha256.Sum256([]byte(value))
	// A shared hash tag keeps both keys passed to the Lua script in one Redis
	// Cluster slot, preserving the atomic two-dimensional decision.
	return l.prefix + ":{smtp-auth}:" + dimension + ":" + hex.EncodeToString(sum[:])
}

func normalizeRateLimitIP(value string) string {
	value = strings.TrimSpace(value)
	if parsed := net.ParseIP(value); parsed != nil {
		return parsed.String()
	}
	return value
}
