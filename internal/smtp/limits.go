package smtp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
)

// ServerLimits bounds SMTP resource consumption. Authentication attempt limits
// are applied by the configured AuthAttemptLimiter. Message quotas are enforced
// separately by the PostgreSQL message repository.
type ServerLimits struct {
	MaxConnections          int
	MaxRecipients           int
	MaxConcurrentAuth       int
	MaxConcurrentData       int
	AuthAttemptsPerIP       int
	AuthAttemptsPerUsername int
	AuthWindow              time.Duration
}

func DefaultServerLimits() ServerLimits {
	return ServerLimits{
		MaxConnections:          100,
		MaxRecipients:           100,
		MaxConcurrentAuth:       10,
		MaxConcurrentData:       10,
		AuthAttemptsPerIP:       100,
		AuthAttemptsPerUsername: 30,
		AuthWindow:              5 * time.Minute,
	}
}

func normalizeServerLimits(limits ServerLimits) ServerLimits {
	defaults := DefaultServerLimits()
	if limits.MaxConnections <= 0 {
		limits.MaxConnections = defaults.MaxConnections
	}
	if limits.MaxRecipients <= 0 {
		limits.MaxRecipients = defaults.MaxRecipients
	}
	if limits.MaxConcurrentAuth <= 0 {
		limits.MaxConcurrentAuth = defaults.MaxConcurrentAuth
	}
	if limits.MaxConcurrentData <= 0 {
		limits.MaxConcurrentData = defaults.MaxConcurrentData
	}
	if limits.AuthAttemptsPerIP <= 0 {
		limits.AuthAttemptsPerIP = defaults.AuthAttemptsPerIP
	}
	if limits.AuthAttemptsPerUsername <= 0 {
		limits.AuthAttemptsPerUsername = defaults.AuthAttemptsPerUsername
	}
	if limits.AuthWindow <= 0 {
		limits.AuthWindow = defaults.AuthWindow
	}
	return limits
}

type windowCounter struct {
	startedAt time.Time
	lastSeen  time.Time
	count     int
	window    time.Duration
}

type fixedWindowLimiter struct {
	mu         sync.Mutex
	entries    map[string]windowCounter
	maxEntries int
}

func newFixedWindowLimiter(maxEntries int) *fixedWindowLimiter {
	if maxEntries < 1 {
		maxEntries = 10_000
	}
	return &fixedWindowLimiter{
		entries:    make(map[string]windowCounter),
		maxEntries: maxEntries,
	}
}

func (l *fixedWindowLimiter) Allow(key string, limit int, window time.Duration) bool {
	if l == nil || key == "" || limit < 1 || window <= 0 {
		return false
	}

	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, exists := l.entries[key]
	if !exists && len(l.entries) >= l.maxEntries {
		// Bound memory even if an attacker rotates usernames/IPs. Expired keys
		// are reclaimed opportunistically; new keys fail closed while full.
		for existingKey, existing := range l.entries {
			if now.Sub(existing.lastSeen) > 2*existing.window {
				delete(l.entries, existingKey)
			}
		}
		if len(l.entries) >= l.maxEntries {
			return false
		}
	}

	if !exists || now.Sub(entry.startedAt) >= window || entry.window != window {
		entry = windowCounter{startedAt: now, lastSeen: now, window: window}
	}
	entry.lastSeen = now
	if entry.count >= limit {
		l.entries[key] = entry
		return false
	}
	entry.count++
	l.entries[key] = entry
	return true
}

func limiterKey(namespace, value string) string {
	sum := sha256.Sum256([]byte(value))
	return namespace + ":" + hex.EncodeToString(sum[:])
}

// AllowPair applies the IP and username counters atomically within this process.
// It is retained as a development/test fallback; production submission servers
// should use RedisAuthAttemptLimiter so all replicas share the same counters.
func (l *fixedWindowLimiter) AllowPair(ipKey, usernameKey string, ipLimit, usernameLimit int, window time.Duration) bool {
	if l == nil || ipKey == "" || usernameKey == "" || ipLimit < 1 || usernameLimit < 1 || window <= 0 {
		return false
	}

	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	missing := 0
	if _, exists := l.entries[ipKey]; !exists {
		missing++
	}
	if _, exists := l.entries[usernameKey]; !exists {
		missing++
	}
	if len(l.entries)+missing > l.maxEntries {
		for existingKey, existing := range l.entries {
			if now.Sub(existing.lastSeen) > 2*existing.window {
				delete(l.entries, existingKey)
			}
		}
		missing = 0
		if _, exists := l.entries[ipKey]; !exists {
			missing++
		}
		if _, exists := l.entries[usernameKey]; !exists {
			missing++
		}
		if len(l.entries)+missing > l.maxEntries {
			return false
		}
	}

	ipEntry := activeWindowEntry(l.entries[ipKey], window, now)
	usernameEntry := activeWindowEntry(l.entries[usernameKey], window, now)
	if ipEntry.count >= ipLimit || usernameEntry.count >= usernameLimit {
		return false
	}
	ipEntry.count++
	ipEntry.lastSeen = now
	usernameEntry.count++
	usernameEntry.lastSeen = now
	l.entries[ipKey] = ipEntry
	l.entries[usernameKey] = usernameEntry
	return true
}

func activeWindowEntry(entry windowCounter, window time.Duration, now time.Time) windowCounter {
	if entry.startedAt.IsZero() || now.Sub(entry.startedAt) >= window || entry.window != window {
		return windowCounter{startedAt: now, lastSeen: now, window: window}
	}
	return entry
}

type localAuthAttemptLimiter struct {
	limiter *fixedWindowLimiter
}

func (l *localAuthAttemptLimiter) Allow(ctx context.Context, clientIP, username string, ipLimit, usernameLimit int, window time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if l == nil || l.limiter == nil {
		return false, fmt.Errorf("local SMTP authentication limiter is not configured")
	}
	return l.limiter.AllowPair(
		limiterKey("auth-ip", clientIP),
		limiterKey("auth-user", strings.ToLower(strings.TrimSpace(username))),
		ipLimit, usernameLimit, window,
	), nil
}
