package smtp

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// ServerLimits bounds SMTP resource consumption. Authentication attempt counters
// are process-local; deployments with multiple submission replicas should put
// authentication counters in a shared store. Message quotas are enforced
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
