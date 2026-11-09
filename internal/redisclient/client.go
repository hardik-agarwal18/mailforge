// Package redisclient centralizes Redis connection configuration for MailForge.
//
// REDIS_URL takes precedence when set. It accepts redis:// and rediss:// URLs;
// go-redis configures TLS automatically for rediss://. When REDIS_URL is not
// set, the package falls back to REDIS_ADDR and REDIS_PASSWORD for local
// development with a self-hosted Redis instance.
package redisclient

import (
	"errors"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

const defaultAddress = "127.0.0.1:6379"

// NewFromEnv creates a Redis client using REDIS_URL when provided, otherwise
// using REDIS_ADDR and REDIS_PASSWORD. The supplied defaults allow each
// MailForge process to retain its existing pool and timeout configuration.
//
// Connection identity settings (address, username, password, database and TLS)
// from REDIS_URL are preserved. Only operational pool/timeout settings from
// defaults are overlaid onto URL-based configuration.
func NewFromEnv(defaults redis.Options) (*redis.Client, error) {
	redisURL := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if redisURL != "" {
		parsed, err := redis.ParseURL(redisURL)
		if err != nil {
			// Do not echo the URL or parser error: either may contain credentials.
			return nil, errors.New("invalid REDIS_URL: expected a valid redis:// or rediss:// URL")
		}

		applyOperationalDefaults(parsed, defaults)
		return redis.NewClient(parsed), nil
	}

	options := defaults
	address := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if address != "" {
		options.Addr = address
	}
	if strings.TrimSpace(options.Addr) == "" {
		options.Addr = defaultAddress
	}
	if password, ok := os.LookupEnv("REDIS_PASSWORD"); ok {
		options.Password = password
	}

	return redis.NewClient(&options), nil
}

// applyOperationalDefaults copies only non-zero tuning values. In particular,
// it deliberately does not overwrite URL-provided DB, credentials, address,
// protocol, or TLS settings.
func applyOperationalDefaults(target *redis.Options, defaults redis.Options) {
	if defaults.PoolSize > 0 {
		target.PoolSize = defaults.PoolSize
	}
	if defaults.MinIdleConns > 0 {
		target.MinIdleConns = defaults.MinIdleConns
	}
	if defaults.MaxIdleConns > 0 {
		target.MaxIdleConns = defaults.MaxIdleConns
	}
	if defaults.DialTimeout > 0 {
		target.DialTimeout = defaults.DialTimeout
	}
	if defaults.ReadTimeout != 0 {
		target.ReadTimeout = defaults.ReadTimeout
	}
	if defaults.WriteTimeout != 0 {
		target.WriteTimeout = defaults.WriteTimeout
	}
	if defaults.PoolTimeout > 0 {
		target.PoolTimeout = defaults.PoolTimeout
	}
	if defaults.MaxRetries != 0 {
		target.MaxRetries = defaults.MaxRetries
	}
}
