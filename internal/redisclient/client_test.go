package redisclient

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewFromEnvUsesRedissURLAndPreservesURLDatabase(t *testing.T) {
	t.Setenv("REDIS_URL", "rediss://default:test-secret@redis.example.com:6379/2")
	t.Setenv("REDIS_ADDR", "127.0.0.1:6379")
	t.Setenv("REDIS_PASSWORD", "local-password")

	client, err := NewFromEnv(redis.Options{
		PoolSize:     20,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxRetries:   1,
	})
	if err != nil {
		t.Fatalf("NewFromEnv() error = %v", err)
	}
	defer client.Close()

	options := client.Options()
	if options.Addr != "redis.example.com:6379" {
		t.Errorf("Addr = %q, want redis.example.com:6379", options.Addr)
	}
	if options.Username != "default" {
		t.Errorf("Username = %q, want default", options.Username)
	}
	if options.Password != "test-secret" {
		t.Errorf("Password was not taken from REDIS_URL")
	}
	if options.DB != 2 {
		t.Errorf("DB = %d, want URL database 2", options.DB)
	}
	if options.TLSConfig == nil {
		t.Fatal("TLSConfig is nil for rediss:// URL")
	}
	if options.PoolSize != 20 {
		t.Errorf("PoolSize = %d, want 20", options.PoolSize)
	}
	if options.DialTimeout != 3*time.Second {
		t.Errorf("DialTimeout = %s, want 3s", options.DialTimeout)
	}
	if options.ReadTimeout != 10*time.Second {
		t.Errorf("ReadTimeout = %s, want 10s", options.ReadTimeout)
	}
	if options.WriteTimeout != 5*time.Second {
		t.Errorf("WriteTimeout = %s, want 5s", options.WriteTimeout)
	}
	if options.MaxRetries != 1 {
		t.Errorf("MaxRetries = %d, want 1", options.MaxRetries)
	}
}

func TestNewFromEnvFallsBackToLocalAddressAndPassword(t *testing.T) {
	t.Setenv("REDIS_URL", "")
	t.Setenv("REDIS_ADDR", "127.0.0.1:6381")
	t.Setenv("REDIS_PASSWORD", "local-secret")

	client, err := NewFromEnv(redis.Options{DB: 0, ReadTimeout: 4 * time.Second})
	if err != nil {
		t.Fatalf("NewFromEnv() error = %v", err)
	}
	defer client.Close()

	options := client.Options()
	if options.Addr != "127.0.0.1:6381" {
		t.Errorf("Addr = %q, want 127.0.0.1:6381", options.Addr)
	}
	if options.Password != "local-secret" {
		t.Errorf("Password was not taken from REDIS_PASSWORD")
	}
	if options.ReadTimeout != 4*time.Second {
		t.Errorf("ReadTimeout = %s, want 4s", options.ReadTimeout)
	}
}

func TestNewFromEnvUsesDefaultLocalAddress(t *testing.T) {
	t.Setenv("REDIS_URL", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("REDIS_PASSWORD", "")

	client, err := NewFromEnv(redis.Options{})
	if err != nil {
		t.Fatalf("NewFromEnv() error = %v", err)
	}
	defer client.Close()

	if got := client.Options().Addr; got != defaultAddress {
		t.Errorf("Addr = %q, want %q", got, defaultAddress)
	}
}

func TestNewFromEnvRejectsInvalidURL(t *testing.T) {
	t.Setenv("REDIS_URL", "not-a-valid-redis-url")

	client, err := NewFromEnv(redis.Options{})
	if err == nil {
		if client != nil {
			_ = client.Close()
		}
		t.Fatal("NewFromEnv() error = nil, want invalid REDIS_URL error")
	}
}
