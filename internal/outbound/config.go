package outbound

import (
	"fmt"
	"net"
	"time"
)

type Config struct {
	RelayHost       string
	RelayPort       int
	RequireSTARTTLS bool
}

type Client struct {
	resolver *MXResolver
	dialer   *net.Dialer
	config   Config
}

func NewClient() *Client {
	return newClient(Config{})
}

func NewRelayClient(cfg Config) (*Client, error) {
	if cfg.RelayHost == "" {
		return nil, fmt.Errorf("relay host is required")
	}

	if cfg.RelayPort < 1 || cfg.RelayPort > 65535 {
		return nil, fmt.Errorf("invalid relay port: %d", cfg.RelayPort)
	}

	return newClient(cfg), nil
}

func newClient(cfg Config) *Client {
	return &Client{
		resolver: NewMXResolver(),
		dialer: &net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		},
		config: cfg,
	}
}
