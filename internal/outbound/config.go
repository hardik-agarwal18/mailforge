package outbound

import (
	"fmt"
	"net"
	"time"
)

const (
	defaultConnectTimeout   = 15 * time.Second
	defaultOperationTimeout = 30 * time.Second
	defaultDeliveryTimeout  = 2 * time.Minute
)

type Config struct {
	RelayHost       string
	RelayPort       int
	RequireSTARTTLS bool

	// OperationTimeout bounds each SMTP protocol operation. DeliveryTimeout
	// bounds DNS resolution and all MX attempts for one recipient.
	OperationTimeout time.Duration
	DeliveryTimeout  time.Duration
}

type Client struct {
	resolver *MXResolver
	dialer   *net.Dialer
	config   Config
}

func NewClient() *Client {
	return newClient(Config{})
}

// NewClientWithConfig creates a direct-to-MX client with explicit timeout
// settings. RelayHost/RelayPort are reserved for NewRelayClient.
func NewClientWithConfig(cfg Config) (*Client, error) {
	if cfg.RelayHost != "" || cfg.RelayPort != 0 {
		return nil, fmt.Errorf("direct MX client cannot use relay host or port; use NewRelayClient")
	}
	return newClient(cfg), nil
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
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = defaultOperationTimeout
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = defaultDeliveryTimeout
	}
	return &Client{
		resolver: NewMXResolver(),
		dialer: &net.Dialer{
			Timeout:   defaultConnectTimeout,
			KeepAlive: 30 * time.Second,
		},
		config: cfg,
	}
}
