package outbound

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

const (
	defaultConnectTimeout   = 15 * time.Second
	defaultOperationTimeout = 30 * time.Second
	defaultDeliveryTimeout  = 2 * time.Minute
	defaultEHLOName         = "localhost"
)

type Config struct {
	RelayHost       string
	RelayPort       int
	RequireSTARTTLS bool

	// EHLOName is the public hostname MailForge presents in EHLO/HELO.
	// Configure this to the server's externally resolvable FQDN in production.
	EHLOName string

	// ConnectTimeout bounds each TCP connection attempt. OperationTimeout
	// bounds each SMTP protocol operation. DeliveryTimeout bounds DNS
	// resolution and all MX attempts for one recipient.
	ConnectTimeout   time.Duration
	OperationTimeout time.Duration
	DeliveryTimeout  time.Duration
}

type Client struct {
	resolver mxResolver
	dialer   contextDialer
	config   Config
}

type mxResolver interface {
	Lookup(ctx context.Context, email string) ([]*net.MX, error)
	LookupPublicIPs(ctx context.Context, host string) ([]net.IP, error)
}

type contextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

func NewClient() *Client {
	return newClient(Config{}, NewMXResolver(), nil)
}

// NewClientWithConfig creates a direct-to-MX client with explicit timeout
// settings. RelayHost/RelayPort are reserved for NewRelayClient.
func NewClientWithConfig(cfg Config) (*Client, error) {
	if cfg.RelayHost != "" || cfg.RelayPort != 0 {
		return nil, fmt.Errorf("direct MX client cannot use relay host or port; use NewRelayClient")
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return newClient(cfg, NewMXResolver(), nil), nil
}

func NewRelayClient(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.RelayHost) == "" {
		return nil, fmt.Errorf("relay host is required")
	}
	if cfg.RelayPort < 1 || cfg.RelayPort > 65535 {
		return nil, fmt.Errorf("invalid relay port: %d", cfg.RelayPort)
	}
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return newClient(cfg, NewMXResolver(), nil), nil
}

func validateConfig(cfg *Config) error {
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.OperationTimeout <= 0 {
		cfg.OperationTimeout = defaultOperationTimeout
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = defaultDeliveryTimeout
	}
	if strings.TrimSpace(cfg.EHLOName) == "" {
		cfg.EHLOName = defaultEHLOName
	}
	if err := validateEHLOName(cfg.EHLOName); err != nil {
		return fmt.Errorf("invalid SMTP EHLO name: %w", err)
	}
	return nil
}

func newClient(cfg Config, resolver mxResolver, dialer contextDialer) *Client {
	if err := validateConfig(&cfg); err != nil {
		// The zero-configuration NewClient constructor deliberately remains
		// infallible; its only default identity is the safe development value.
		cfg = Config{
			EHLOName:         defaultEHLOName,
			ConnectTimeout:   defaultConnectTimeout,
			OperationTimeout: defaultOperationTimeout,
			DeliveryTimeout:  defaultDeliveryTimeout,
		}
	}
	if resolver == nil {
		resolver = NewMXResolver()
	}
	if dialer == nil {
		dialer = &net.Dialer{
			Timeout:   cfg.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}
	}
	return &Client{resolver: resolver, dialer: dialer, config: cfg}
}

// validateEHLOName prevents command injection and ensures the identity is an
// ASCII DNS name. Single-label names are accepted for local development;
// production deployments should configure a public fully-qualified hostname.
func validateEHLOName(name string) error {
	if name == "" || len(name) > 255 || strings.TrimSpace(name) != name {
		return fmt.Errorf("must be a non-empty DNS hostname no longer than 255 octets")
	}
	if strings.ContainsAny(name, " \t\r\n[]") {
		return fmt.Errorf("contains whitespace or unsupported characters")
	}
	if !isASCII(name) {
		return fmt.Errorf("must use ASCII/IDNA form")
	}
	if strings.HasSuffix(name, ".") {
		name = strings.TrimSuffix(name, ".")
	}
	if name == "" {
		return fmt.Errorf("hostname is empty")
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 {
			return fmt.Errorf("contains an empty or overlong DNS label")
		}
		if !isAlphaNumeric(label[0]) || !isAlphaNumeric(label[len(label)-1]) {
			return fmt.Errorf("DNS labels must begin and end with a letter or digit")
		}
		for i := 0; i < len(label); i++ {
			ch := label[i]
			if !isAlphaNumeric(ch) && ch != '-' {
				return fmt.Errorf("contains an invalid DNS label character")
			}
		}
	}
	return nil
}

func isASCII(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] >= 128 {
			return false
		}
	}
	return true
}

func isAlphaNumeric(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9')
}
