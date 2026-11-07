package outbound

import (
	"fmt"
	"testing"
)

func TestRelayTLSModeDefaultsByPort(t *testing.T) {
	for _, test := range []struct {
		port int
		want TLSMode
	}{
		{port: 465, want: TLSModeImplicit},
		{port: 587, want: TLSModeRequiredSTARTTLS},
		{port: 25, want: TLSModeStartTLS},
		{port: 1025, want: TLSModeNone},
	} {
		t.Run(fmt.Sprintf("port-%d", test.port), func(t *testing.T) {
			client, err := NewRelayClient(Config{RelayHost: "smtp.example.test", RelayPort: test.port})
			if err != nil {
				t.Fatalf("NewRelayClient: %v", err)
			}
			if client.config.RelayTLSMode != test.want {
				t.Fatalf("RelayTLSMode = %q, want %q", client.config.RelayTLSMode, test.want)
			}
		})
	}
}

func TestRelayTLSModeRejectsUnknownValue(t *testing.T) {
	if _, err := NewRelayClient(Config{
		RelayHost:    "smtp.example.test",
		RelayPort:    2525,
		RelayTLSMode: TLSMode("stls"),
	}); err == nil {
		t.Fatal("NewRelayClient accepted an unknown TLS mode")
	}
}

func TestRelayTLSModeRequiresSTARTTLSWhenConfigured(t *testing.T) {
	client, err := NewRelayClient(Config{
		RelayHost:       "smtp.example.test",
		RelayPort:       25,
		RequireSTARTTLS: true,
	})
	if err != nil {
		t.Fatalf("NewRelayClient: %v", err)
	}
	if client.config.RelayTLSMode != TLSModeRequiredSTARTTLS {
		t.Fatalf("RelayTLSMode = %q, want %q", client.config.RelayTLSMode, TLSModeRequiredSTARTTLS)
	}
}
