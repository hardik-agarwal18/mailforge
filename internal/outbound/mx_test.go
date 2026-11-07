package outbound

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestIsPublicIP(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{name: "public IPv4", ip: "8.8.8.8", want: true},
		{name: "public IPv6", ip: "2606:4700:4700::1111", want: true},
		{name: "RFC1918", ip: "10.0.0.1", want: false},
		{name: "loopback", ip: "127.0.0.1", want: false},
		{name: "link local metadata", ip: "169.254.169.254", want: false},
		{name: "shared carrier grade NAT", ip: "100.64.0.1", want: false},
		{name: "documentation IPv4", ip: "192.0.2.10", want: false},
		{name: "reserved IPv4", ip: "240.0.0.1", want: false},
		{name: "IPv6 unique local", ip: "fd00::1", want: false},
		{name: "IPv6 discard only", ip: "100::1", want: false},
		{name: "IPv6 documentation", ip: "3fff::1", want: false},
		{name: "IPv6 link local", ip: "fe80::1", want: false},
		{name: "IPv6 documentation", ip: "2001:db8::1", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isPublicIP(net.ParseIP(test.ip)); got != test.want {
				t.Fatalf("isPublicIP(%s) = %v, want %v", test.ip, got, test.want)
			}
		})
	}
}

func TestMXResolverRejectsMalformedRecipientBeforeDNS(t *testing.T) {
	resolver := NewMXResolver()
	_, err := resolver.Lookup(context.Background(), "recipient@@example.com")
	if err == nil {
		t.Fatal("Lookup() accepted malformed recipient address")
	}
}

func TestNullMXDetection(t *testing.T) {
	err := detectNullMX("example.invalid", []*net.MX{{Host: ".", Pref: 0}})
	if !errors.Is(err, ErrNullMX) {
		t.Fatalf("detectNullMX error = %v, want ErrNullMX", err)
	}
	if err := detectNullMX("example.invalid", []*net.MX{{Host: "mx.example.invalid.", Pref: 10}}); err != nil {
		t.Fatalf("ordinary MX should be accepted: %v", err)
	}
	if err := detectNullMX("example.invalid", []*net.MX{{Host: ".", Pref: 0}, {Host: "mx.example.invalid.", Pref: 10}}); !errors.Is(err, ErrInvalidMXSet) {
		t.Fatalf("mixed Null MX and ordinary MX set error = %v, want ErrInvalidMXSet", err)
	}
}
