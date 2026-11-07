package outbound

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"sort"

	"mailforge/internal/emailaddr"
)

var (
	ErrNoPublicMXAddress = errors.New("MX host has no publicly routable IP address")
	ErrNullMX            = errors.New("recipient domain publishes a Null MX and does not accept email")
	ErrInvalidMXSet      = errors.New("recipient domain publishes an invalid MX set")
)

// MXResolver performs MX and address lookups. Direct-to-MX delivery must use
// LookupPublicIPs and dial the returned IPs directly; dialing the hostname
// after validating it would reintroduce a DNS-rebinding window.
type MXResolver struct {
	resolver *net.Resolver
}

func NewMXResolver() *MXResolver {
	return &MXResolver{resolver: net.DefaultResolver}
}

func (r *MXResolver) Lookup(ctx context.Context, email string) ([]*net.MX, error) {
	mailbox, err := emailaddr.ParseMailbox(email)
	if err != nil {
		return nil, fmt.Errorf("invalid recipient address: %w", err)
	}
	domain := mailbox.Domain
	records, err := r.resolver.LookupMX(ctx, domain)
	if err != nil {
		// RFC 5321 implicit MX fallback: when no MX record exists, try the
		// recipient domain itself. Its addresses are still subject to the
		// public-address validation performed before dialing.
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			return []*net.MX{{Host: domain, Pref: 0}}, nil
		}
		return nil, fmt.Errorf("MX lookup for %s: %w", domain, err)
	}

	if len(records) == 0 {
		return []*net.MX{{Host: domain, Pref: 0}}, nil
	}
	if err := detectNullMX(domain, records); err != nil {
		return nil, err
	}

	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Pref < records[j].Pref
	})

	// RFC 5321 requires randomizing hosts that have equal preference so
	// senders do not concentrate all traffic on one nominally equivalent MX.
	for start := 0; start < len(records); {
		end := start + 1
		for end < len(records) && records[end].Pref == records[start].Pref {
			end++
		}
		rand.Shuffle(end-start, func(i, j int) {
			records[start+i], records[start+j] = records[start+j], records[start+i]
		})
		start = end
	}
	return records, nil
}

func detectNullMX(domain string, records []*net.MX) error {
	if len(records) == 1 && records[0].Host == "." && records[0].Pref == 0 {
		return fmt.Errorf("%w: %s", ErrNullMX, domain)
	}
	for _, record := range records {
		if record.Host == "." {
			return fmt.Errorf("%w: %s: Null MX must be the only MX record", ErrInvalidMXSet, domain)
		}
	}
	return nil
}

// LookupPublicIPs returns only globally routable addresses. The caller must
// dial one of these IPs directly and keep host as the SMTP/TLS server name.
func (r *MXResolver) LookupPublicIPs(ctx context.Context, host string) ([]net.IP, error) {
	addresses, err := r.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve MX host %s: %w", host, err)
	}

	public := make([]net.IP, 0, len(addresses))
	for _, address := range addresses {
		if isPublicIP(address.IP) {
			public = append(public, address.IP)
		}
	}
	if len(public) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoPublicMXAddress, host)
	}
	return public, nil
}

// isPublicIP rejects private, local, link-local, shared, documentation,
// benchmarking and other special-purpose address ranges. This is deliberately
// stricter than net.IP.IsGlobalUnicast, which includes private addresses.
func isPublicIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsValid() || !addr.IsGlobalUnicast() || addr.IsPrivate() ||
		addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}

	blockedCIDRs := []string{
		"0.0.0.0/8",
		"100.64.0.0/10", // Shared address space (RFC 6598).
		"192.0.0.0/24",
		"192.0.2.0/24",
		"192.88.99.0/24",
		"198.18.0.0/15", // Benchmarking.
		"198.51.100.0/24",
		"203.0.113.0/24",
		"240.0.0.0/4",    // Reserved for future use.
		"100::/64",       // IPv6 discard-only prefix.
		"2001::/23",      // IPv6 protocol assignments / special purpose.
		"2001:db8::/32",  // Documentation.
		"2002::/16",      // 6to4 transition addresses.
		"64:ff9b::/96",   // Well-known NAT64 translation prefix.
		"64:ff9b:1::/48", // Local-use NAT64 translation prefix.
		"3fff::/20",      // IPv6 documentation range.
		"fec0::/10",      // Deprecated IPv6 site-local space.
	}
	for _, prefix := range blockedCIDRs {
		parsed, err := netip.ParsePrefix(prefix)
		if err == nil && parsed.Contains(addr) {
			return false
		}
	}
	return true
}
