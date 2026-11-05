package outbound

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
)

type MXResolver struct {
	resolver *net.Resolver
}

func NewMXResolver() *MXResolver {
	return &MXResolver{
		resolver: net.DefaultResolver,
	}
}

func (r *MXResolver) Lookup(
	ctx context.Context,
	email string,
) ([]*net.MX, error) {

	parts := strings.Split(email, "@")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("invalid recipient address: %s", email)
	}

	domain := strings.TrimSuffix(parts[1], ".")

	records, err := r.resolver.LookupMX(ctx, domain)
	if err != nil {
		// RFC 5321 implicit MX fallback:
		// If there are no MX records, try the domain itself.
		if dnsErr, ok := err.(*net.DNSError); ok && dnsErr.IsNotFound {
			return []*net.MX{
				{Host: domain, Pref: 0},
			}, nil
		}

		return nil, fmt.Errorf("MX lookup for %s: %w", domain, err)
	}

	if len(records) == 0 {
		return []*net.MX{
			{Host: domain, Pref: 0},
		}, nil
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Pref < records[j].Pref
	})

	return records, nil
}
