// Package domainauth performs DNS configuration checks for SPF, DKIM and
// DMARC. It is a diagnostics tool, not a complete SPF authorization engine.
package domainauth

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type TXTResolver interface {
	LookupTXT(context.Context, string) ([]string, error)
}

type Check struct {
	Name   string   `json:"name"`
	Status string   `json:"status"` // pass, warning, fail
	Record string   `json:"record,omitempty"`
	Issues []string `json:"issues,omitempty"`
}

type Result struct {
	Domain   string `json:"domain"`
	Selector string `json:"selector"`
	SPF      Check  `json:"spf"`
	DKIM     Check  `json:"dkim"`
	DMARC    Check  `json:"dmarc"`
}

var enhancedSPFMechanism = regexp.MustCompile(`(?i)^(?:include:|a(?:$|[:/])|mx(?:$|[:/])|ptr(?:$|:)|exists:|redirect=|exp=)`)

func Inspect(ctx context.Context, resolver TXTResolver, domain, selector, expectedDKIMRecord string) (Result, error) {
	if resolver == nil {
		return Result{}, fmt.Errorf("DNS TXT resolver is required")
	}
	domain = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
	selector = strings.ToLower(strings.TrimSpace(selector))
	if domain == "" || selector == "" {
		return Result{}, fmt.Errorf("domain and DKIM selector are required")
	}
	result := Result{Domain: domain, Selector: selector}
	var err error
	result.SPF, err = inspectSPF(ctx, resolver, domain)
	if err != nil {
		return result, err
	}
	result.DKIM, err = inspectDKIM(ctx, resolver, selector, domain, expectedDKIMRecord)
	if err != nil {
		return result, err
	}
	result.DMARC, err = inspectDMARC(ctx, resolver, domain)
	if err != nil {
		return result, err
	}
	return result, nil
}

func inspectSPF(ctx context.Context, resolver TXTResolver, domain string) (Check, error) {
	check := Check{Name: "SPF", Status: "fail"}
	records, err := resolver.LookupTXT(ctx, domain)
	if err != nil {
		return check, fmt.Errorf("lookup SPF TXT for %s: %w", domain, err)
	}
	var spf []string
	for _, record := range records {
		fields := strings.Fields(strings.TrimSpace(record))
		if len(fields) > 0 && strings.EqualFold(fields[0], "v=spf1") {
			spf = append(spf, strings.TrimSpace(record))
		}
	}
	switch len(spf) {
	case 0:
		check.Issues = []string{"no v=spf1 TXT record found"}
		return check, nil
	case 1:
		check.Record = spf[0]
	default:
		check.Issues = []string{fmt.Sprintf("found %d SPF records; publish exactly one", len(spf))}
		return check, nil
	}
	fields := strings.Fields(check.Record)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "v=spf1") {
		check.Issues = []string{"SPF record does not begin with v=spf1"}
		return check, nil
	}
	lookupTerms := 0
	allCount := 0
	for index, field := range fields[1:] {
		lower := strings.ToLower(field)
		if enhancedSPFMechanism.MatchString(lower) {
			lookupTerms++
		}
		mechanism := strings.TrimLeft(lower, "+?~-")
		if mechanism == "all" {
			allCount++
			if index != len(fields[1:])-1 {
				check.Issues = append(check.Issues, "the all mechanism should be the final SPF term")
			}
		}
	}
	if lookupTerms > 10 {
		check.Issues = append(check.Issues, fmt.Sprintf("record has at least %d DNS-lookup terms; SPF permits at most 10", lookupTerms))
	}
	if allCount == 0 {
		check.Issues = append(check.Issues, "record has no all mechanism; unmatched senders may receive a neutral result")
	}
	if check.Record == "" {
		check.Issues = append(check.Issues, "SPF record is empty")
	}
	if len(check.Issues) > 0 {
		check.Status = "warning"
	} else {
		check.Status = "pass"
	}
	return check, nil
}

func inspectDKIM(ctx context.Context, resolver TXTResolver, selector, domain, expected string) (Check, error) {
	name := selector + "._domainkey." + domain
	check := Check{Name: "DKIM", Status: "fail"}
	records, err := resolver.LookupTXT(ctx, name)
	if err != nil {
		return check, fmt.Errorf("lookup DKIM TXT for %s: %w", name, err)
	}
	if len(records) != 1 {
		check.Issues = []string{fmt.Sprintf("expected exactly one DKIM TXT record at %s, found %d", name, len(records))}
		return check, nil
	}
	check.Record = records[0]
	tags := parseTags(check.Record)
	if value := tags["v"]; value != "" && !strings.EqualFold(value, "DKIM1") {
		check.Issues = append(check.Issues, "DKIM v tag must be DKIM1")
	}
	if keyType := tags["k"]; keyType != "" && !strings.EqualFold(keyType, "rsa") {
		check.Issues = append(check.Issues, "published DKIM key type is not rsa")
	}
	if strings.TrimSpace(tags["p"]) == "" {
		check.Issues = append(check.Issues, "DKIM p tag is empty; the key is revoked or missing")
	} else {
		publicBytes, decodeErr := base64.StdEncoding.DecodeString(tags["p"])
		if decodeErr != nil {
			check.Issues = append(check.Issues, "DKIM p tag is not valid base64")
		} else {
			publicKey, keyErr := x509.ParsePKCS1PublicKey(publicBytes)
			if keyErr != nil {
				parsed, parseErr := x509.ParsePKIXPublicKey(publicBytes)
				if parseErr == nil {
					publicKey, _ = parsed.(*rsa.PublicKey)
				}
			}
			if publicKey == nil {
				check.Issues = append(check.Issues, "DKIM p tag does not contain a valid RSA public key")
			} else if publicKey.N.BitLen() < 2048 {
				check.Issues = append(check.Issues, "published DKIM RSA key is smaller than MailForge's 2048-bit signing minimum")
			}
		}
	}
	if expected != "" && canonicalTags(check.Record) != canonicalTags(expected) {
		check.Issues = append(check.Issues, "published DKIM public key does not match the configured active private key")
	}
	if len(check.Issues) > 0 {
		check.Status = "fail"
	} else {
		check.Status = "pass"
	}
	return check, nil
}

func inspectDMARC(ctx context.Context, resolver TXTResolver, domain string) (Check, error) {
	name := "_dmarc." + domain
	check := Check{Name: "DMARC", Status: "fail"}
	records, err := resolver.LookupTXT(ctx, name)
	if err != nil {
		return check, fmt.Errorf("lookup DMARC TXT for %s: %w", name, err)
	}
	if len(records) != 1 {
		check.Issues = []string{fmt.Sprintf("expected exactly one DMARC TXT record at %s, found %d", name, len(records))}
		return check, nil
	}
	check.Record = records[0]
	tags := parseTags(check.Record)
	if !strings.EqualFold(tags["v"], "DMARC1") {
		check.Issues = append(check.Issues, "DMARC v tag must be DMARC1")
	}
	policy := strings.ToLower(tags["p"])
	if policy != "none" && policy != "quarantine" && policy != "reject" {
		check.Issues = append(check.Issues, "DMARC p tag must be none, quarantine, or reject")
	}
	if policy == "none" {
		check.Issues = append(check.Issues, "DMARC policy is monitoring-only (p=none)")
	}
	for _, tag := range []string{"aspf", "adkim"} {
		if value := strings.ToLower(tags[tag]); value != "" && value != "r" && value != "s" {
			check.Issues = append(check.Issues, tag+" must be r or s")
		}
	}
	if pct := tags["pct"]; pct != "" {
		value, parseErr := strconv.Atoi(pct)
		if parseErr != nil || value < 0 || value > 100 {
			check.Issues = append(check.Issues, "pct must be an integer between 0 and 100")
		}
	}
	if len(check.Issues) > 0 {
		check.Status = "warning"
	} else {
		check.Status = "pass"
	}
	return check, nil
}

func parseTags(value string) map[string]string {
	tags := make(map[string]string)
	for _, entry := range strings.Split(value, ";") {
		key, val, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key != "" {
			tags[key] = strings.TrimSpace(val)
		}
	}
	return tags
}

func canonicalTags(value string) string {
	tags := parseTags(value)
	keys := []string{"v", "k", "p", "h", "t", "s", "n", "g"}
	parts := make([]string, 0, len(tags))
	for _, key := range keys {
		if val, ok := tags[key]; ok {
			parts = append(parts, key+"="+val)
		}
	}
	return strings.Join(parts, ";")
}
