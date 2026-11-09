package domainauth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"strings"
	"testing"
)

type mapResolver map[string][]string

func (m mapResolver) LookupTXT(_ context.Context, name string) ([]string, error) {
	return m[name], nil
}

func testDKIMTXT(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(x509.MarshalPKCS1PublicKey(&key.PublicKey))
	return "v=DKIM1; k=rsa; p=" + encoded
}

func TestInspectPassesAlignedDNSRecords(t *testing.T) {
	domain := "example.com"
	dkim := testDKIMTXT(t)
	resolver := mapResolver{
		domain:                    {"v=spf1 ip4:192.0.2.1 include:_spf.example.net -all"},
		"s1._domainkey." + domain: {dkim},
		"_dmarc." + domain:        {"v=DMARC1; p=reject; adkim=s; aspf=s; pct=100"},
	}
	got, err := Inspect(context.Background(), resolver, domain, "s1", dkim)
	if err != nil {
		t.Fatal(err)
	}
	if got.SPF.Status != "pass" || got.DKIM.Status != "pass" || got.DMARC.Status != "pass" {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestInspectWarnsOnMultipleSPFAndMonitoringDMARC(t *testing.T) {
	domain := "example.com"
	dkim := testDKIMTXT(t)
	resolver := mapResolver{
		domain:                    {"v=spf1 -all", "v=spf1 ~all"},
		"s1._domainkey." + domain: {dkim},
		"_dmarc." + domain:        {"v=DMARC1; p=none"},
	}
	got, err := Inspect(context.Background(), resolver, domain, "s1", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.SPF.Status != "fail" {
		t.Fatalf("SPF status = %s", got.SPF.Status)
	}
	if got.DMARC.Status != "warning" || !strings.Contains(strings.Join(got.DMARC.Issues, " "), "monitoring-only") {
		t.Fatalf("DMARC = %#v", got.DMARC)
	}
}

func TestInspectDetectsDKIMKeyMismatchAndSPFLookupBudget(t *testing.T) {
	domain := "example.com"
	published := testDKIMTXT(t)
	expected := testDKIMTXT(t)
	resolver := mapResolver{
		domain:                    {"v=spf1 a mx include:a.example.net include:b.example.net include:c.example.net include:d.example.net include:e.example.net include:f.example.net include:g.example.net include:h.example.net include:i.example.net include:j.example.net include:k.example.net -all"},
		"s1._domainkey." + domain: {published},
		"_dmarc." + domain:        {"v=DMARC1; p=quarantine"},
	}
	got, err := Inspect(context.Background(), resolver, domain, "s1", expected)
	if err != nil {
		t.Fatal(err)
	}
	if got.DKIM.Status != "fail" || got.SPF.Status != "warning" {
		t.Fatalf("unexpected result: %#v", got)
	}
}
