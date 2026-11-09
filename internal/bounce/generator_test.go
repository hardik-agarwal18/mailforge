package bounce

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func testGenerator(t *testing.T) *Generator {
	t.Helper()
	g, err := NewGenerator("Bounces.Example.com", []byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestGeneratorRoundTrip(t *testing.T) {
	g := testGenerator(t)
	id := uuid.NewString()
	address, err := g.AddressForRecipient(id)
	if err != nil {
		t.Fatal(err)
	}
	local, domain, _ := strings.Cut(address, "@")
	got, err := g.RecipientID(local + "@" + strings.ToUpper(domain))
	if err != nil {
		t.Fatal(err)
	}
	if got != id {
		t.Fatalf("RecipientID() = %s, want %s", got, id)
	}
}

func TestGeneratorRejectsTamperingAndWrongDomain(t *testing.T) {
	g := testGenerator(t)
	address, err := g.AddressForRecipient(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	local, domain, _ := strings.Cut(address, "@")
	mutated := local[:len(local)-1] + "A@" + domain
	if _, err := g.RecipientID(mutated); err == nil {
		t.Fatal("tampered token accepted")
	}
	if _, err := g.RecipientID(strings.Replace(address, domain, "other.example.com", 1)); err == nil {
		t.Fatal("wrong domain accepted")
	}
}

func TestGeneratorRejectsWeakSecret(t *testing.T) {
	if _, err := NewGenerator("example.com", []byte("short")); err == nil {
		t.Fatal("weak secret accepted")
	}
}
