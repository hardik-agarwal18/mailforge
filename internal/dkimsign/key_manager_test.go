package dkimsign

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func writeTestKey(t *testing.T, path string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	data := pemEncodeTestKey(key)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func pemEncodeTestKey(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func TestKeyManagerHotReloadsActiveSelector(t *testing.T) {
	dir := t.TempDir()
	writeTestKey(t, filepath.Join(dir, "old.pem"))
	writeTestKey(t, filepath.Join(dir, "new.pem"))
	manifestPath := filepath.Join(dir, "dkim-keyset.json")
	manifest := KeysetManifest{Domain: "example.com", ActiveSelector: "s1", Keys: []KeysetEntry{
		{Selector: "s1", PrivateKeyFile: "old.pem", State: "active"},
		{Selector: "s2", State: "retiring"},
	}}
	writeManifestTest(t, manifestPath, manifest)
	manager, err := NewKeyManager(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if manager.Selector() != "s1" {
		t.Fatalf("selector = %s, want s1", manager.Selector())
	}

	manifest.ActiveSelector = "s2"
	manifest.Keys[0].State = "retiring"
	manifest.Keys[1].State = "active"
	manifest.Keys[1].PrivateKeyFile = "new.pem"
	writeManifestTest(t, manifestPath, manifest)
	if err := manager.Reload(); err != nil {
		t.Fatal(err)
	}
	if manager.Selector() != "s2" {
		t.Fatalf("selector after reload = %s, want s2", manager.Selector())
	}
	if _, err := manager.Sign([]byte("From: sender@example.com\r\nTo: user@example.net\r\n\r\nbody\r\n")); err != nil {
		t.Fatalf("sign with new selector: %v", err)
	}
}

func TestKeyManagerKeepsLastKnownGoodOnInvalidManifest(t *testing.T) {
	dir := t.TempDir()
	writeTestKey(t, filepath.Join(dir, "key.pem"))
	manifestPath := filepath.Join(dir, "dkim-keyset.json")
	writeManifestTest(t, manifestPath, KeysetManifest{Domain: "example.com", ActiveSelector: "s1", Keys: []KeysetEntry{{Selector: "s1", PrivateKeyFile: "key.pem", State: "active"}}})
	manager, err := NewKeyManager(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte(`{"domain":"example.com","active_selector":"missing","keys":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.Reload(); err == nil {
		t.Fatal("invalid manifest reload succeeded")
	}
	if manager.Selector() != "s1" {
		t.Fatalf("last-known-good selector = %s, want s1", manager.Selector())
	}
}

func writeManifestTest(t *testing.T, path string, manifest KeysetManifest) {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
