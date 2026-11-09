package dkimsign

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// KeysetManifest is the worker's hot-reloadable DKIM configuration. Keep
// retired selectors in Keys while their public DNS records remain published;
// only ActiveSelector is used for new signatures.
type KeysetManifest struct {
	Domain         string        `json:"domain"`
	ActiveSelector string        `json:"active_selector"`
	Keys           []KeysetEntry `json:"keys"`
}

type KeysetEntry struct {
	Selector       string `json:"selector"`
	PrivateKeyFile string `json:"private_key_file,omitempty"`
	State          string `json:"state,omitempty"` // active or retiring
}

type KeyManager struct {
	path string
	mu   sync.Mutex
	last [32]byte
	live atomic.Pointer[Signer]
}

func NewKeyManager(manifestPath string) (*KeyManager, error) {
	manifestPath = strings.TrimSpace(manifestPath)
	if manifestPath == "" {
		return nil, errors.New("DKIM keyset manifest path is required")
	}
	manager := &KeyManager{path: manifestPath}
	if err := manager.Reload(); err != nil {
		return nil, err
	}
	return manager, nil
}

// Reload validates the complete manifest and atomically swaps the live signer.
// A failed reload leaves the last-known-good signer active.
func (m *KeyManager) Reload() error {
	if m == nil {
		return errors.New("DKIM key manager is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.path)
	if err != nil {
		return fmt.Errorf("read DKIM keyset manifest: %w", err)
	}
	digest := sha256.Sum256(data)
	if digest == m.last {
		return nil
	}

	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var manifest KeysetManifest
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("decode DKIM keyset manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("DKIM keyset manifest contains multiple JSON values")
		}
		return fmt.Errorf("DKIM keyset manifest has trailing data: %w", err)
	}
	manifest.Domain = strings.TrimSpace(manifest.Domain)
	manifest.ActiveSelector = strings.TrimSpace(manifest.ActiveSelector)
	if manifest.Domain == "" || manifest.ActiveSelector == "" || len(manifest.Keys) == 0 {
		return errors.New("DKIM keyset requires domain, active_selector, and at least one key")
	}

	seen := make(map[string]struct{}, len(manifest.Keys))
	var active *KeysetEntry
	activeStates := 0
	for i := range manifest.Keys {
		entry := &manifest.Keys[i]
		entry.Selector = strings.TrimSpace(entry.Selector)
		if err := validateSelector(strings.ToLower(entry.Selector)); err != nil {
			return fmt.Errorf("invalid DKIM key selector %q: %w", entry.Selector, err)
		}
		entry.Selector = strings.ToLower(entry.Selector)
		if _, exists := seen[entry.Selector]; exists {
			return fmt.Errorf("duplicate DKIM selector %q in keyset", entry.Selector)
		}
		seen[entry.Selector] = struct{}{}
		if entry.State != "" && entry.State != "active" && entry.State != "retiring" {
			return fmt.Errorf("DKIM selector %q has invalid state %q", entry.Selector, entry.State)
		}
		if entry.State == "active" {
			activeStates++
			if entry.Selector != strings.ToLower(manifest.ActiveSelector) {
				return fmt.Errorf("selector %q is marked active but active_selector is %q", entry.Selector, manifest.ActiveSelector)
			}
		}
		if entry.Selector == strings.ToLower(manifest.ActiveSelector) {
			active = entry
		}
	}
	if active == nil {
		return fmt.Errorf("active DKIM selector %q is not present in keyset", manifest.ActiveSelector)
	}
	if activeStates > 1 {
		return errors.New("DKIM keyset must not mark more than one selector active")
	}
	if strings.TrimSpace(active.PrivateKeyFile) == "" {
		return fmt.Errorf("active DKIM selector %q has no private_key_file", active.Selector)
	}
	if active.State == "retiring" {
		return fmt.Errorf("active DKIM selector %q cannot have state retiring", active.Selector)
	}

	keyPath := active.PrivateKeyFile
	if !filepath.IsAbs(keyPath) {
		keyPath = filepath.Join(filepath.Dir(m.path), keyPath)
	}
	key, err := LoadRSAPrivateKey(keyPath)
	if err != nil {
		return fmt.Errorf("load active DKIM selector %q: %w", active.Selector, err)
	}
	signer, err := NewSigner(manifest.Domain, active.Selector, key)
	if err != nil {
		return fmt.Errorf("configure active DKIM selector: %w", err)
	}

	m.live.Store(signer)
	m.last = digest
	return nil
}

func (m *KeyManager) Sign(raw []byte) ([]byte, error) {
	if m == nil {
		return nil, errors.New("DKIM key manager is nil")
	}
	signer := m.live.Load()
	if signer == nil {
		return nil, errors.New("DKIM signer is not loaded")
	}
	return signer.Sign(raw)
}

func (m *KeyManager) Domain() string {
	if m == nil || m.live.Load() == nil {
		return ""
	}
	return m.live.Load().Domain()
}

func (m *KeyManager) Selector() string {
	if m == nil || m.live.Load() == nil {
		return ""
	}
	return m.live.Load().Selector()
}

func (m *KeyManager) DNSRecord() (string, error) {
	if m == nil || m.live.Load() == nil {
		return "", errors.New("DKIM signer is not loaded")
	}
	return m.live.Load().DNSRecord()
}

// Watch polls the manifest and atomically activates a new selector when the
// file changes. Invalid edits are logged and do not interrupt current signing.
func (m *KeyManager) Watch(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.Reload(); err != nil {
				log.Printf("DKIM keyset reload rejected; retaining selector=%s: %v", m.Selector(), err)
			}
		}
	}
}

// WriteKeyset writes a compact manifest. Reload validates it before activation;
// callers should atomically replace the file to avoid observing partial writes.
func WriteKeyset(path string, manifest KeysetManifest) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("DKIM keyset path is required")
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal DKIM keyset: %w", err)
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create DKIM keyset directory: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write DKIM keyset temporary file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("activate DKIM keyset manifest: %w", err)
	}
	return nil
}

// ManifestDigest is useful in operational diagnostics without exposing paths
// or private key material.
func (m *KeyManager) ManifestDigest() string {
	if m == nil {
		return ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return hex.EncodeToString(m.last[:])
}
