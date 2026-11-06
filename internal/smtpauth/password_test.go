package smtpauth

import (
	"fmt"
	"testing"
)

func TestHashPasswordAndVerify(t *testing.T) {
	password := "correct-horse-battery-staple"

	hash, err := hashPassword(password)
	if err != nil {
		t.Fatalf("hashPassword() error = %v", err)
	}

	if hash == password {
		t.Fatal("password was stored in plaintext")
	}
	if !verifyPassword(password, hash) {
		t.Fatal("verifyPassword() rejected the correct password")
	}
	if verifyPassword("incorrect-password", hash) {
		t.Fatal("verifyPassword() accepted an incorrect password")
	}
}

func TestHashPasswordRejectsWeakOrOversizedPasswords(t *testing.T) {
	if _, err := hashPassword("short"); err == nil {
		t.Fatal("expected short password to be rejected")
	}
	if _, err := hashPassword(string(make([]byte, maxPasswordBytes+1))); err == nil {
		t.Fatal("expected oversized password to be rejected")
	}
}

func TestPBKDF2SHA256KnownVector(t *testing.T) {
	got := pbkdf2SHA256([]byte("password"), []byte("salt"), 1, 32)
	const want = "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
	if fmt.Sprintf("%x", got) != want {
		t.Fatalf("PBKDF2 vector = %x, want %s", got, want)
	}
}
