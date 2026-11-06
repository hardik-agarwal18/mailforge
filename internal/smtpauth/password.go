package smtpauth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const (
	passwordHashIterations = 600_000
	passwordSaltLength     = 16
	passwordKeyLength      = 32
	maxPasswordBytes       = 512
)

// hashPassword stores a versioned PBKDF2-HMAC-SHA256 password hash. A fresh
// cryptographically random salt is generated for every credential.
func hashPassword(password string) (string, error) {
	if len(password) < 12 || len(password) > maxPasswordBytes {
		return "", fmt.Errorf("password must be between 12 and %d bytes", maxPasswordBytes)
	}

	salt := make([]byte, passwordSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}

	derived := pbkdf2SHA256([]byte(password), salt, passwordHashIterations, passwordKeyLength)
	return fmt.Sprintf(
		"$pbkdf2-sha256$%d$%s$%s",
		passwordHashIterations,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(derived),
	), nil
}

func verifyPassword(password, encodedHash string) bool {
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 5 || parts[1] != "pbkdf2-sha256" {
		return false
	}

	iterations, err := strconv.Atoi(parts[2])
	if err != nil || iterations < 100_000 || iterations > 2_000_000 {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil || len(salt) < 16 || len(salt) > 64 {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(want) != passwordKeyLength {
		return false
	}

	got := pbkdf2SHA256([]byte(password), salt, iterations, len(want))
	return subtle.ConstantTimeCompare(got, want) == 1
}

func pbkdf2SHA256(password, salt []byte, iterations, keyLength int) []byte {
	const hashLength = 32
	blocks := (keyLength + hashLength - 1) / hashLength
	derived := make([]byte, 0, blocks*hashLength)
	message := make([]byte, len(salt)+4)
	copy(message, salt)

	for block := 1; block <= blocks; block++ {
		binary.BigEndian.PutUint32(message[len(salt):], uint32(block))

		mac := hmac.New(sha256.New, password)
		_, _ = mac.Write(message)
		u := mac.Sum(nil)
		t := make([]byte, hashLength)
		copy(t, u)

		for iteration := 1; iteration < iterations; iteration++ {
			mac.Reset()
			_, _ = mac.Write(u)
			u = mac.Sum(u[:0])
			for i := range t {
				t[i] ^= u[i]
			}
		}

		derived = append(derived, t...)
	}

	return derived[:keyLength]
}
