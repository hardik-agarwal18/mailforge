package smtp

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"mailforge/internal/message"
)

type testAcceptor struct {
	mu    sync.Mutex
	calls int
	input message.NewMessage
}

func (a *testAcceptor) Accept(_ context.Context, input message.NewMessage) (message.StoredMessage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.input = input
	return message.StoredMessage{ID: "stored-id", MessageID: "<test@mailforge.local>"}, nil
}

type testAuthenticator struct{}

func (testAuthenticator) Authenticate(_ context.Context, username, password string) (Identity, error) {
	if username != "hardik" || password != "test-password" {
		return Identity{}, ErrInvalidCredentials
	}
	return Identity{
		Username:          "hardik",
		Email:             "sender@example.com",
		AuthorizedDomains: []string{"verified.example.org"},
	}, nil
}

type testMetrics struct {
	mu          sync.Mutex
	opened      int
	closed      int
	submissions int
	accepted    []bool
}

func (m *testMetrics) RecordConnectionOpened() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.opened++
}

func (m *testMetrics) RecordConnectionClosed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed++
}

func (m *testMetrics) RecordAuthentication(_ bool) {}

func (m *testMetrics) RecordSubmission(_ time.Duration, accepted bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submissions++
	m.accepted = append(m.accepted, accepted)
}

func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}

	now := time.Now()
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create test certificate: %v", err)
	}

	certificate := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  privateKey,
	}

	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}
}

func readReply(t *testing.T, reader *bufio.Reader) []string {
	t.Helper()
	var lines []string
	for {
		line, err := readSMTPLine(reader, maxLineSize)
		if err != nil {
			t.Fatalf("read SMTP reply: %v", err)
		}
		lines = append(lines, line)
		if len(line) < 4 || line[3] != '-' {
			return lines
		}
	}
}

func writeCommand(t *testing.T, conn net.Conn, command string) {
	t.Helper()
	if _, err := conn.Write([]byte(command + "\r\n")); err != nil {
		t.Fatalf("write SMTP command %q: %v", command, err)
	}
}

func assertCode(t *testing.T, lines []string, code string) {
	t.Helper()
	if len(lines) == 0 || !strings.HasPrefix(lines[len(lines)-1], code) {
		t.Fatalf("SMTP response = %v, want code %s", lines, code)
	}
}

func TestSubmissionRequiresTLSAndAuthenticationAndBindsSender(t *testing.T) {
	acceptor := &testAcceptor{}
	metrics := &testMetrics{}
	server, err := NewSubmissionServer("127.0.0.1:0", acceptor, metrics, testTLSConfig(t), testAuthenticator{})
	if err != nil {
		t.Fatalf("NewSubmissionServer() error = %v", err)
	}

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		server.handleConnection(serverConn)
		close(done)
	}()
	defer clientConn.Close()

	reader := bufio.NewReader(clientConn)
	assertCode(t, readReply(t, reader), "220")

	writeCommand(t, clientConn, "EHLO client.example")
	capabilities := strings.Join(readReply(t, reader), "\n")
	if !strings.Contains(capabilities, "STARTTLS") {
		t.Fatal("STARTTLS was not advertised before TLS")
	}
	if strings.Contains(capabilities, "AUTH") {
		t.Fatal("AUTH must not be advertised before TLS")
	}

	plainAuth := base64.StdEncoding.EncodeToString([]byte("\x00hardik\x00test-password"))
	writeCommand(t, clientConn, "AUTH PLAIN "+plainAuth)
	assertCode(t, readReply(t, reader), "538")

	writeCommand(t, clientConn, "MAIL FROM:<sender@example.com>")
	assertCode(t, readReply(t, reader), "530")

	writeCommand(t, clientConn, "STARTTLS")
	assertCode(t, readReply(t, reader), "220")

	tlsClient := tls.Client(clientConn, &tls.Config{
		InsecureSkipVerify: true, // Test certificate is self-signed.
		MinVersion:         tls.VersionTLS12,
	})
	handshakeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tlsClient.HandshakeContext(handshakeCtx); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}
	reader = bufio.NewReader(tlsClient)

	writeCommand(t, tlsClient, "EHLO client.example")
	capabilities = strings.Join(readReply(t, reader), "\n")
	if !strings.Contains(capabilities, "AUTH PLAIN LOGIN") {
		t.Fatal("AUTH mechanisms were not advertised after TLS")
	}
	if strings.Contains(capabilities, "STARTTLS") {
		t.Fatal("STARTTLS must not be advertised after TLS is active")
	}

	writeCommand(t, tlsClient, "MAIL FROM:<sender@example.com>")
	assertCode(t, readReply(t, reader), "530")

	writeCommand(t, tlsClient, "AUTH PLAIN "+plainAuth)
	assertCode(t, readReply(t, reader), "235")

	writeCommand(t, tlsClient, "MAIL FROM:<sender@@example.com>")
	assertCode(t, readReply(t, reader), "553")

	writeCommand(t, tlsClient, "MAIL FROM:<attacker@example.com>")
	assertCode(t, readReply(t, reader), "553")
	writeCommand(t, tlsClient, "MAIL FROM:<attacker@unverified.example.org>")
	assertCode(t, readReply(t, reader), "553")

	// A DNS-verified sending domain permits alternate mailbox local-parts.
	writeCommand(t, tlsClient, "MAIL FROM:<billing@verified.example.org>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, tlsClient, "RSET")
	assertCode(t, readReply(t, reader), "250")

	// Domain names are case-insensitive, but SMTP local-parts are not.
	writeCommand(t, tlsClient, "MAIL FROM:<sender@EXAMPLE.COM>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, tlsClient, "RSET")
	assertCode(t, readReply(t, reader), "250")

	writeCommand(t, tlsClient, "MAIL FROM:<Sender@example.com>")
	assertCode(t, readReply(t, reader), "553")

	writeCommand(t, tlsClient, "MAIL FROM:<>")
	assertCode(t, readReply(t, reader), "553")

	writeCommand(t, tlsClient, "MAIL FROM:<sender@example.com>")
	assertCode(t, readReply(t, reader), "250")

	writeCommand(t, tlsClient, "RCPT TO:<recipient@@example.net>")
	assertCode(t, readReply(t, reader), "553")
	writeCommand(t, tlsClient, "RCPT TO:<recipient@>")
	assertCode(t, readReply(t, reader), "553")

	writeCommand(t, tlsClient, "RCPT TO:<recipient@example.net>")
	assertCode(t, readReply(t, reader), "250")

	writeCommand(t, tlsClient, "DATA")
	assertCode(t, readReply(t, reader), "354")
	if _, err := tlsClient.Write([]byte("From: sender@example.com\r\n" +
		"To: visible@example.net\r\n" +
		"Cc: copy@example.org\r\n" +
		"Bcc: blind@example.com\r\n" +
		"Reply-To: replies@example.net\r\n" +
		"Subject: Submission test\r\n\r\nHello\r\n.\r\n")); err != nil {
		t.Fatalf("write DATA: %v", err)
	}
	assertCode(t, readReply(t, reader), "250")

	writeCommand(t, tlsClient, "QUIT")
	assertCode(t, readReply(t, reader), "221")
	_ = tlsClient.Close()
	<-done

	if acceptor.calls != 1 {
		t.Fatalf("message acceptor calls = %d, want 1", acceptor.calls)
	}
	if acceptor.input.Sender != "sender@example.com" {
		t.Fatalf("stored envelope sender = %q", acceptor.input.Sender)
	}
	if acceptor.input.AuthenticatedEmail != "sender@example.com" {
		t.Fatalf("authenticated submission identity = %q, want sender@example.com", acceptor.input.AuthenticatedEmail)
	}
	if len(acceptor.input.AuthorizedDomains) != 1 || acceptor.input.AuthorizedDomains[0] != "verified.example.org" {
		t.Fatalf("authorized domains = %v, want [verified.example.org]", acceptor.input.AuthorizedDomains)
	}
	if len(acceptor.input.Recipients) != 1 || acceptor.input.Recipients[0] != "recipient@example.net" {
		t.Fatalf("stored recipients = %v", acceptor.input.Recipients)
	}
	if !strings.Contains(string(acceptor.input.RawMessage), "Bcc: blind@example.com") {
		t.Fatal("SMTP acceptance should preserve the original Bcc header for internal parsing/storage")
	}
	if metrics.opened != 1 || metrics.closed != 1 {
		t.Fatalf("connection metrics opened=%d closed=%d", metrics.opened, metrics.closed)
	}
	if metrics.submissions != 1 || len(metrics.accepted) != 1 || !metrics.accepted[0] {
		t.Fatalf("submission metrics = %+v", metrics)
	}
}

func TestUnauthenticatedListenerMustBindToLoopback(t *testing.T) {
	tests := []struct {
		name    string
		address string
		wantErr bool
	}{
		{name: "IPv4 loopback", address: "127.0.0.1:2525"},
		{name: "IPv6 loopback", address: "[::1]:2525"},
		{name: "ephemeral IPv4 loopback", address: "127.0.0.1:0"},
		{name: "all interfaces", address: ":2525", wantErr: true},
		{name: "IPv4 wildcard", address: "0.0.0.0:2525", wantErr: true},
		{name: "IPv6 wildcard", address: "[::]:2525", wantErr: true},
		{name: "public IP", address: "192.0.2.10:2525", wantErr: true},
		{name: "hostname", address: "localhost:2525", wantErr: true},
		{name: "missing port", address: "127.0.0.1", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateUnauthenticatedListenAddress(test.address)
			if test.wantErr && err == nil {
				t.Fatalf("validateUnauthenticatedListenAddress(%q) unexpectedly succeeded", test.address)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validateUnauthenticatedListenAddress(%q) error = %v", test.address, err)
			}
		})
	}
}

func TestStartContextRefusesPublicUnauthenticatedListener(t *testing.T) {
	server := NewServer("0.0.0.0:0", &testAcceptor{}, nil)
	if err := server.StartContext(context.Background()); err == nil {
		t.Fatal("StartContext() unexpectedly started an unauthenticated public listener")
	}
}

func TestNewSubmissionServerRequiresTLSAndAuthenticator(t *testing.T) {
	acceptor := &testAcceptor{}
	if _, err := NewSubmissionServer(":587", acceptor, nil, nil, testAuthenticator{}); err == nil {
		t.Fatal("expected missing TLS config to fail")
	}
	if _, err := NewSubmissionServer(":587", acceptor, nil, testTLSConfig(t), nil); err == nil {
		t.Fatal("expected missing authenticator to fail")
	}
}

func TestReadAuthCredentialsLogin(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	type result struct {
		username string
		password string
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		username, password, err := readAuthCredentials(
			serverConn,
			bufio.NewReader(serverConn),
			"LOGIN",
		)
		resultCh <- result{username: username, password: password, err: err}
	}()

	reader := bufio.NewReader(clientConn)
	assertCode(t, readReply(t, reader), "334")
	writeCommand(t, clientConn, base64.StdEncoding.EncodeToString([]byte("hardik")))
	assertCode(t, readReply(t, reader), "334")
	writeCommand(t, clientConn, base64.StdEncoding.EncodeToString([]byte("test-password")))

	got := <-resultCh
	if got.err != nil {
		t.Fatalf("readAuthCredentials() error = %v", got.err)
	}
	if got.username != "hardik" || got.password != "test-password" {
		t.Fatalf("credentials = (%q, %q)", got.username, got.password)
	}
}

func TestHELOAllowsMailTransaction(t *testing.T) {
	acceptor := &testAcceptor{}
	server := NewServer("127.0.0.1:0", acceptor, nil)
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		server.handleConnection(serverConn)
		close(done)
	}()
	defer clientConn.Close()

	reader := bufio.NewReader(clientConn)
	assertCode(t, readReply(t, reader), "220")
	writeCommand(t, clientConn, "HELO legacy-client.example")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "MAIL FROM:<sender@example.com>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "RCPT TO:<recipient@example.net>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "DATA")
	assertCode(t, readReply(t, reader), "354")
	if _, err := clientConn.Write([]byte("Subject: HELO test\r\n\r\nhello\r\n.\r\n")); err != nil {
		t.Fatalf("write DATA: %v", err)
	}
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "QUIT")
	assertCode(t, readReply(t, reader), "221")
	_ = clientConn.Close()
	<-done

	if acceptor.calls != 1 {
		t.Fatalf("message acceptor calls = %d, want 1", acceptor.calls)
	}
}

func TestRecipientLimitIsEnforced(t *testing.T) {
	acceptor := &testAcceptor{}
	server := &Server{
		messageAcceptor: acceptor,
		limits:          normalizeServerLimits(ServerLimits{MaxRecipients: 1}),
	}
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		server.handleConnection(serverConn)
		close(done)
	}()
	defer clientConn.Close()

	reader := bufio.NewReader(clientConn)
	assertCode(t, readReply(t, reader), "220")
	writeCommand(t, clientConn, "HELO legacy-client.example")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "MAIL FROM:<sender@example.com>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "RCPT TO:<first@example.net>")
	assertCode(t, readReply(t, reader), "250")
	// A duplicate is accepted but does not consume another recipient slot.
	writeCommand(t, clientConn, "RCPT TO:<first@EXAMPLE.NET>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "RCPT TO:<second@example.net>")
	assertCode(t, readReply(t, reader), "452")
	writeCommand(t, clientConn, "QUIT")
	assertCode(t, readReply(t, reader), "221")
	_ = clientConn.Close()
	<-done
}

func TestSMTPTransactionDeliversEachUniqueRecipientOnce(t *testing.T) {
	acceptor := &testAcceptor{}
	server := &Server{
		messageAcceptor: acceptor,
		limits:          normalizeServerLimits(ServerLimits{MaxRecipients: 3}),
	}
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		server.handleConnection(serverConn)
		close(done)
	}()
	defer clientConn.Close()

	reader := bufio.NewReader(clientConn)
	assertCode(t, readReply(t, reader), "220")
	writeCommand(t, clientConn, "EHLO dedupe-client.example")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "MAIL FROM:<sender@example.com>")
	assertCode(t, readReply(t, reader), "250")

	writeCommand(t, clientConn, "RCPT TO:<Alice@example.net>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "RCPT TO:<Alice@EXAMPLE.NET>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "RCPT TO:<alice@example.net>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "RCPT TO:<bob@example.net>")
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "RCPT TO:<carol@example.net>")
	assertCode(t, readReply(t, reader), "452")

	writeCommand(t, clientConn, "DATA")
	assertCode(t, readReply(t, reader), "354")
	if _, err := clientConn.Write([]byte("Subject: duplicate recipient test\r\n\r\nhello\r\n.\r\n")); err != nil {
		t.Fatalf("write DATA: %v", err)
	}
	assertCode(t, readReply(t, reader), "250")
	writeCommand(t, clientConn, "QUIT")
	assertCode(t, readReply(t, reader), "221")
	_ = clientConn.Close()
	<-done

	want := []string{"Alice@example.net", "alice@example.net", "bob@example.net"}
	if acceptor.calls != 1 {
		t.Fatalf("message acceptor calls = %d, want 1", acceptor.calls)
	}
	if len(acceptor.input.Recipients) != len(want) {
		t.Fatalf("accepted recipients = %v, want %v", acceptor.input.Recipients, want)
	}
	for i := range want {
		if acceptor.input.Recipients[i] != want[i] {
			t.Fatalf("accepted recipients = %v, want %v", acceptor.input.Recipients, want)
		}
	}
}
