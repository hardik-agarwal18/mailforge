package bouncereceiver

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mailforge/internal/bounce"
	"mailforge/internal/dsn"
)

type recordingProcessor struct {
	mu    sync.Mutex
	calls int
	rcpt  string
	raw   []byte
	err   error
}

func (p *recordingProcessor) ProcessDSN(_ context.Context, envelopeRecipient string, raw []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.rcpt = envelopeRecipient
	p.raw = append([]byte(nil), raw...)
	return p.err
}

func (p *recordingProcessor) snapshot() (int, string, []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, p.rcpt, append([]byte(nil), p.raw...)
}

func startTestReceiver(t *testing.T, processor Processor) (*bounce.Generator, net.Conn, func()) {
	t.Helper()
	generator, err := bounce.NewGenerator("bounces.example.test", []byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{Hostname: "bounce.example.test", CommandTimeout: 2 * time.Second, DataTimeout: 2 * time.Second}, generator, processor)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(ctx, listener)
	}()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		cancel()
		<-serveDone
		t.Fatal(err)
	}
	cleanup := func() {
		_ = conn.Close()
		cancel()
		<-serveDone
	}
	return generator, conn, cleanup
}

func readReply(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	var lines []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SMTP reply: %v", err)
		}
		line = strings.TrimSpace(line)
		lines = append(lines, line)
		if len(line) < 4 || line[3] == ' ' {
			return strings.Join(lines, "\n")
		}
	}
}

func sendCommand(t *testing.T, conn net.Conn, reader *bufio.Reader, command, prefix string) string {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(conn, "%s\r\n", command); err != nil {
		t.Fatalf("write SMTP command %q: %v", command, err)
	}
	reply := readReply(t, reader)
	if !strings.HasPrefix(reply, prefix) {
		t.Fatalf("SMTP reply to %q = %q, want prefix %q", command, reply, prefix)
	}
	return reply
}

func TestReceiverAcceptsOnlyValidSingleRecipientDSN(t *testing.T) {
	processor := &recordingProcessor{}
	generator, conn, cleanup := startTestReceiver(t, processor)
	defer cleanup()
	reader := bufio.NewReader(conn)
	if got := readReply(t, reader); !strings.HasPrefix(got, "220 ") {
		t.Fatalf("greeting = %q", got)
	}
	id := "8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350"
	verp, err := generator.AddressForRecipient(id)
	if err != nil {
		t.Fatal(err)
	}
	sendCommand(t, conn, reader, "EHLO remote.example", "250-")
	sendCommand(t, conn, reader, "MAIL FROM:<>", "250 ")
	sendCommand(t, conn, reader, "RCPT TO:<"+verp+">", "250 ")
	sendCommand(t, conn, reader, "DATA", "354 ")
	body := "From: Mail Delivery Subsystem <mailer-daemon@remote.example>\r\nTo: " + verp + "\r\n\r\nnot parsed by test processor\r\n.\r\n"
	if _, err := conn.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if got := readReply(t, reader); !strings.HasPrefix(got, "250 ") {
		t.Fatalf("DATA completion = %q", got)
	}
	calls, gotRecipient, gotRaw := processor.snapshot()
	if calls != 1 || gotRecipient != verp || !strings.Contains(string(gotRaw), "From: Mail Delivery") {
		t.Fatalf("processor call not captured: calls=%d rcpt=%q raw=%q", calls, gotRecipient, gotRaw)
	}
}

func TestReceiverRejectsNonNullReversePath(t *testing.T) {
	processor := &recordingProcessor{}
	_, conn, cleanup := startTestReceiver(t, processor)
	defer cleanup()
	reader := bufio.NewReader(conn)
	_ = readReply(t, reader)
	sendCommand(t, conn, reader, "EHLO remote.example", "250-")
	sendCommand(t, conn, reader, "MAIL FROM:<>", "250 ")
	sendCommand(t, conn, reader, "RCPT TO:<b+invalid@bounces.example.test>", "550 ")
	sendCommand(t, conn, reader, "MAIL FROM:<attacker@example.net>", "550 ")
	sendCommand(t, conn, reader, "DATA", "503 ")
	if calls, _, _ := processor.snapshot(); calls != 0 {
		t.Fatalf("processor called %d times", calls)
	}
}

func TestReceiverRejectsInvalidVERPAndMultipleRecipients(t *testing.T) {
	processor := &recordingProcessor{}
	generator, conn, cleanup := startTestReceiver(t, processor)
	defer cleanup()
	reader := bufio.NewReader(conn)
	_ = readReply(t, reader)
	id := "8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350"
	valid, _ := generator.AddressForRecipient(id)
	sendCommand(t, conn, reader, "EHLO remote.example", "250-")
	sendCommand(t, conn, reader, "MAIL FROM:<>", "250 ")
	sendCommand(t, conn, reader, "RCPT TO:<b+invalid@bounces.example.test>", "550 ")
	sendCommand(t, conn, reader, "RCPT TO:<"+valid+">", "250 ")
	sendCommand(t, conn, reader, "RCPT TO:<"+valid+">", "452 ")
	if calls, _, _ := processor.snapshot(); calls != 0 {
		t.Fatalf("processor called %d times", calls)
	}
}

func TestReceiverReturns451ForTemporaryProcessorFailure(t *testing.T) {
	processor := &recordingProcessor{err: TemporaryError{Err: fmt.Errorf("database unavailable")}}
	generator, conn, cleanup := startTestReceiver(t, processor)
	defer cleanup()
	reader := bufio.NewReader(conn)
	_ = readReply(t, reader)
	verp, _ := generator.AddressForRecipient("8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350")
	sendCommand(t, conn, reader, "EHLO remote.example", "250-")
	sendCommand(t, conn, reader, "MAIL FROM:<>", "250 ")
	sendCommand(t, conn, reader, "RCPT TO:<"+verp+">", "250 ")
	sendCommand(t, conn, reader, "DATA", "354 ")
	if _, err := conn.Write([]byte("Subject: test\r\n\r\nreport\r\n.\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readReply(t, reader); !strings.HasPrefix(got, "451 ") {
		t.Fatalf("DATA completion = %q, want 451", got)
	}
}

func TestParseSMTPPath(t *testing.T) {
	for _, tc := range []struct {
		input   string
		keyword string
		want    string
		bad     bool
	}{
		{input: "FROM:<>", keyword: "FROM", want: ""},
		{input: "TO:<b+abc@example.test> NOTIFY=FAILURE", keyword: "TO", want: "b+abc@example.test"},
		{input: "TO:b+abc@example.test", keyword: "TO", bad: true},
		{input: "FROM:<user@example.test", keyword: "FROM", bad: true},
	} {
		got, err := parseSMTPPath(tc.input, tc.keyword)
		if tc.bad && err == nil {
			t.Errorf("parseSMTPPath(%q) succeeded, want error", tc.input)
		}
		if !tc.bad && (err != nil || got != tc.want) {
			t.Errorf("parseSMTPPath(%q) = %q, %v; want %q", tc.input, got, err, tc.want)
		}
	}
}

func TestReceiverRejectsOversizedDSN(t *testing.T) {
	processor := &recordingProcessor{}
	generator, conn, cleanup := startTestReceiver(t, processor)
	defer cleanup()
	reader := bufio.NewReader(conn)
	_ = readReply(t, reader)
	verp, _ := generator.AddressForRecipient("8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350")
	sendCommand(t, conn, reader, "EHLO remote.example", "250-")
	sendCommand(t, conn, reader, "MAIL FROM:<>", "250 ")
	sendCommand(t, conn, reader, "RCPT TO:<"+verp+">", "250 ")
	sendCommand(t, conn, reader, "DATA", "354 ")
	var message strings.Builder
	message.WriteString("Subject: too large\r\n\r\n")
	for i := 0; i < 300; i++ {
		message.WriteString(strings.Repeat("x", 900))
		message.WriteString("\r\n")
	}
	message.WriteString(".\r\n")
	if _, err := conn.Write([]byte(message.String())); err != nil {
		t.Fatal(err)
	}
	if got := readReply(t, reader); !strings.HasPrefix(got, "552 ") {
		t.Fatalf("DATA completion = %q, want 552", got)
	}
	if calls, _, _ := processor.snapshot(); calls != 0 {
		t.Fatalf("processor called %d times for oversized DSN", calls)
	}
}

func TestReceiverProcessesRFC3464DSNBeforeAcknowledging(t *testing.T) {
	const report = "From: Mail Delivery Subsystem <mailer-daemon@example.net>\r\n" +
		"Message-ID: <dsn-1@example.net>\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: multipart/report; report-type=delivery-status; boundary=dsn-boundary\r\n" +
		"\r\n" +
		"--dsn-boundary\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n\r\nDelivery failed.\r\n" +
		"--dsn-boundary\r\n" +
		"Content-Type: message/delivery-status\r\n\r\n" +
		"Reporting-MTA: dns; mx.example.net\r\n" +
		"\r\n" +
		"Final-Recipient: rfc822; alice@example.com\r\n" +
		"Action: failed\r\n" +
		"Status: 5.1.1\r\n" +
		"Remote-MTA: dns; mx.example.com\r\n" +
		"Diagnostic-Code: smtp; 550 5.1.1 user unknown\r\n" +
		"\r\n--dsn-boundary--\r\n"

	var processed atomic.Bool
	processor := ProcessFunc(func(_ context.Context, _ string, raw []byte) error {
		parsed, err := dsn.Parse(raw)
		if err != nil {
			return err
		}
		if parsed.Action != "failed" || parsed.Status != "5.1.1" || parsed.FinalRecipient != "alice@example.com" {
			return fmt.Errorf("unexpected parsed DSN: %+v", parsed)
		}
		processed.Store(true)
		return nil
	})
	generator, conn, cleanup := startTestReceiver(t, processor)
	defer cleanup()
	reader := bufio.NewReader(conn)
	_ = readReply(t, reader)
	verp, err := generator.AddressForRecipient("8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350")
	if err != nil {
		t.Fatal(err)
	}
	sendCommand(t, conn, reader, "EHLO remote.example", "250-")
	sendCommand(t, conn, reader, "MAIL FROM:<>", "250 ")
	sendCommand(t, conn, reader, "RCPT TO:<"+verp+">", "250 ")
	sendCommand(t, conn, reader, "DATA", "354 ")
	if _, err := conn.Write([]byte(report + ".\r\n")); err != nil {
		t.Fatal(err)
	}
	if got := readReply(t, reader); !strings.HasPrefix(got, "250 ") {
		t.Fatalf("DATA completion = %q, want 250 after DSN parser succeeds", got)
	}
	if !processed.Load() {
		t.Fatal("DSN processor was not called")
	}
}

func TestNewRequiresTrustedCIDRsForNonLoopbackListener(t *testing.T) {
	generator, err := bounce.NewGenerator("bounces.example.test", []byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Address: "0.0.0.0:2526"}, generator, &recordingProcessor{}); err == nil {
		t.Fatal("New() accepted a non-loopback listener without trusted peer CIDRs")
	}
	if _, err := New(Config{Address: "0.0.0.0:2526", TrustedPeerCIDRs: []string{"not-a-cidr"}}, generator, &recordingProcessor{}); err == nil {
		t.Fatal("New() accepted an invalid trusted peer CIDR")
	}
	if _, err := New(Config{Address: "0.0.0.0:2526", TrustedPeerCIDRs: []string{"192.0.2.10/32"}}, generator, &recordingProcessor{}); err == nil {
		t.Fatal("New() accepted non-loopback listener without mutual TLS")
	}
	mtls := &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: x509.NewCertPool()}
	if _, err := New(Config{Address: "0.0.0.0:2526", TrustedPeerCIDRs: []string{"192.0.2.10/32"}, TLSConfig: mtls}, generator, &recordingProcessor{}); err != nil {
		t.Fatalf("New() rejected trusted peer allowlist with mutual TLS: %v", err)
	}
	if _, err := New(Config{Address: "0.0.0.0:2526", TrustedPeerCIDRs: []string{"0.0.0.0/0"}, TLSConfig: mtls}, generator, &recordingProcessor{}); err == nil {
		t.Fatal("New() accepted an unrestricted /0 trusted peer range")
	}
}

func TestReceiverPeerAllowlistAndPerPeerRateLimit(t *testing.T) {
	generator, err := bounce.NewGenerator("bounces.example.test", []byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{MaxDSNsPerPeerPerMinute: 1}, generator, &recordingProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	if !server.isTrustedPeer(net.ParseIP("127.0.0.1")) {
		t.Fatal("loopback peer should be trusted by default")
	}
	if server.isTrustedPeer(net.ParseIP("192.0.2.10")) {
		t.Fatal("non-loopback peer should not be trusted without an allowlist")
	}
	now := time.Date(2026, time.November, 9, 12, 0, 0, 0, time.UTC)
	if !server.allowDSN("127.0.0.1", now) {
		t.Fatal("first DSN should pass the per-peer rate limit")
	}
	if server.allowDSN("127.0.0.1", now.Add(10*time.Second)) {
		t.Fatal("second DSN in the same window should be rate limited")
	}
	if !server.allowDSN("127.0.0.1", now.Add(time.Minute)) {
		t.Fatal("DSN should be accepted after the rate window resets")
	}
}

func TestServeCannotBypassExternalListenerTrustValidation(t *testing.T) {
	generator, err := bounce.NewGenerator("bounces.example.test", []byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{}, generator, &recordingProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Skipf("cannot bind wildcard test listener: %v", err)
	}
	defer listener.Close()
	if err := server.Serve(context.Background(), listener); err == nil || !strings.Contains(err.Error(), "TrustedPeerCIDRs") {
		t.Fatalf("Serve() error = %v, want external-listener trust error", err)
	}
}

func TestReceiverRequiresSTARTTLSBeforeMAILWhenMutualTLSIsConfigured(t *testing.T) {
	generator, err := bounce.NewGenerator("bounces.example.test", []byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{
		TLSConfig: &tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  x509.NewCertPool(),
		},
	}, generator, &recordingProcessor{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(ctx, listener)
	}()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		cancel()
		<-serveDone
		t.Fatal(err)
	}
	defer func() {
		_ = conn.Close()
		cancel()
		<-serveDone
	}()
	reader := bufio.NewReader(conn)
	if got := readReply(t, reader); !strings.HasPrefix(got, "220 ") {
		t.Fatalf("greeting = %q", got)
	}
	sendCommand(t, conn, reader, "EHLO local.example", "250-")
	sendCommand(t, conn, reader, "MAIL FROM:<>", "530 ")
}
