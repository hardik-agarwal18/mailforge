package outbound

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type staticResolver struct {
	mxRecords  []*net.MX
	ips        map[string][]net.IP
	hostErrors map[string]error
	err        error
}

func (r staticResolver) Lookup(context.Context, string) ([]*net.MX, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.mxRecords, nil
}

func (r staticResolver) LookupPublicIPs(_ context.Context, host string) ([]net.IP, error) {
	if r.err != nil {
		return nil, r.err
	}
	if err := r.hostErrors[host]; err != nil {
		return nil, err
	}
	ips, ok := r.ips[host]
	if !ok {
		return nil, fmt.Errorf("no test IP for MX host %s", host)
	}
	return ips, nil
}

type targetDialer struct {
	mu      sync.Mutex
	target  string
	failFor map[string]error
	calls   []string
}

func (d *targetDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	d.calls = append(d.calls, address)
	failure := d.failFor[address]
	target := d.target
	d.mu.Unlock()
	if failure != nil {
		return nil, failure
	}
	return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, target)
}

func (d *targetDialer) recordedCalls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

func TestSendDirectMXConnectsAndDelivers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverResult := make(chan string, 1)
	serverErr := make(chan error, 1)
	go serveSMTPTransaction(listener, serverResult, serverErr)

	resolver := staticResolver{
		mxRecords: []*net.MX{{Host: "mx.example.test.", Pref: 10}},
		ips: map[string][]net.IP{
			"mx.example.test": {net.ParseIP("203.0.113.10")},
		},
	}
	dialer := &targetDialer{target: listener.Addr().String()}
	client := newClient(Config{
		EHLOName:         "outbound.mailforge.test",
		ConnectTimeout:   time.Second,
		OperationTimeout: time.Second,
		DeliveryTimeout:  3 * time.Second,
	}, resolver, dialer)

	message := []byte("From: sender@example.com\r\nTo: recipient@example.net\r\nSubject: direct MX test\r\n\r\nhello\r\n")
	if err := client.Send(context.Background(), "sender@example.com", "recipient@example.net", message); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatalf("SMTP test server: %v", err)
		}
	case got := <-serverResult:
		for _, want := range []string{
			"EHLO outbound.mailforge.test",
			"MAIL FROM:<sender@example.com>",
			"RCPT TO:<recipient@example.net>",
			"Subject: direct MX test",
			"hello",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("SMTP exchange missing %q; got:\n%s", want, got)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SMTP test server")
	}

	calls := dialer.recordedCalls()
	if len(calls) != 1 || calls[0] != "203.0.113.10:25" {
		t.Fatalf("dial calls = %v, want [203.0.113.10:25]", calls)
	}
}

func TestSendDirectMXTriesNextMXAfterConnectionFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverResult := make(chan string, 1)
	serverErr := make(chan error, 1)
	go serveSMTPTransaction(listener, serverResult, serverErr)

	firstAddress := "203.0.113.10:25"
	secondAddress := "203.0.113.11:25"
	resolver := staticResolver{
		mxRecords: []*net.MX{
			{Host: "mx1.example.test.", Pref: 10},
			{Host: "mx2.example.test.", Pref: 20},
		},
		ips: map[string][]net.IP{
			"mx1.example.test": {net.ParseIP("203.0.113.10")},
			"mx2.example.test": {net.ParseIP("203.0.113.11")},
		},
	}
	dialer := &targetDialer{
		target: listener.Addr().String(),
		failFor: map[string]error{
			firstAddress: errors.New("simulated primary MX connection refusal"),
		},
	}
	client := newClient(Config{
		EHLOName:         "outbound.mailforge.test",
		ConnectTimeout:   time.Second,
		OperationTimeout: time.Second,
		DeliveryTimeout:  3 * time.Second,
	}, resolver, dialer)

	if err := client.Send(context.Background(), "sender@example.com", "recipient@example.net", []byte("Subject: failover\r\n\r\nbody\r\n")); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	calls := dialer.recordedCalls()
	if len(calls) != 2 || calls[0] != firstAddress || calls[1] != secondAddress {
		t.Fatalf("dial calls = %v, want primary then secondary MX", calls)
	}
	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatalf("SMTP test server: %v", err)
		}
	case <-serverResult:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fallback SMTP server")
	}
}

func TestSendRejectsNullMXWithoutDialing(t *testing.T) {
	dialer := &targetDialer{target: "127.0.0.1:1"}
	client := newClient(
		Config{EHLOName: "outbound.mailforge.test"},
		staticResolver{err: ErrNullMX},
		dialer,
	)
	err := client.Send(context.Background(), "sender@example.com", "recipient@example.net", []byte("Subject: test\r\n\r\nbody\r\n"))
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || !deliveryErr.Permanent {
		t.Fatalf("Send() error = %v, want permanent Null MX DeliveryError", err)
	}
	if calls := dialer.recordedCalls(); len(calls) != 0 {
		t.Fatalf("dialer called for Null MX domain: %v", calls)
	}
}

func TestSendKeepsFailureTemporaryWhenAnyMXMayRecover(t *testing.T) {
	firstErr := fmt.Errorf("%w: private address", ErrNoPublicMXAddress)
	resolver := staticResolver{
		mxRecords: []*net.MX{
			{Host: "mx-private.example.test.", Pref: 10},
			{Host: "mx-temporary.example.test.", Pref: 20},
		},
		hostErrors: map[string]error{
			"mx-private.example.test": firstErr,
		},
		ips: map[string][]net.IP{
			"mx-temporary.example.test": {net.ParseIP("203.0.113.11")},
		},
	}
	dialer := &targetDialer{failFor: map[string]error{"203.0.113.11:25": errors.New("connection refused")}}
	client := newClient(Config{EHLOName: "outbound.mailforge.test", DeliveryTimeout: time.Second}, resolver, dialer)

	err := client.Send(context.Background(), "sender@example.com", "recipient@example.net", []byte("Subject: test\r\n\r\nbody\r\n"))
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) {
		t.Fatalf("Send() error = %v, want DeliveryError", err)
	}
	if deliveryErr.Permanent {
		t.Fatalf("Send() marked recipient permanently failed despite a temporary MX failure: %v", err)
	}
}

func TestConnectToHostRequiresSTARTTLSWhenConfigured(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, _ = io.WriteString(conn, "220 mx.example.test ESMTP ready\r\n")
		_, _ = reader.ReadString('\n') // EHLO
		_, _ = io.WriteString(conn, "250-mx.example.test\r\n250 SIZE 100000\r\n")
	}()

	client := newClient(Config{
		EHLOName:         "outbound.mailforge.test",
		RequireSTARTTLS:  true,
		ConnectTimeout:   time.Second,
		OperationTimeout: time.Second,
		DeliveryTimeout:  2 * time.Second,
	}, nil, &targetDialer{target: listener.Addr().String()})

	_, err = client.connectToHost(context.Background(), "mx.example.test", "203.0.113.20:25", TLSModeRequiredSTARTTLS)
	if err == nil {
		t.Fatal("connectToHost() succeeded without STARTTLS; want temporary policy error")
	}
	var deliveryErr *DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Permanent {
		t.Fatalf("error = %v, want temporary DeliveryError", err)
	}
	<-serverDone
}

func TestConnectToHostSendsEHLOAndReadsCapabilities(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	commands := make(chan string, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			commands <- "accept error: " + acceptErr.Error()
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, _ = io.WriteString(conn, "220 mx.example.test ESMTP ready\r\n")
		command, _ := reader.ReadString('\n')
		commands <- strings.TrimSpace(command)
		_, _ = io.WriteString(conn, "250-mx.example.test\r\n250-PIPELINING\r\n250 SIZE 10485760\r\n")
		// The test closes the client immediately after receiving the capabilities.
		_, _ = reader.ReadString('\n')
	}()

	client := newClient(Config{EHLOName: "outbound.mailforge.test", OperationTimeout: time.Second}, nil, &targetDialer{target: listener.Addr().String()})
	session, err := client.connectToHost(context.Background(), "mx.example.test", "203.0.113.20:25", TLSModeStartTLS)
	if err != nil {
		t.Fatalf("connectToHost(): %v", err)
	}
	defer session.close()

	if got := <-commands; got != "EHLO outbound.mailforge.test" {
		t.Fatalf("EHLO command = %q", got)
	}
	if ok, _ := session.client.Extension("PIPELINING"); !ok {
		t.Fatal("PIPELINING extension not parsed from EHLO response")
	}
}

func TestValidateEHLOName(t *testing.T) {
	for _, valid := range []string{"mail.example.com", "localhost", "mail-1.example.com", "mail.example.com."} {
		if err := validateEHLOName(valid); err != nil {
			t.Errorf("validateEHLOName(%q): %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "mail example.com", "mail\r\nQUIT", "-mail.example.com", "mail..example.com", "mail_1.example.com", "máil.example.com"} {
		if err := validateEHLOName(invalid); err == nil {
			t.Errorf("validateEHLOName(%q) succeeded; want error", invalid)
		}
	}
}

func TestNewClientWithConfigRejectsInvalidEHLOName(t *testing.T) {
	_, err := NewClientWithConfig(Config{EHLOName: "mail.example.com\r\nMAIL FROM:<x@y.com>"})
	if err == nil {
		t.Fatal("NewClientWithConfig accepted SMTP command injection in EHLO name")
	}
}

func serveSMTPTransaction(listener net.Listener, result chan<- string, serverErr chan<- error) {
	conn, err := listener.Accept()
	if err != nil {
		serverErr <- err
		return
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var exchange strings.Builder
	write := func(value string) bool {
		_, writeErr := io.WriteString(conn, value)
		if writeErr != nil {
			serverErr <- writeErr
			return false
		}
		return true
	}
	if !write("220 mx.example.test ESMTP ready\r\n") {
		return
	}
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			serverErr <- readErr
			return
		}
		exchange.WriteString(line)
		command := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(strings.ToUpper(command), "EHLO "):
			if !write("250-mx.example.test\r\n250 SIZE 10485760\r\n") {
				return
			}
		case strings.HasPrefix(strings.ToUpper(command), "MAIL FROM:"):
			if !write("250 2.1.0 sender ok\r\n") {
				return
			}
		case strings.HasPrefix(strings.ToUpper(command), "RCPT TO:"):
			if !write("250 2.1.5 recipient ok\r\n") {
				return
			}
		case strings.EqualFold(command, "DATA"):
			if !write("354 end with dot\r\n") {
				return
			}
			for {
				dataLine, dataErr := reader.ReadString('\n')
				if dataErr != nil {
					serverErr <- dataErr
					return
				}
				if dataLine == ".\r\n" {
					break
				}
				exchange.WriteString(dataLine)
			}
			if !write("250 2.0.0 queued\r\n") {
				return
			}
			result <- exchange.String()
			return
		default:
			if !write("500 unsupported command\r\n") {
				return
			}
		}
	}
}
