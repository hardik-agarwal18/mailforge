package outbound

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestSendTimesOutWhenRelayNeverSendsGreeting(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Simulate an SMTP server that accepts TCP but hangs before greeting.
		buffer := make([]byte, 1)
		_, _ = conn.Read(buffer)
	}()

	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	var port int
	for _, ch := range portText {
		port = port*10 + int(ch-'0')
	}

	client, err := NewRelayClient(Config{
		RelayHost:        "127.0.0.1",
		RelayPort:        port,
		OperationTimeout: 100 * time.Millisecond,
		DeliveryTimeout:  500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRelayClient: %v", err)
	}

	started := time.Now()
	err = client.Send(context.Background(), "sender@example.com", "recipient@example.net", []byte("Subject: test\r\n\r\nhello\r\n"))
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("Send() succeeded; want timeout")
	}
	if elapsed > time.Second {
		t.Fatalf("Send() took %s, want bounded timeout under 1s", elapsed)
	}

	_ = listener.Close()
	<-serverDone
}

func TestSendRejectsInvalidEnvelopeBeforeNetworkAccess(t *testing.T) {
	client := NewClient()
	tests := []struct {
		name      string
		sender    string
		recipient string
	}{
		{name: "invalid sender", sender: "sender@@example.com", recipient: "recipient@example.net"},
		{name: "invalid recipient", sender: "sender@example.com", recipient: "recipient@"},
		{name: "null recipient", sender: "sender@example.com", recipient: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := client.Send(context.Background(), test.sender, test.recipient, nil)
			if err == nil {
				t.Fatal("Send() succeeded for invalid envelope")
			}
			var deliveryErr *DeliveryError
			if !errors.As(err, &deliveryErr) || !deliveryErr.Permanent {
				t.Fatalf("Send() error = %v, want permanent DeliveryError", err)
			}
		})
	}
}

func TestSendStripsBccHeadersBeforeSMTPData(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	var port int
	for _, ch := range portText {
		port = port*10 + int(ch-'0')
	}

	type smtpResult struct {
		data []byte
		err  error
	}
	result := make(chan smtpResult, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			result <- smtpResult{err: acceptErr}
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		writeResponse := func(response string) error {
			_, writeErr := io.WriteString(conn, response)
			return writeErr
		}
		if writeErr := writeResponse("220 relay.test ESMTP\r\n"); writeErr != nil {
			result <- smtpResult{err: writeErr}
			return
		}

		for {
			command, readErr := reader.ReadString('\n')
			if readErr != nil {
				result <- smtpResult{err: readErr}
				return
			}
			upper := strings.ToUpper(strings.TrimSpace(command))
			switch {
			case strings.HasPrefix(upper, "EHLO "), strings.HasPrefix(upper, "HELO "):
				if writeErr := writeResponse("250 relay.test\r\n"); writeErr != nil {
					result <- smtpResult{err: writeErr}
					return
				}
			case strings.HasPrefix(upper, "MAIL FROM:"):
				if writeErr := writeResponse("250 2.1.0 sender ok\r\n"); writeErr != nil {
					result <- smtpResult{err: writeErr}
					return
				}
			case strings.HasPrefix(upper, "RCPT TO:"):
				if writeErr := writeResponse("250 2.1.5 recipient ok\r\n"); writeErr != nil {
					result <- smtpResult{err: writeErr}
					return
				}
			case upper == "DATA":
				if writeErr := writeResponse("354 end with dot\r\n"); writeErr != nil {
					result <- smtpResult{err: writeErr}
					return
				}
				var data bytes.Buffer
				for {
					line, dataErr := reader.ReadString('\n')
					if dataErr != nil {
						result <- smtpResult{err: dataErr}
						return
					}
					if line == ".\r\n" {
						break
					}
					data.WriteString(line)
				}
				writeErr := writeResponse("250 2.0.0 accepted\r\n")
				result <- smtpResult{data: data.Bytes(), err: writeErr}
				return
			default:
				if writeErr := writeResponse("500 unsupported command\r\n"); writeErr != nil {
					result <- smtpResult{err: writeErr}
					return
				}
			}
		}
	}()

	client, err := NewRelayClient(Config{
		RelayHost:        "127.0.0.1",
		RelayPort:        port,
		OperationTimeout: time.Second,
		DeliveryTimeout:  3 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRelayClient: %v", err)
	}

	raw := []byte("From: sender@example.com\r\n" +
		"To: visible@example.net\r\n" +
		"Cc: copy@example.org\r\n" +
		"Bcc: hidden@example.com\r\n" +
		"Reply-To: replies@example.net\r\n" +
		"Subject: privacy test\r\n\r\nbody\r\n")
	if err := client.Send(context.Background(), "sender@example.com", "visible@example.net", raw); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	got := <-result
	if got.err != nil {
		t.Fatalf("test SMTP server: %v", got.err)
	}
	data := string(got.data)
	for _, expected := range []string{"To: visible@example.net", "Cc: copy@example.org", "Reply-To: replies@example.net", "Subject: privacy test", "body"} {
		if !strings.Contains(data, expected) {
			t.Errorf("SMTP DATA missing %q: %q", expected, data)
		}
	}
	for _, secret := range []string{"Bcc:", "hidden@example.com"} {
		if strings.Contains(data, secret) {
			t.Errorf("SMTP DATA exposed %q: %q", secret, data)
		}
	}
}
