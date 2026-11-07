package outbound

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestSMTPResponseClassificationByTransactionStage(t *testing.T) {
	tests := []struct {
		name          string
		stage         string
		mailReply     string
		rcptReply     string
		dataReply     string
		finalReply    string
		wantCode      int
		wantEnhanced  string
		wantPermanent bool
	}{
		{
			name:          "temporary MAIL FROM rejection",
			stage:         "mail_from",
			mailReply:     "451 4.7.0 temporarily rate limited\r\n",
			wantCode:      451,
			wantEnhanced:  "4.7.0",
			wantPermanent: false,
		},
		{
			name:          "permanent RCPT TO rejection",
			stage:         "rcpt_to",
			rcptReply:     "550 5.1.1 user unknown\r\n",
			wantCode:      550,
			wantEnhanced:  "5.1.1",
			wantPermanent: true,
		},
		{
			name:          "temporary DATA command rejection",
			stage:         "data",
			dataReply:     "452 4.3.1 insufficient system storage\r\n",
			wantCode:      452,
			wantEnhanced:  "4.3.1",
			wantPermanent: false,
		},
		{
			name:          "temporary final DATA rejection",
			stage:         "data_final",
			finalReply:    "451 4.3.0 temporary queue failure\r\n",
			wantCode:      451,
			wantEnhanced:  "4.3.0",
			wantPermanent: false,
		},
		{
			name:          "permanent final DATA rejection",
			stage:         "data_final",
			finalReply:    "554 5.7.1 message rejected by policy\r\n",
			wantCode:      554,
			wantEnhanced:  "5.7.1",
			wantPermanent: true,
		},
		{
			name:          "unknown 4xx code is temporary",
			stage:         "rcpt_to",
			rcptReply:     "499 4.0.0 vendor temporary response\r\n",
			wantCode:      499,
			wantEnhanced:  "4.0.0",
			wantPermanent: false,
		},
		{
			name:          "unknown 5xx code is permanent",
			stage:         "rcpt_to",
			rcptReply:     "599 5.0.0 vendor permanent response\r\n",
			wantCode:      599,
			wantEnhanced:  "5.0.0",
			wantPermanent: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startScriptedSMTPServer(t, test.mailReply, test.rcptReply, test.dataReply, test.finalReply)
			client, err := NewRelayClient(Config{
				RelayHost:        "127.0.0.1",
				RelayPort:        server.port,
				EHLOName:         "sender.example.test",
				OperationTimeout: time.Second,
				DeliveryTimeout:  3 * time.Second,
			})
			if err != nil {
				t.Fatalf("NewRelayClient: %v", err)
			}

			err = client.Send(
				context.Background(),
				"sender@example.test",
				"recipient@example.net",
				[]byte("From: sender@example.test\r\nTo: recipient@example.net\r\nSubject: status test\r\n\r\nhello\r\n"),
			)
			if err == nil {
				t.Fatal("Send() succeeded, want SMTP rejection")
			}

			var deliveryErr *DeliveryError
			if !errors.As(err, &deliveryErr) {
				t.Fatalf("Send() error type = %T (%v), want *DeliveryError", err, err)
			}
			if deliveryErr.SMTPCode != test.wantCode {
				t.Errorf("SMTPCode = %d, want %d", deliveryErr.SMTPCode, test.wantCode)
			}
			if deliveryErr.EnhancedStatusCode != test.wantEnhanced {
				t.Errorf("EnhancedStatusCode = %q, want %q", deliveryErr.EnhancedStatusCode, test.wantEnhanced)
			}
			if deliveryErr.Stage != test.stage {
				t.Errorf("Stage = %q, want %q", deliveryErr.Stage, test.stage)
			}
			if deliveryErr.Permanent != test.wantPermanent {
				t.Errorf("Permanent = %t, want %t", deliveryErr.Permanent, test.wantPermanent)
			}
		})
	}
}

type scriptedSMTPServer struct {
	port int
}

func startScriptedSMTPServer(t *testing.T, mailReply, rcptReply, dataReply, finalReply string) scriptedSMTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	var port int
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatalf("parse listener port: %v", err)
	}

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		write := func(value string) error {
			_, writeErr := io.WriteString(conn, value)
			return writeErr
		}
		readCommand := func() (string, error) {
			line, readErr := reader.ReadString('\n')
			return strings.TrimSpace(line), readErr
		}
		if write("220 mx.example.test ESMTP ready\r\n") != nil {
			return
		}
		command, readErr := readCommand()
		if readErr != nil || !strings.HasPrefix(strings.ToUpper(command), "EHLO ") {
			return
		}
		if write("250 mx.example.test\r\n") != nil {
			return
		}

		command, readErr = readCommand()
		if readErr != nil || !strings.HasPrefix(strings.ToUpper(command), "MAIL FROM:") {
			return
		}
		if mailReply == "" {
			mailReply = "250 2.1.0 sender accepted\r\n"
		}
		if write(mailReply) != nil || strings.HasPrefix(mailReply, "4") || strings.HasPrefix(mailReply, "5") {
			return
		}

		command, readErr = readCommand()
		if readErr != nil || !strings.HasPrefix(strings.ToUpper(command), "RCPT TO:") {
			return
		}
		if rcptReply == "" {
			rcptReply = "250 2.1.5 recipient accepted\r\n"
		}
		if write(rcptReply) != nil || strings.HasPrefix(rcptReply, "4") || strings.HasPrefix(rcptReply, "5") {
			return
		}

		command, readErr = readCommand()
		if readErr != nil || strings.ToUpper(command) != "DATA" {
			return
		}
		if dataReply == "" {
			dataReply = "354 end with <CRLF>.<CRLF>\r\n"
		}
		if write(dataReply) != nil || !strings.HasPrefix(dataReply, "354") {
			return
		}

		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil {
				return
			}
			if line == ".\r\n" {
				break
			}
		}
		if finalReply == "" {
			finalReply = "250 2.0.0 message accepted\r\n"
		}
		_ = write(finalReply)
	}()

	return scriptedSMTPServer{port: port}
}
