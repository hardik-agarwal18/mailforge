package deliverybench

import (
	"io"
	"net"
	"net/smtp"
	"testing"
	"time"
)

func TestMockSMTPAcceptsMessageAndSupportsSessionReuse(t *testing.T) {
	server, err := StartMockSMTPServer("127.0.0.1:0", MockSMTPConfig{})
	if err != nil {
		t.Fatalf("start mock SMTP server: %v", err)
	}
	defer server.Close()

	client := dialMockClient(t, server.Addr().String())
	defer client.Close()

	for i := 0; i < 2; i++ {
		if err := sendMockMessage(client, "recipient@example.test"); err != nil {
			t.Fatalf("send message %d over reused session: %v", i+1, err)
		}
	}

	stats := server.Stats()
	if stats.Connections != 1 {
		t.Fatalf("connections = %d, want 1", stats.Connections)
	}
	if stats.AcceptedMessages != 2 {
		t.Fatalf("accepted messages = %d, want 2", stats.AcceptedMessages)
	}
	if stats.AcceptedRecipients != 2 {
		t.Fatalf("accepted recipients = %d, want 2", stats.AcceptedRecipients)
	}
}

func TestMockSMTPRejectsConfiguredRecipient(t *testing.T) {
	server, err := StartMockSMTPServer("127.0.0.1:0", MockSMTPConfig{
		RejectEvery: 1,
		RejectCode:  550,
	})
	if err != nil {
		t.Fatalf("start mock SMTP server: %v", err)
	}
	defer server.Close()

	client := dialMockClient(t, server.Addr().String())
	defer client.Close()
	if err := client.Mail("sender@example.test"); err != nil {
		t.Fatalf("MAIL FROM: %v", err)
	}
	if err := client.Rcpt("recipient@example.test"); err == nil {
		t.Fatal("RCPT TO succeeded; want configured 550 rejection")
	}

	stats := server.Stats()
	if stats.RejectedRecipients != 1 {
		t.Fatalf("rejected recipients = %d, want 1", stats.RejectedRecipients)
	}
	if stats.AcceptedRecipients != 0 {
		t.Fatalf("accepted recipients = %d, want 0", stats.AcceptedRecipients)
	}
}

func TestMockSMTPCanDropAfterDATAAcceptance(t *testing.T) {
	server, err := StartMockSMTPServer("127.0.0.1:0", MockSMTPConfig{
		DropAfterDATAEvery: 1,
	})
	if err != nil {
		t.Fatalf("start mock SMTP server: %v", err)
	}
	defer server.Close()

	client := dialMockClient(t, server.Addr().String())
	defer client.Close()

	if err := client.Mail("sender@example.test"); err != nil {
		t.Fatalf("MAIL FROM: %v", err)
	}
	if err := client.Rcpt("recipient@example.test"); err != nil {
		t.Fatalf("RCPT TO: %v", err)
	}
	writer, err := client.Data()
	if err != nil {
		t.Fatalf("DATA: %v", err)
	}
	if _, err := io.WriteString(writer, "Subject: drop test\r\n\r\nbody\r\n"); err != nil {
		t.Fatalf("write DATA: %v", err)
	}
	if err := writer.Close(); err == nil {
		t.Fatal("DATA close succeeded; want dropped final response")
	}

	stats := server.Stats()
	if stats.AcceptedMessages != 1 || stats.DroppedAfterDATA != 1 {
		t.Fatalf("mock stats = %+v, want one accepted message with dropped final reply", stats)
	}
}

func dialMockClient(t *testing.T, address string) *smtp.Client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatalf("dial mock SMTP: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	client, err := smtp.NewClient(conn, "127.0.0.1")
	if err != nil {
		_ = conn.Close()
		t.Fatalf("create SMTP client: %v", err)
	}
	if err := client.Hello("benchmark-test.mailforge.test"); err != nil {
		_ = client.Close()
		t.Fatalf("EHLO: %v", err)
	}
	return client
}

func sendMockMessage(client *smtp.Client, recipient string) error {
	if err := client.Mail("sender@example.test"); err != nil {
		return err
	}
	if err := client.Rcpt(recipient); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := io.WriteString(writer, "Subject: benchmark test\r\n\r\nhello\r\n"); err != nil {
		return err
	}
	return writer.Close()
}
