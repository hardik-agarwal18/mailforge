package outbound

import (
	"context"
	"net"
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
