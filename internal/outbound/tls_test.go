package outbound

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func trustedTestCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	cert := certServer.TLS.Certificates[0]
	certServer.Close()
	if cert.Leaf == nil {
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			t.Fatalf("parse test certificate: %v", err)
		}
		cert.Leaf = leaf
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert.Leaf)
	return cert, pool
}

func TestConnectToHostNegotiatesAndRecordsSTARTTLS(t *testing.T) {
	cert, roots := trustedTestCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		if _, writeErr := io.WriteString(conn, "220 mx.example.com ESMTP ready\r\n"); writeErr != nil {
			serverErr <- writeErr
			return
		}
		if _, readErr := reader.ReadString('\n'); readErr != nil { // cleartext EHLO
			serverErr <- readErr
			return
		}
		if _, writeErr := io.WriteString(conn, "250-mx.example.com\r\n250-STARTTLS\r\n250 SIZE 1048576\r\n"); writeErr != nil {
			serverErr <- writeErr
			return
		}
		command, readErr := reader.ReadString('\n')
		if readErr != nil {
			serverErr <- readErr
			return
		}
		if !strings.EqualFold(strings.TrimSpace(command), "STARTTLS") {
			serverErr <- &testError{message: "expected STARTTLS, got " + strings.TrimSpace(command)}
			return
		}
		if _, writeErr := io.WriteString(conn, "220 2.0.0 begin TLS\r\n"); writeErr != nil {
			serverErr <- writeErr
			return
		}
		tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if handshakeErr := tlsConn.Handshake(); handshakeErr != nil {
			serverErr <- handshakeErr
			return
		}
		tlsReader := bufio.NewReader(tlsConn)
		command, readErr = tlsReader.ReadString('\n') // EHLO must be repeated after TLS
		if readErr != nil {
			serverErr <- readErr
			return
		}
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(command)), "EHLO ") {
			serverErr <- &testError{message: "expected post-TLS EHLO, got " + strings.TrimSpace(command)}
			return
		}
		_, writeErr := io.WriteString(tlsConn, "250-mx.example.com\r\n250 SIZE 1048576\r\n")
		serverErr <- writeErr
	}()

	client := newClient(Config{
		EHLOName:         "sender.example.test",
		OperationTimeout: 2 * time.Second,
		DeliveryTimeout:  3 * time.Second,
		TLSConfig:        &tls.Config{RootCAs: roots},
	}, nil, &targetDialer{target: listener.Addr().String()})
	session, err := client.connectToHost(context.Background(), "example.com", "203.0.113.50:25", TLSModeStartTLS)
	if err != nil {
		t.Fatalf("connectToHost STARTTLS: %v", err)
	}
	defer session.close()

	info := deliveryInfoFromSession(session, "example.com", TLSModeStartTLS)
	if !info.TLSUsed || info.TLSMode != string(TLSModeStartTLS) || info.TLSVersion == "" || info.TLSCipherSuite == "" {
		t.Fatalf("TLS delivery info = %+v, want negotiated TLS details", info)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("fake STARTTLS server: %v", err)
	}
}

func TestConnectToHostSupportsImplicitTLS(t *testing.T) {
	cert, roots := trustedTestCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()
		tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		if err := tlsConn.Handshake(); err != nil {
			serverErr <- err
			return
		}
		if _, err := io.WriteString(tlsConn, "220 relay.example.com ESMTP ready\r\n"); err != nil {
			serverErr <- err
			return
		}
		reader := bufio.NewReader(tlsConn)
		command, err := reader.ReadString('\n')
		if err != nil {
			serverErr <- err
			return
		}
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(command)), "EHLO ") {
			serverErr <- &testError{message: "expected EHLO over implicit TLS"}
			return
		}
		_, err = io.WriteString(tlsConn, "250-relay.example.com\r\n250 SIZE 1048576\r\n")
		serverErr <- err
	}()

	client := newClient(Config{
		EHLOName:         "sender.example.test",
		OperationTimeout: 2 * time.Second,
		DeliveryTimeout:  3 * time.Second,
		TLSConfig:        &tls.Config{RootCAs: roots},
	}, nil, &targetDialer{target: listener.Addr().String()})
	session, err := client.connectToHost(context.Background(), "example.com", "127.0.0.1:465", TLSModeImplicit)
	if err != nil {
		t.Fatalf("connectToHost implicit TLS: %v", err)
	}
	defer session.close()

	info := deliveryInfoFromSession(session, "example.com", TLSModeImplicit)
	if !info.TLSUsed || info.TLSMode != string(TLSModeImplicit) || info.TLSVersion == "" {
		t.Fatalf("implicit TLS info = %+v, want TLS negotiated", info)
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("fake implicit TLS server: %v", err)
	}
}

func TestTLSConfigurationRejectsInsecureVerification(t *testing.T) {
	_, err := NewRelayClient(Config{
		RelayHost:    "smtp.example.com",
		RelayPort:    465,
		RelayTLSMode: TLSModeImplicit,
		TLSConfig:    &tls.Config{InsecureSkipVerify: true},
	})
	if err == nil {
		t.Fatal("NewRelayClient accepted InsecureSkipVerify=true")
	}
}

type testError struct{ message string }

func (e *testError) Error() string { return e.message }
