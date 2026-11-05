package outbound

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"strings"
	"time"
)

type Client struct {
	resolver *MXResolver
	dialer   *net.Dialer
}

func NewClient() *Client {
	return &Client{
		resolver: NewMXResolver(),
		dialer: &net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		},
	}
}

func (c *Client) Send(
	ctx context.Context,
	sender string,
	recipient string,
	rawMessage []byte,
) error {

	mxRecords, err := c.resolver.Lookup(ctx, recipient)
	if err != nil {
		return classifyError(err)
	}

	var lastErr error

	for _, mx := range mxRecords {
		host := strings.TrimSuffix(mx.Host, ".")

		err := c.sendToMX(ctx, host, sender, recipient, rawMessage)
		if err == nil {
			return nil
		}

		lastErr = err

		// A permanent SMTP rejection is not fixed by trying
		// another MX host for the same recipient.
		if deliveryErr, ok := err.(*DeliveryError); ok && deliveryErr.Permanent {
			return deliveryErr
		}
	}

	if lastErr == nil {
		return fmt.Errorf("no usable MX servers found")
	}

	return lastErr
}

func (c *Client) sendToMX(
	ctx context.Context,
	host string,
	sender string,
	recipient string,
	rawMessage []byte,
) error {

	address := net.JoinHostPort(host, "25")

	conn, err := c.dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return classifyError(fmt.Errorf("connect to %s: %w", address, err))
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return classifyError(fmt.Errorf("SMTP greeting from %s: %w", host, err))
	}
	defer client.Close()

	if err := client.Hello("mailforge.local"); err != nil {
		return classifyError(fmt.Errorf("EHLO: %w", err))
	}

	// Opportunistic STARTTLS.
	// If the remote server advertises STARTTLS but negotiation fails,
	// we stop rather than silently downgrading to plaintext.
	if ok, _ := client.Extension("STARTTLS"); ok {
		tlsConfig := &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		}

		if err := client.StartTLS(tlsConfig); err != nil {
			return classifyError(fmt.Errorf("STARTTLS with %s: %w", host, err))
		}
	}

	if err := client.Mail(sender); err != nil {
		return classifyError(fmt.Errorf("MAIL FROM: %w", err))
	}

	if err := client.Rcpt(recipient); err != nil {
		return classifyError(fmt.Errorf("RCPT TO: %w", err))
	}

	writer, err := client.Data()
	if err != nil {
		return classifyError(fmt.Errorf("DATA: %w", err))
	}

	if _, err := io.Copy(writer, bytes.NewReader(rawMessage)); err != nil {
		_ = writer.Close()
		return classifyError(fmt.Errorf("write message data: %w", err))
	}

	if err := writer.Close(); err != nil {
		return classifyError(fmt.Errorf("finish message data: %w", err))
	}

	return nil
}
