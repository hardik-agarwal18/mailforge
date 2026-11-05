package outbound

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"strconv"
	"strings"
)

func (c *Client) Send(
	ctx context.Context,
	sender string,
	recipient string,
	rawMessage []byte,
) error {

	// Development or configured relay mode.
	if c.config.RelayHost != "" {
		return c.sendToHost(
			ctx,
			c.config.RelayHost,
			c.config.RelayPort,
			sender,
			recipient,
			rawMessage,
		)
	}

	// Direct-to-MX mode.
	mxRecords, err := c.resolver.Lookup(ctx, recipient)
	if err != nil {
		return classifyError(err)
	}

	var lastErr error

	for _, mx := range mxRecords {
		host := strings.TrimSuffix(mx.Host, ".")

		err := c.sendToHost(
			ctx,
			host,
			25,
			sender,
			recipient,
			rawMessage,
		)

		if err == nil {
			return nil
		}

		lastErr = err

		var deliveryErr *DeliveryError
		if errors.As(err, &deliveryErr) && deliveryErr.Permanent {
			return deliveryErr
		}
	}

	if lastErr == nil {
		return fmt.Errorf("no usable MX servers found")
	}

	return lastErr
}

func (c *Client) sendToHost(
	ctx context.Context,
	host string,
	port int,
	sender string,
	recipient string,
	rawMessage []byte,
) error {

	address := net.JoinHostPort(host, strconv.Itoa(port))

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
		return classifyError(fmt.Errorf("SMTP greeting: %w", err))
	}
	defer client.Close()

	if err := client.Hello("mailforge.local"); err != nil {
		return classifyError(fmt.Errorf("EHLO: %w", err))
	}

	hasSTARTTLS, _ := client.Extension("STARTTLS")

	if c.config.RequireSTARTTLS && !hasSTARTTLS {
		return &DeliveryError{
			Message:   "remote server does not support required STARTTLS",
			Permanent: false,
		}
	}

	if hasSTARTTLS {
		tlsConfig := &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		}

		if err := client.StartTLS(tlsConfig); err != nil {
			return classifyError(fmt.Errorf("STARTTLS: %w", err))
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
		return classifyError(fmt.Errorf("write message: %w", err))
	}

	if err := writer.Close(); err != nil {
		return classifyError(fmt.Errorf("finish DATA: %w", err))
	}

	return nil
}
