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
	"strings"
	"time"
)

func (c *Client) Send(
	ctx context.Context,
	sender string,
	recipient string,
	rawMessage []byte,
) error {
	deliveryCtx, cancel := context.WithTimeout(ctx, c.config.DeliveryTimeout)
	defer cancel()

	// An explicitly configured relay is trusted deployment configuration. It
	// may intentionally be private (for example, a local Mailpit instance).
	if c.config.RelayHost != "" {
		return c.sendToHost(
			deliveryCtx,
			c.config.RelayHost,
			net.JoinHostPort(c.config.RelayHost, fmt.Sprintf("%d", c.config.RelayPort)),
			sender,
			recipient,
			rawMessage,
		)
	}

	mxRecords, err := c.resolver.Lookup(deliveryCtx, recipient)
	if err != nil {
		return classifyError(err)
	}

	var lastErr error
	for _, mx := range mxRecords {
		host := strings.TrimSuffix(mx.Host, ".")
		ips, resolveErr := c.resolver.LookupPublicIPs(deliveryCtx, host)
		if resolveErr != nil {
			if errors.Is(resolveErr, ErrNoPublicMXAddress) {
				// Skip this MX rather than stopping: the domain may have other
				// publicly routable MX records at a later preference.
				lastErr = &DeliveryError{Message: resolveErr.Error(), Permanent: true}
			} else {
				lastErr = classifyError(resolveErr)
			}
			continue
		}

		// Connect only to the exact addresses that were validated. The
		// hostname is retained separately for SMTP and TLS identity checks.
		for _, ip := range ips {
			dialAddress := net.JoinHostPort(ip.String(), "25")
			err := c.sendToHost(
				deliveryCtx,
				host,
				dialAddress,
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
			if deliveryCtx.Err() != nil {
				return classifyError(deliveryCtx.Err())
			}
		}
	}

	if lastErr == nil {
		return fmt.Errorf("no usable MX servers found")
	}
	return lastErr
}

// sendToHost uses dialAddress for the network connection and tlsServerName for
// protocol identity. For direct MX delivery dialAddress is a pinned IP; for an
// explicitly configured relay it is the configured hostname and port.
func (c *Client) sendToHost(
	ctx context.Context,
	tlsServerName string,
	dialAddress string,
	sender string,
	recipient string,
	rawMessage []byte,
) error {
	conn, err := c.dialer.DialContext(ctx, "tcp", dialAddress)
	if err != nil {
		return classifyError(fmt.Errorf("connect to %s: %w", dialAddress, err))
	}
	defer conn.Close()

	// A cancelled parent context must interrupt a blocked socket operation.
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		return classifyError(err)
	}
	client, err := smtp.NewClient(conn, tlsServerName)
	if err != nil {
		return classifyError(fmt.Errorf("SMTP greeting: %w", err))
	}
	// Close the socket directly on return. Client.Close would issue QUIT and
	// wait for another SMTP response after delivery has already completed.

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		return classifyError(err)
	}
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
			ServerName: tlsServerName,
			MinVersion: tls.VersionTLS12,
		}
		if err := c.setOperationDeadline(ctx, conn); err != nil {
			return classifyError(err)
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			return classifyError(fmt.Errorf("STARTTLS: %w", err))
		}
	}

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		return classifyError(err)
	}
	if err := client.Mail(sender); err != nil {
		return classifyError(fmt.Errorf("MAIL FROM: %w", err))
	}

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		return classifyError(err)
	}
	if err := client.Rcpt(recipient); err != nil {
		return classifyError(fmt.Errorf("RCPT TO: %w", err))
	}

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		return classifyError(err)
	}
	writer, err := client.Data()
	if err != nil {
		return classifyError(fmt.Errorf("DATA: %w", err))
	}

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		_ = writer.Close()
		return classifyError(err)
	}
	if _, err := io.Copy(writer, bytes.NewReader(rawMessage)); err != nil {
		_ = writer.Close()
		return classifyError(fmt.Errorf("write message: %w", err))
	}

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		_ = writer.Close()
		return classifyError(err)
	}
	if err := writer.Close(); err != nil {
		return classifyError(fmt.Errorf("finish DATA: %w", err))
	}
	return nil
}

func (c *Client) setOperationDeadline(ctx context.Context, conn net.Conn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(c.config.OperationTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	return conn.SetDeadline(deadline)
}
