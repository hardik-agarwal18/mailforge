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

	"mailforge/internal/emailaddr"
	"mailforge/internal/mimeparser"
)

// smtpSession owns one already-greeted SMTP connection. It is deliberately
// kept separate from Send so connection establishment can be tested and
// reasoned about independently from the SMTP mail transaction.
type smtpSession struct {
	client    *smtp.Client
	conn      net.Conn
	stopClose func() bool
}

func (s *smtpSession) close() {
	if s == nil || s.conn == nil {
		return
	}
	if s.stopClose != nil {
		s.stopClose()
	}
	_ = s.conn.Close()
}

func (c *Client) Send(
	ctx context.Context,
	sender string,
	recipient string,
	rawMessage []byte,
) error {
	if err := emailaddr.ValidateReversePath(sender); err != nil {
		return &DeliveryError{Message: fmt.Sprintf("invalid envelope sender: %v", err), Permanent: true}
	}
	if err := emailaddr.ValidateMailbox(recipient); err != nil {
		return &DeliveryError{Message: fmt.Sprintf("invalid envelope recipient: %v", err), Permanent: true}
	}

	// Bcc addresses are carried in the SMTP envelope (RCPT TO), not exposed
	// in the message data sent to any recipient. Strip both ordinary and
	// resent Bcc header fields from every per-recipient delivery copy.
	deliveryMessage, err := mimeparser.StripBccHeaders(rawMessage)
	if err != nil {
		return &DeliveryError{Message: fmt.Sprintf("invalid message headers: %v", err), Permanent: true}
	}

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
			deliveryMessage,
		)
	}

	mxRecords, err := c.resolver.Lookup(deliveryCtx, recipient)
	if err != nil {
		if errors.Is(err, ErrNullMX) || errors.Is(err, ErrInvalidMXSet) {
			return &DeliveryError{Message: err.Error(), Permanent: true}
		}
		return classifyError(err)
	}

	var lastTemporaryErr error
	var lastPermanentErr error
	for _, mx := range mxRecords {
		host := strings.TrimSuffix(mx.Host, ".")
		ips, resolveErr := c.resolver.LookupPublicIPs(deliveryCtx, host)
		if resolveErr != nil {
			if errors.Is(resolveErr, ErrNoPublicMXAddress) {
				lastPermanentErr = &DeliveryError{Message: resolveErr.Error(), Permanent: true}
			} else {
				classified := classifyError(resolveErr)
				var deliveryErr *DeliveryError
				if errors.As(classified, &deliveryErr) && deliveryErr.Permanent {
					lastPermanentErr = classified
				} else {
					lastTemporaryErr = classified
				}
			}
			// Skip this MX rather than stopping: the domain may have other
			// publicly routable MX records at a later preference.
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
				deliveryMessage,
			)
			if err == nil {
				return nil
			}

			var deliveryErr *DeliveryError
			if errors.As(err, &deliveryErr) && deliveryErr.Permanent {
				// A definitive 5xx SMTP response is a permanent transaction
				// outcome. Do not route the same recipient through another MX.
				return deliveryErr
			}
			lastTemporaryErr = err
			if deliveryCtx.Err() != nil {
				return classifyError(deliveryCtx.Err())
			}
		}
	}

	// A temporarily unreachable MX must not be hidden by a later MX that was
	// unusable for a permanent local-policy reason (for example, a private IP
	// target). Retry the recipient if any candidate may become reachable.
	if lastTemporaryErr != nil {
		return lastTemporaryErr
	}
	if lastPermanentErr != nil {
		return lastPermanentErr
	}
	return &DeliveryError{Message: "no usable MX servers found", Permanent: false}
}

func (c *Client) sendToHost(
	ctx context.Context,
	tlsServerName string,
	dialAddress string,
	sender string,
	recipient string,
	rawMessage []byte,
) error {
	session, err := c.connectToHost(ctx, tlsServerName, dialAddress)
	if err != nil {
		return err
	}
	defer session.close()

	client := session.client
	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		return classifyErrorAtStage("mail_from", err)
	}
	if err := client.Mail(sender); err != nil {
		return classifyErrorAtStage("mail_from", fmt.Errorf("MAIL FROM: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		return classifyErrorAtStage("rcpt_to", err)
	}
	if err := client.Rcpt(recipient); err != nil {
		return classifyErrorAtStage("rcpt_to", fmt.Errorf("RCPT TO: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		return classifyError(err)
	}
	writer, err := client.Data()
	if err != nil {
		return classifyErrorAtStage("data", fmt.Errorf("DATA: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		// Do not close the DATA writer here: Close sends the terminating dot
		// and could cause the remote server to accept a partial/empty message.
		// The deferred session close aborts the incomplete SMTP transaction.
		return classifyErrorAtStage("data_write", err)
	}
	if _, err := io.Copy(writer, bytes.NewReader(rawMessage)); err != nil {
		return classifyErrorAtStage("data_write", fmt.Errorf("write message: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		return classifyErrorAtStage("data_final", err)
	}
	if err := writer.Close(); err != nil {
		return classifyErrorAtStage("data_final", fmt.Errorf("finish DATA: %w", err))
	}
	return nil
}

// connectToHost performs TCP connection establishment, reads the 220 greeting,
// sends EHLO, and negotiates STARTTLS when offered. It never silently falls
// back to plaintext after a TLS handshake failure.
func (c *Client) connectToHost(
	ctx context.Context,
	tlsServerName string,
	dialAddress string,
) (*smtpSession, error) {
	conn, err := c.dialer.DialContext(ctx, "tcp", dialAddress)
	if err != nil {
		return nil, classifyErrorAtStage("connect", fmt.Errorf("connect to %s: %w", dialAddress, err))
	}

	// A cancelled parent context must interrupt a blocked greeting, EHLO or
	// TLS handshake. The callback closes the underlying socket immediately.
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		stopClose()
		_ = conn.Close()
		return nil, classifyErrorAtStage("greeting", err)
	}
	client, err := smtp.NewClient(conn, tlsServerName)
	if err != nil {
		stopClose()
		_ = conn.Close()
		return nil, classifyErrorAtStage("greeting", fmt.Errorf("SMTP greeting from %s: %w", tlsServerName, err))
	}

	if err := c.setOperationDeadline(ctx, conn); err != nil {
		stopClose()
		_ = conn.Close()
		return nil, classifyErrorAtStage("ehlo", err)
	}
	if err := client.Hello(c.config.EHLOName); err != nil {
		stopClose()
		_ = conn.Close()
		return nil, classifyErrorAtStage("ehlo", fmt.Errorf("EHLO %s at %s: %w", c.config.EHLOName, tlsServerName, err))
	}

	hasSTARTTLS, _ := client.Extension("STARTTLS")
	if c.config.RequireSTARTTLS && !hasSTARTTLS {
		stopClose()
		_ = conn.Close()
		return nil, &DeliveryError{
			Message:   fmt.Sprintf("MX host %s does not support required STARTTLS", tlsServerName),
			Permanent: false,
			Stage:     "starttls_policy",
		}
	}

	if hasSTARTTLS {
		tlsConfig := &tls.Config{
			ServerName: tlsServerName,
			MinVersion: tls.VersionTLS12,
		}
		if err := c.setOperationDeadline(ctx, conn); err != nil {
			stopClose()
			_ = conn.Close()
			return nil, classifyErrorAtStage("starttls", err)
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			stopClose()
			_ = conn.Close()
			return nil, classifyErrorAtStage("starttls", fmt.Errorf("STARTTLS with %s: %w", tlsServerName, err))
		}
	}

	return &smtpSession{client: client, conn: conn, stopClose: stopClose}, nil
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
