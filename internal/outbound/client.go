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
// kept separate from Send so connection establishment can be tested
// independently from the SMTP mail transaction.
type smtpSession struct {
	client    *smtp.Client
	conn      net.Conn // underlying socket; closing it also closes a TLS wrapper
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

// Send preserves the original API for callers that do not need transport
// metadata. Use SendWithResult when delivery status/audit records need to
// persist the TLS and remote-MX details.
func (c *Client) Send(ctx context.Context, sender, recipient string, rawMessage []byte) error {
	_, err := c.SendWithResult(ctx, sender, recipient, rawMessage)
	return err
}

// SendWithResult sends one envelope recipient and returns the actual transport
// security used by the accepting MX/relay. SMTP acceptance means the next-hop
// server accepted responsibility; it does not prove final inbox placement.
func (c *Client) SendWithResult(
	ctx context.Context,
	sender string,
	recipient string,
	rawMessage []byte,
) (DeliveryInfo, error) {
	if err := emailaddr.ValidateReversePath(sender); err != nil {
		return DeliveryInfo{}, &DeliveryError{Message: fmt.Sprintf("invalid envelope sender: %v", err), Permanent: true}
	}
	if err := emailaddr.ValidateMailbox(recipient); err != nil {
		return DeliveryInfo{}, &DeliveryError{Message: fmt.Sprintf("invalid envelope recipient: %v", err), Permanent: true}
	}

	// Bcc addresses are carried in the SMTP envelope (RCPT TO), not exposed
	// in the message data sent to any recipient. Strip both ordinary and
	// resent Bcc header fields from every per-recipient delivery copy.
	deliveryMessage, err := mimeparser.StripBccHeaders(rawMessage)
	if err != nil {
		return DeliveryInfo{}, &DeliveryError{Message: fmt.Sprintf("invalid message headers: %v", err), Permanent: true}
	}
	// Sign only after Bcc headers have been removed. The signature therefore
	// covers the exact RFC 5322 message that will be transmitted and cannot
	// disclose blind-copy envelope recipients.
	if c.config.DKIMSigner != nil {
		deliveryMessage, err = c.config.DKIMSigner.Sign(deliveryMessage)
		if err != nil {
			return DeliveryInfo{}, &DeliveryError{
				Message:   fmt.Sprintf("DKIM signing failed: %v", err),
				Permanent: true,
				Stage:     "dkim_sign",
			}
		}
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
			c.config.RelayTLSMode,
		)
	}

	mxRecords, err := c.resolver.Lookup(deliveryCtx, recipient)
	if err != nil {
		if errors.Is(err, ErrNullMX) || errors.Is(err, ErrInvalidMXSet) {
			return DeliveryInfo{}, &DeliveryError{Message: err.Error(), Permanent: true}
		}
		return DeliveryInfo{}, classifyError(err)
	}

	var lastInfo DeliveryInfo
	var lastTemporaryErr error
	var lastPermanentErr error
	directTLSMode := TLSModeStartTLS
	if c.config.RequireSTARTTLS {
		directTLSMode = TLSModeRequiredSTARTTLS
	}

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
			info, sendErr := c.sendToHost(
				deliveryCtx,
				host,
				dialAddress,
				sender,
				recipient,
				deliveryMessage,
				directTLSMode,
			)
			lastInfo = info
			if sendErr == nil {
				return info, nil
			}

			var deliveryErr *DeliveryError
			if errors.As(sendErr, &deliveryErr) && deliveryErr.Permanent {
				// A definitive 5xx SMTP response is a permanent transaction
				// outcome. Do not route the same recipient through another MX.
				return info, deliveryErr
			}
			lastTemporaryErr = sendErr
			if deliveryCtx.Err() != nil {
				return info, classifyError(deliveryCtx.Err())
			}
		}
	}

	// A temporarily unreachable MX must not be hidden by a later MX that was
	// unusable for a permanent local-policy reason (for example, a private IP
	// target). Retry the recipient if any candidate may become reachable.
	if lastTemporaryErr != nil {
		return lastInfo, lastTemporaryErr
	}
	if lastPermanentErr != nil {
		return lastInfo, lastPermanentErr
	}
	return lastInfo, &DeliveryError{Message: "no usable MX servers found", Permanent: false}
}

func (c *Client) sendToHost(
	ctx context.Context,
	tlsServerName string,
	dialAddress string,
	sender string,
	recipient string,
	rawMessage []byte,
	tlsMode TLSMode,
) (DeliveryInfo, error) {
	session, err := c.connectToHost(ctx, tlsServerName, dialAddress, tlsMode)
	if err != nil {
		return DeliveryInfo{RemoteHost: tlsServerName, TLSMode: string(tlsMode)}, err
	}
	defer session.close()

	info := deliveryInfoFromSession(session, tlsServerName, tlsMode)
	client := session.client
	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		return info, classifyErrorAtStage("mail_from", err)
	}
	if err := client.Mail(sender); err != nil {
		return info, classifyErrorAtStage("mail_from", fmt.Errorf("MAIL FROM: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		return info, classifyErrorAtStage("rcpt_to", err)
	}
	if err := client.Rcpt(recipient); err != nil {
		return info, classifyErrorAtStage("rcpt_to", fmt.Errorf("RCPT TO: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		return info, classifyErrorAtStage("data", err)
	}
	writer, err := client.Data()
	if err != nil {
		return info, classifyErrorAtStage("data", fmt.Errorf("DATA: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		// Do not close the DATA writer here: Close sends the terminating dot
		// and could cause the remote server to accept a partial/empty message.
		// The deferred session close aborts the incomplete SMTP transaction.
		return info, classifyErrorAtStage("data_write", err)
	}
	if _, err := io.Copy(writer, bytes.NewReader(rawMessage)); err != nil {
		return info, classifyErrorAtStage("data_write", fmt.Errorf("write message: %w", err))
	}

	if err := c.setOperationDeadline(ctx, session.conn); err != nil {
		// The DATA terminator has not been sent yet, so the remote server
		// cannot have accepted this message. This is safe to retry.
		return info, classifyErrorAtStage("data_write", err)
	}
	if err := writer.Close(); err != nil {
		return info, classifyErrorAtStage("data_final", fmt.Errorf("finish DATA: %w", err))
	}
	return info, nil
}

// connectToHost establishes TCP (or implicit TLS), reads the 220 greeting,
// sends EHLO and negotiates STARTTLS according to tlsMode. A TLS handshake
// or certificate validation failure always aborts the session; it never
// falls back to plaintext.
func (c *Client) connectToHost(
	ctx context.Context,
	tlsServerName string,
	dialAddress string,
	tlsMode TLSMode,
) (*smtpSession, error) {
	rawConn, err := c.dialer.DialContext(ctx, "tcp", dialAddress)
	if err != nil {
		return nil, classifyErrorAtStage("connect", fmt.Errorf("connect to %s: %w", dialAddress, err))
	}

	// A cancelled parent context must interrupt a blocked greeting, EHLO or
	// TLS handshake. The callback closes the underlying socket immediately.
	stopClose := context.AfterFunc(ctx, func() { _ = rawConn.Close() })
	conn := net.Conn(rawConn)

	if tlsMode == TLSModeImplicit {
		if err := c.setOperationDeadline(ctx, rawConn); err != nil {
			stopClose()
			_ = rawConn.Close()
			return nil, classifyErrorAtStage("tls_handshake", err)
		}
		tlsConn := tls.Client(rawConn, c.tlsConfigForServer(tlsServerName))
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			stopClose()
			_ = rawConn.Close()
			return nil, classifyErrorAtStage("tls_handshake", fmt.Errorf("implicit TLS with %s: %w", tlsServerName, err))
		}
		conn = tlsConn
	}

	if err := c.setOperationDeadline(ctx, rawConn); err != nil {
		stopClose()
		_ = rawConn.Close()
		return nil, classifyErrorAtStage("greeting", err)
	}
	client, err := smtp.NewClient(conn, tlsServerName)
	if err != nil {
		stopClose()
		_ = rawConn.Close()
		return nil, classifyErrorAtStage("greeting", fmt.Errorf("SMTP greeting from %s: %w", tlsServerName, err))
	}

	if err := c.setOperationDeadline(ctx, rawConn); err != nil {
		stopClose()
		_ = rawConn.Close()
		return nil, classifyErrorAtStage("ehlo", err)
	}
	if err := client.Hello(c.config.EHLOName); err != nil {
		stopClose()
		_ = rawConn.Close()
		return nil, classifyErrorAtStage("ehlo", fmt.Errorf("EHLO %s at %s: %w", c.config.EHLOName, tlsServerName, err))
	}

	if tlsMode == TLSModeNone || tlsMode == TLSModeImplicit {
		return &smtpSession{client: client, conn: rawConn, stopClose: stopClose}, nil
	}

	hasSTARTTLS, _ := client.Extension("STARTTLS")
	if tlsMode == TLSModeRequiredSTARTTLS && !hasSTARTTLS {
		stopClose()
		_ = rawConn.Close()
		return nil, &DeliveryError{
			Message:   fmt.Sprintf("SMTP host %s does not support required STARTTLS", tlsServerName),
			Permanent: false,
			Stage:     "starttls_policy",
		}
	}

	if hasSTARTTLS {
		if err := c.setOperationDeadline(ctx, rawConn); err != nil {
			stopClose()
			_ = rawConn.Close()
			return nil, classifyErrorAtStage("starttls", err)
		}
		if err := client.StartTLS(c.tlsConfigForServer(tlsServerName)); err != nil {
			stopClose()
			_ = rawConn.Close()
			return nil, classifyErrorAtStage("starttls", fmt.Errorf("STARTTLS with %s: %w", tlsServerName, err))
		}
	}

	return &smtpSession{client: client, conn: rawConn, stopClose: stopClose}, nil
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
