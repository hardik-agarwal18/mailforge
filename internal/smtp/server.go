package smtp

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"mailforge/internal/message"
	"net"
	"strings"
	"time"
)

const (
	maxLineSize           = 1000
	maxMessageSize        = 10 * 1024 * 1024
	maxAuthFailures       = 5
	tlsHandshakeTimeout   = 10 * time.Second
	authenticationTimeout = 5 * time.Second
)

var (
	errAuthCancelled            = errors.New("SMTP authentication cancelled")
	errAuthProtocol             = errors.New("invalid SMTP authentication exchange")
	errUnsupportedAuthMechanism = errors.New("unsupported SMTP AUTH mechanism")
)

type Server struct {
	Address         string
	messageAcceptor MessageAcceptor
	metrics         MetricsRecorder

	submission    bool
	tlsConfig     *tls.Config
	authenticator Authenticator
}

func NewServer(
	address string,
	messageAcceptor MessageAcceptor,
	metrics MetricsRecorder,
) *Server {
	return &Server{
		Address:         address,
		messageAcceptor: messageAcceptor,
		metrics:         metrics,
	}
}

// NewSubmissionServer creates an SMTP submission listener. STARTTLS and AUTH are
// mandatory before accepting a message. The supplied TLS configuration must
// contain a certificate (or a certificate callback).
func NewSubmissionServer(
	address string,
	messageAcceptor MessageAcceptor,
	metrics MetricsRecorder,
	tlsConfig *tls.Config,
	authenticator Authenticator,
) (*Server, error) {
	if messageAcceptor == nil {
		return nil, fmt.Errorf("message acceptor is required")
	}
	if tlsConfig == nil || (len(tlsConfig.Certificates) == 0 && tlsConfig.GetCertificate == nil) {
		return nil, fmt.Errorf("a TLS certificate or certificate callback is required")
	}
	if authenticator == nil {
		return nil, fmt.Errorf("SMTP authenticator is required")
	}

	config := tlsConfig.Clone()
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}

	return &Server{
		Address:         address,
		messageAcceptor: messageAcceptor,
		metrics:         metrics,
		submission:      true,
		tlsConfig:       config,
		authenticator:   authenticator,
	}, nil
}

func (s *Server) Start() error {
	return s.StartContext(context.Background())
}

func (s *Server) StartContext(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	stopClose := context.AfterFunc(ctx, func() {
		_ = listener.Close()
	})
	defer stopClose()

	log.Println("MailForge SMTP listening on", s.Address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept SMTP connection: %w", err)
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	if s.metrics != nil {
		s.metrics.RecordConnectionOpened()
		defer s.metrics.RecordConnectionClosed()
	}
	defer func() {
		_ = conn.Close()
	}()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))

	reader := bufio.NewReaderSize(conn, 4096)
	session := NewSession()

	if err := writeReply(conn, "220 mailforge.local ESMTP ready"); err != nil {
		return
	}

	for {
		line, err := readSMTPLine(reader, maxLineSize)
		if err != nil {
			if err != io.EOF {
				log.Println("SMTP read error:", err)
			}
			return
		}

		cmd, err := ParseCommand(line)
		if err != nil {
			_ = writeReply(conn, "500 5.5.2 Syntax error")
			continue
		}

		// Never log AUTH arguments: they can contain base64-encoded credentials.
		if cmd.Name != "AUTH" {
			log.Printf("SMTP command: %s", cmd.Name)
		}

		switch cmd.Name {
		case "EHLO":
			if cmd.Arg == "" {
				_ = writeReply(conn, "501 5.5.4 EHLO requires a hostname")
				continue
			}

			session.ResetTransaction()
			session.Hostname = cmd.Arg
			session.ESMTP = true
			_ = writeMultilineReply(conn, s.ehloCapabilities(session))

		case "HELO":
			if cmd.Arg == "" {
				_ = writeReply(conn, "501 5.5.4 HELO requires a hostname")
				continue
			}

			session.ResetTransaction()
			session.Hostname = cmd.Arg
			session.ESMTP = false
			_ = writeReply(conn, "250 mailforge.local")

		case "STARTTLS":
			if !s.submission {
				_ = writeReply(conn, "502 5.5.1 STARTTLS is not available")
				continue
			}
			if session.TLSActive {
				_ = writeReply(conn, "503 5.5.1 TLS is already active")
				continue
			}
			if !session.ESMTP || session.Hostname == "" || session.State != StateReady {
				_ = writeReply(conn, "503 5.5.1 Issue EHLO before STARTTLS")
				continue
			}
			if cmd.Arg != "" {
				_ = writeReply(conn, "501 5.5.4 STARTTLS takes no arguments")
				continue
			}

			if err := writeReply(conn, "220 2.0.0 Ready to start TLS"); err != nil {
				return
			}

			tlsConn := tls.Server(conn, s.tlsConfig)
			handshakeCtx, cancel := context.WithTimeout(context.Background(), tlsHandshakeTimeout)
			err := tlsConn.HandshakeContext(handshakeCtx)
			cancel()
			if err != nil {
				log.Printf("SMTP STARTTLS handshake failed: %v", err)
				return
			}

			conn = tlsConn
			reader = bufio.NewReaderSize(conn, 4096)
			_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
			session.ResetTransaction()
			session.Hostname = ""
			session.ESMTP = false
			session.TLSActive = true
			session.Authenticated = false
			session.AuthenticatedUsername = ""
			session.AuthenticatedEmail = ""

		case "AUTH":
			if !s.submission {
				_ = writeReply(conn, "502 5.5.1 AUTH is not available")
				continue
			}
			if !session.TLSActive {
				_ = writeReply(conn, "538 5.7.11 Encryption required for requested authentication mechanism")
				continue
			}
			if !session.ESMTP || session.Hostname == "" || session.State != StateReady {
				_ = writeReply(conn, "503 5.5.1 Issue EHLO before AUTH")
				continue
			}
			if session.Authenticated {
				_ = writeReply(conn, "503 5.5.1 Already authenticated")
				continue
			}
			if session.AuthFailures >= maxAuthFailures {
				_ = writeReply(conn, "421 4.7.0 Too many authentication failures")
				return
			}

			username, password, authErr := readAuthCredentials(conn, reader, cmd.Arg)
			if authErr != nil {
				if errors.Is(authErr, errUnsupportedAuthMechanism) {
					_ = writeReply(conn, "504 5.5.4 Unrecognized authentication mechanism")
					continue
				}
				if errors.Is(authErr, errAuthCancelled) {
					_ = writeReply(conn, "501 5.7.0 Authentication cancelled")
				} else {
					_ = writeReply(conn, "501 5.5.2 Invalid authentication exchange")
				}
				continue
			}

			authCtx, cancel := context.WithTimeout(context.Background(), authenticationTimeout)
			identity, authErr := s.authenticator.Authenticate(authCtx, username, password)
			cancel()
			if authErr != nil {
				if errors.Is(authErr, ErrInvalidCredentials) {
					if s.metrics != nil {
						s.metrics.RecordAuthentication(false)
					}
					session.AuthFailures++
					_ = writeReply(conn, "535 5.7.8 Authentication credentials invalid")
					if session.AuthFailures >= maxAuthFailures {
						_ = writeReply(conn, "421 4.7.0 Too many authentication failures")
						return
					}
					continue
				}

				log.Printf("SMTP authentication backend error: %v", authErr)
				_ = writeReply(conn, "454 4.7.0 Temporary authentication failure")
				continue
			}

			if identity.Username == "" || identity.Email == "" {
				log.Printf("SMTP authenticator returned an incomplete identity")
				_ = writeReply(conn, "454 4.7.0 Temporary authentication failure")
				continue
			}

			if s.metrics != nil {
				s.metrics.RecordAuthentication(true)
			}
			session.Authenticated = true
			session.AuthenticatedUsername = identity.Username
			session.AuthenticatedEmail = identity.Email
			_ = writeReply(conn, "235 2.7.0 Authentication successful")

		case "MAIL":
			if s.submission {
				if !session.TLSActive {
					_ = writeReply(conn, "530 5.7.0 Must issue STARTTLS first")
					continue
				}
				if !session.Authenticated {
					_ = writeReply(conn, "530 5.7.0 Authentication required")
					continue
				}
			}
			if session.State != StateReady {
				_ = writeReply(conn, "503 5.5.1 Bad sequence of commands")
				continue
			}

			address, ok := parsePath(cmd.Arg, "FROM:")
			if !ok {
				_ = writeReply(conn, "501 5.1.7 Invalid sender")
				continue
			}
			if s.submission && !strings.EqualFold(address, session.AuthenticatedEmail) {
				_ = writeReply(conn, "553 5.7.1 Sender address is not authorized for this account")
				continue
			}

			session.Sender = address
			session.Recipients = nil
			session.State = StateMail
			_ = writeReply(conn, "250 2.1.0 Sender accepted")

		case "RCPT":
			if !session.CanReceiveRecipient() {
				_ = writeReply(conn, "503 5.5.1 Need MAIL FROM first")
				continue
			}

			address, ok := parsePath(cmd.Arg, "TO:")
			if !ok {
				_ = writeReply(conn, "501 5.1.3 Invalid recipient")
				continue
			}

			session.Recipients = append(session.Recipients, address)
			session.State = StateRecipient
			_ = writeReply(conn, "250 2.1.5 Recipient accepted")

		case "DATA":
			if session.State != StateRecipient {
				_ = writeReply(conn, "503 5.5.1 Need RCPT TO first")
				continue
			}
			if cmd.Arg != "" {
				_ = writeReply(conn, "501 5.5.4 DATA takes no arguments")
				continue
			}

			if err := writeReply(conn, "354 End data with <CRLF>.<CRLF>"); err != nil {
				return
			}

			submissionStarted := time.Now()
			rawMessage, err := receiveMessage(reader)
			if err != nil {
				if s.metrics != nil {
					s.metrics.RecordSubmission(time.Since(submissionStarted), false)
				}
				_ = writeReply(conn, "552 5.3.4 Message size exceeds fixed maximum message size")
				session.ResetTransaction()
				return
			}

			session.MessageData = rawMessage
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			stored, err := s.messageAcceptor.Accept(
				ctx,
				message.NewMessage{
					Sender:     session.Sender,
					Recipients: session.Recipients,
					RawMessage: session.MessageData,
				},
			)
			cancel()

			if s.metrics != nil {
				s.metrics.RecordSubmission(time.Since(submissionStarted), err == nil)
			}
			if err != nil {
				log.Printf("Message persistence failed: %v", err)
				session.ResetTransaction()
				_ = writeReply(conn, "451 4.3.0 Temporary message storage failure")
				continue
			}

			log.Printf("Message persisted: ID=%s Message-ID=%s", stored.ID, stored.MessageID)
			session.ResetTransaction()
			_ = writeReply(conn, fmt.Sprintf("250 2.0.0 Message accepted: %s", stored.MessageID))

		case "RSET":
			if session.State == StateConnected || session.State == StateClosed {
				_ = writeReply(conn, "503 5.5.1 Bad sequence")
				continue
			}
			session.ResetTransaction()
			_ = writeReply(conn, "250 2.0.0 Transaction reset")

		case "NOOP":
			_ = writeReply(conn, "250 2.0.0 OK")

		case "QUIT":
			session.State = StateClosed
			_ = writeReply(conn, "221 2.0.0 Bye")
			return

		default:
			_ = writeReply(conn, "502 5.5.1 Command not implemented")
		}
	}
}

func (s *Server) ehloCapabilities(session *Session) []string {
	lines := []string{"250-mailforge.local"}
	if s.submission && !session.TLSActive {
		lines = append(lines, "250-STARTTLS")
	}
	if s.submission && session.TLSActive {
		lines = append(lines, "250-AUTH PLAIN LOGIN")
	}
	lines = append(lines, "250 HELP")
	return lines
}

func readAuthCredentials(conn net.Conn, reader *bufio.Reader, argument string) (string, string, error) {
	fields := strings.Fields(argument)
	if len(fields) == 0 || len(fields) > 2 {
		return "", "", errAuthProtocol
	}

	mechanism := strings.ToUpper(fields[0])
	switch mechanism {
	case "PLAIN":
		var response string
		if len(fields) == 2 {
			response = fields[1]
		} else {
			if err := writeReply(conn, "334 "); err != nil {
				return "", "", err
			}
			line, err := readSMTPLine(reader, maxLineSize)
			if err != nil {
				return "", "", err
			}
			response = line
		}
		if response == "*" {
			return "", "", errAuthCancelled
		}
		decoded, err := base64.StdEncoding.DecodeString(response)
		if err != nil {
			return "", "", errAuthProtocol
		}
		parts := strings.SplitN(string(decoded), "\x00", 3)
		if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
			return "", "", errAuthProtocol
		}
		if parts[0] != "" && !strings.EqualFold(parts[0], parts[1]) {
			return "", "", errAuthProtocol
		}
		return parts[1], parts[2], nil

	case "LOGIN":
		var username string
		if len(fields) == 2 {
			decoded, err := base64.StdEncoding.DecodeString(fields[1])
			if err != nil {
				return "", "", errAuthProtocol
			}
			username = string(decoded)
		} else {
			if err := writeReply(conn, "334 VXNlcm5hbWU6"); err != nil {
				return "", "", err
			}
			line, err := readSMTPLine(reader, maxLineSize)
			if err != nil {
				return "", "", err
			}
			if line == "*" {
				return "", "", errAuthCancelled
			}
			decoded, err := base64.StdEncoding.DecodeString(line)
			if err != nil {
				return "", "", errAuthProtocol
			}
			username = string(decoded)
		}

		if username == "" {
			return "", "", errAuthProtocol
		}
		if err := writeReply(conn, "334 UGFzc3dvcmQ6"); err != nil {
			return "", "", err
		}
		passwordLine, err := readSMTPLine(reader, maxLineSize)
		if err != nil {
			return "", "", err
		}
		if passwordLine == "*" {
			return "", "", errAuthCancelled
		}
		passwordBytes, err := base64.StdEncoding.DecodeString(passwordLine)
		if err != nil || len(passwordBytes) == 0 {
			return "", "", errAuthProtocol
		}
		return username, string(passwordBytes), nil

	default:
		return "", "", fmt.Errorf("%w: %s", errUnsupportedAuthMechanism, mechanism)
	}
}

func readSMTPLine(reader *bufio.Reader, limit int) (string, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > limit {
			return "", fmt.Errorf("SMTP line too long")
		}
		line = append(line, part...)
		if err == nil {
			if len(line) < 2 || line[len(line)-2] != '\r' {
				return "", fmt.Errorf("invalid line ending")
			}
			return string(line[:len(line)-2]), nil
		}
		if err != bufio.ErrBufferFull {
			return "", err
		}
	}
}

func receiveMessage(reader *bufio.Reader) ([]byte, error) {
	var message []byte
	for {
		line, err := readSMTPLine(reader, maxLineSize)
		if err != nil {
			return nil, err
		}
		if line == "." {
			return message, nil
		}
		if strings.HasPrefix(line, "..") {
			line = line[1:]
		}
		if len(message)+len(line)+2 > maxMessageSize {
			return nil, fmt.Errorf("message too large")
		}
		message = append(message, line...)
		message = append(message, '\r', '\n')
	}
}

func parsePath(arg, prefix string) (string, bool) {
	if len(arg) < len(prefix) || !strings.EqualFold(arg[:len(prefix)], prefix) {
		return "", false
	}
	path := strings.TrimSpace(arg[len(prefix):])
	if len(path) < 2 || path[0] != '<' || path[len(path)-1] != '>' {
		return "", false
	}
	address := path[1 : len(path)-1]
	if address == "" {
		return "", prefix == "FROM:"
	}
	if strings.ContainsAny(address, " \t\r\n<>") || !strings.Contains(address, "@") {
		return "", false
	}
	return address, true
}

func writeReply(conn net.Conn, reply string) error {
	_, err := fmt.Fprintf(conn, "%s\r\n", reply)
	return err
}

func writeMultilineReply(conn net.Conn, replies []string) error {
	for _, reply := range replies {
		if err := writeReply(conn, reply); err != nil {
			return err
		}
	}
	return nil
}
