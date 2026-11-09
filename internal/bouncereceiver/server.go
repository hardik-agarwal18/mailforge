// Package bouncereceiver implements a deliberately restricted inbound SMTP
// endpoint for MailForge VERP/DSN addresses. It is not a general-purpose MTA:
// it accepts only null reverse-path DSNs addressed to a valid, signed VERP
// recipient and never relays or forwards mail.
package bouncereceiver

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"mailforge/internal/bounce"
	"mailforge/internal/dsn"
)

const (
	defaultMaxConnections = 100
	defaultCommandTimeout = 2 * time.Minute
	defaultDataTimeout    = 5 * time.Minute
	defaultProcessTimeout = 20 * time.Second
	smtpMaxLineBytes      = 1000 // RFC 5321 command/data line limit, including CRLF.
)

// Processor applies the DSN and its recipient state transition durably. It
// must return nil only after its transaction commits, so a 250 response never
// acknowledges a DSN that MailForge has not persisted.
type Processor interface {
	ProcessDSN(ctx context.Context, envelopeRecipient string, rawMessage []byte) error
}

type ProcessFunc func(context.Context, string, []byte) error

func (f ProcessFunc) ProcessDSN(ctx context.Context, envelopeRecipient string, rawMessage []byte) error {
	return f(ctx, envelopeRecipient, rawMessage)
}

// TemporaryError marks a processing error as retryable by the sending MTA.
// Database outages and timeouts should be wrapped with this type; malformed or
// uncorrelatable DSNs should be returned as ordinary errors and receive 550.
type TemporaryError struct{ Err error }

func (e TemporaryError) Error() string {
	if e.Err == nil {
		return "temporary DSN processing failure"
	}
	return e.Err.Error()
}

func (e TemporaryError) Unwrap() error { return e.Err }

// Config controls the dedicated bounce listener. The default address is
// loopback-only; public deployment should normally put a hardened MTA such as
// Postfix in front and forward only the bounce domain to this listener.
type Config struct {
	Address        string
	Hostname       string
	MaxConnections int
	CommandTimeout time.Duration
	DataTimeout    time.Duration
	ProcessTimeout time.Duration
	TLSConfig      *tls.Config
	// TrustedPeerCIDRs limits the listener to a trusted front MTA or private
	// network. When omitted, only loopback peers are accepted.
	TrustedPeerCIDRs []string
	// MaxDSNsPerPeerPerMinute bounds DSN processing from one source IP.
	// Defaults to 300; set explicitly for the expected MTA delivery volume.
	MaxDSNsPerPeerPerMinute int
}

type Server struct {
	config    Config
	generator *bounce.Generator
	processor Processor

	mu         sync.Mutex
	conns      map[net.Conn]struct{}
	capacity   chan struct{}
	trustedIPs []*net.IPNet
	rateMu     sync.Mutex
	peerRates  map[string]peerRate
	maxDSNRate int
	requireTLS bool
}

func New(config Config, generator *bounce.Generator, processor Processor) (*Server, error) {
	if generator == nil {
		return nil, errors.New("VERP bounce address generator is required")
	}
	if processor == nil {
		return nil, errors.New("DSN processor is required")
	}
	if strings.TrimSpace(config.Address) == "" {
		config.Address = "127.0.0.1:2526"
	}
	if strings.TrimSpace(config.Hostname) == "" {
		config.Hostname = "localhost"
	}
	if strings.ContainsAny(config.Hostname, " \r\n\t") {
		return nil, errors.New("SMTP bounce hostname must be a single hostname token")
	}
	if config.MaxConnections <= 0 {
		config.MaxConnections = defaultMaxConnections
	}
	if config.MaxDSNsPerPeerPerMinute <= 0 {
		config.MaxDSNsPerPeerPerMinute = 300
	}
	trustedIPs := make([]*net.IPNet, 0, len(config.TrustedPeerCIDRs))
	for _, rawCIDR := range config.TrustedPeerCIDRs {
		rawCIDR = strings.TrimSpace(rawCIDR)
		if rawCIDR == "" {
			continue
		}
		if ip := net.ParseIP(rawCIDR); ip != nil {
			maskBits := 128
			if ip.To4() != nil {
				ip = ip.To4()
				maskBits = 32
			}
			trustedIPs = append(trustedIPs, &net.IPNet{IP: ip, Mask: net.CIDRMask(maskBits, maskBits)})
			continue
		}
		_, network, parseErr := net.ParseCIDR(rawCIDR)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid trusted DSN peer CIDR %q: %w", rawCIDR, parseErr)
		}
		if prefix, _ := network.Mask.Size(); prefix == 0 {
			return nil, fmt.Errorf("trusted DSN peer CIDR %q is too broad; /0 is not allowed", rawCIDR)
		}
		trustedIPs = append(trustedIPs, network)
	}
	if err := validateListenerTrust(config.Address, trustedIPs, config.TLSConfig); err != nil {
		return nil, err
	}
	if config.CommandTimeout <= 0 {
		config.CommandTimeout = defaultCommandTimeout
	}
	if config.DataTimeout <= 0 {
		config.DataTimeout = defaultDataTimeout
	}
	if config.ProcessTimeout <= 0 {
		config.ProcessTimeout = defaultProcessTimeout
	}
	if config.TLSConfig != nil {
		config.TLSConfig = config.TLSConfig.Clone()
		if config.TLSConfig.MinVersion == 0 || config.TLSConfig.MinVersion < tls.VersionTLS12 {
			config.TLSConfig.MinVersion = tls.VersionTLS12
		}
	}
	return &Server{
		config:     config,
		generator:  generator,
		processor:  processor,
		conns:      make(map[net.Conn]struct{}),
		capacity:   make(chan struct{}, config.MaxConnections),
		trustedIPs: trustedIPs,
		peerRates:  make(map[string]peerRate),
		maxDSNRate: config.MaxDSNsPerPeerPerMinute,
		requireTLS: config.TLSConfig != nil && config.TLSConfig.ClientAuth == tls.RequireAndVerifyClientCert,
	}, nil
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("listen for inbound DSNs on %s: %w", s.config.Address, err)
	}
	return s.Serve(ctx, listener)
}

// Serve accepts inbound DSN sessions from an existing listener. It closes all
// active sessions on cancellation and waits for their handlers to exit.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if s == nil {
		return errors.New("SMTP bounce receiver is nil")
	}
	if listener == nil {
		return errors.New("SMTP bounce listener is nil")
	}
	// Validate the actual listener too: callers of Serve may supply a socket
	// whose bind address differs from Config.Address.
	if err := validateListenerTrust(listener.Addr().String(), s.trustedIPs, s.config.TLSConfig); err != nil {
		return err
	}
	var handlers sync.WaitGroup
	stopWatcher := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
			s.closeConnections()
		case <-stopWatcher:
		}
	}()
	defer func() {
		close(stopWatcher)
		_ = listener.Close()
		s.closeConnections()
		handlers.Wait()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept inbound DSN connection: %w", err)
		}
		peerIP := remoteIP(conn.RemoteAddr())
		if peerIP == nil || !s.isTrustedPeer(peerIP) {
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_, _ = io.WriteString(conn, "550 5.7.1 DSN receiver accepts connections only from trusted MTA peers\r\n")
			_ = conn.Close()
			continue
		}
		select {
		case s.capacity <- struct{}{}:
			s.mu.Lock()
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			handlers.Add(1)
			go func(c net.Conn) {
				defer handlers.Done()
				defer func() {
					<-s.capacity
					s.mu.Lock()
					delete(s.conns, c)
					s.mu.Unlock()
					_ = c.Close()
				}()
				s.handleConnection(ctx, c)
			}(conn)
		default:
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_, _ = io.WriteString(conn, "421 4.3.2 Too many inbound DSN connections\r\n")
			_ = conn.Close()
		}
	}
}

func (s *Server) closeConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := range s.conns {
		_ = conn.Close()
	}
}

func (s *Server) handleConnection(parent context.Context, conn net.Conn) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	reader := bufio.NewReaderSize(conn, smtpMaxLineBytes+1)
	writer := bufio.NewWriter(conn)
	write := func(reply string) error {
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := writer.WriteString(reply + "\r\n"); err != nil {
			return err
		}
		return writer.Flush()
	}
	writeMultiline := func(lines ...string) error {
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		for _, line := range lines {
			if _, err := writer.WriteString(line + "\r\n"); err != nil {
				return err
			}
		}
		return writer.Flush()
	}

	if err := write("220 " + s.config.Hostname + " MailForge DSN receiver ready"); err != nil {
		return
	}
	greeted := false
	ehloDone := false
	mailAccepted := false
	envelopeRecipient := ""
	secure := false

	resetTransaction := func() {
		mailAccepted = false
		envelopeRecipient = ""
	}

	for {
		timeout := s.config.CommandTimeout
		if mailAccepted && envelopeRecipient != "" {
			timeout = s.config.DataTimeout
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		line, err := readSMTPLine(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				_ = write("500 5.5.2 Invalid or overlong SMTP command line")
			}
			return
		}
		command, argument, _ := strings.Cut(line, " ")
		command = strings.ToUpper(strings.TrimSpace(command))
		argument = strings.TrimSpace(argument)

		switch command {
		case "EHLO":
			if argument == "" {
				if write("501 5.5.4 EHLO requires a domain address") != nil {
					return
				}
				continue
			}
			greeted = true
			ehloDone = true
			resetTransaction()
			lines := []string{"250-" + s.config.Hostname, fmt.Sprintf("250-SIZE %d", dsn.MaxReportBytes)}
			if s.config.TLSConfig != nil && !secure {
				lines = append(lines, "250-STARTTLS")
			}
			lines = append(lines, "250 HELP")
			if writeMultiline(lines...) != nil {
				return
			}
		case "HELO":
			if argument == "" {
				if write("501 5.5.4 HELO requires a domain address") != nil {
					return
				}
				continue
			}
			greeted = true
			ehloDone = false
			resetTransaction()
			if write("250 "+s.config.Hostname) != nil {
				return
			}
		case "STARTTLS":
			if s.config.TLSConfig == nil || secure {
				if write("502 5.5.1 STARTTLS is unavailable") != nil {
					return
				}
				continue
			}
			if !ehloDone || mailAccepted {
				if write("503 5.5.1 Send EHLO before STARTTLS and outside a mail transaction") != nil {
					return
				}
				continue
			}
			if write("220 2.0.0 Ready to start TLS") != nil {
				return
			}
			tlsConn := tls.Server(conn, s.config.TLSConfig)
			handshakeCtx, handshakeCancel := context.WithTimeout(ctx, 15*time.Second)
			err := tlsConn.HandshakeContext(handshakeCtx)
			handshakeCancel()
			if err != nil {
				return
			}
			conn = tlsConn
			reader = bufio.NewReaderSize(conn, smtpMaxLineBytes+1)
			writer = bufio.NewWriter(conn)
			secure = true
			greeted = false // RFC 3207 requires a new EHLO after TLS.
			ehloDone = false
			resetTransaction()
		case "MAIL":
			// A new MAIL command starts a new SMTP transaction even when the
			// supplied reverse-path is rejected.
			resetTransaction()
			if s.requireTLS && !secure {
				if write("530 5.7.0 STARTTLS with a trusted client certificate is required before MAIL") != nil {
					return
				}
				continue
			}
			if !greeted {
				if write("503 5.5.1 Send EHLO or HELO first") != nil {
					return
				}
				continue
			}
			path, parseErr := parseSMTPPath(argument, "FROM")
			if parseErr != nil {
				if write("501 5.5.4 Invalid MAIL FROM") != nil {
					return
				}
				continue
			}
			if path != "" {
				if write("550 5.7.1 Bounce receiver requires the null reverse-path") != nil {
					return
				}
				continue
			}
			mailAccepted = true
			if write("250 2.1.0 Null reverse-path accepted") != nil {
				return
			}
		case "RCPT":
			if !mailAccepted {
				if write("503 5.5.1 MAIL FROM required first") != nil {
					return
				}
				continue
			}
			if envelopeRecipient != "" {
				if write("452 4.5.3 Only one VERP recipient is accepted per DSN") != nil {
					return
				}
				continue
			}
			path, parseErr := parseSMTPPath(argument, "TO")
			if parseErr != nil || path == "" {
				if write("501 5.5.4 Invalid RCPT TO") != nil {
					return
				}
				continue
			}
			if _, verifyErr := s.generator.RecipientID(path); verifyErr != nil {
				if write("550 5.1.1 Unknown or invalid MailForge bounce address") != nil {
					return
				}
				continue
			}
			envelopeRecipient = path
			if write("250 2.1.5 VERP recipient accepted") != nil {
				return
			}
		case "DATA":
			if !mailAccepted || envelopeRecipient == "" {
				if write("503 5.5.1 MAIL FROM and one valid RCPT TO required first") != nil {
					return
				}
				continue
			}
			if argument != "" {
				if write("501 5.5.4 DATA takes no arguments") != nil {
					return
				}
				continue
			}
			if write("354 End data with <CRLF>.<CRLF>") != nil {
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(s.config.DataTimeout))
			raw, tooLarge, readErr := readSMTPData(reader, dsn.MaxReportBytes)
			if readErr != nil {
				return
			}
			if tooLarge {
				resetTransaction()
				if write("552 5.3.4 DSN exceeds the maximum message size") != nil {
					return
				}
				continue
			}
			peer := remoteIP(conn.RemoteAddr())
			if peer == nil || !s.allowDSN(peer.String(), time.Now()) {
				resetTransaction()
				if write("451 4.7.0 DSN rate limit exceeded; retry later") != nil {
					return
				}
				continue
			}
			processCtx, processCancel := context.WithTimeout(ctx, s.config.ProcessTimeout)
			processErr := s.processor.ProcessDSN(processCtx, envelopeRecipient, raw)
			processCancel()
			resetTransaction()
			if processErr != nil {
				var temporary TemporaryError
				if errors.As(processErr, &temporary) {
					if write("451 4.3.0 DSN processing temporarily unavailable; retry later") != nil {
						return
					}
					continue
				}
				if write("550 5.6.0 DSN rejected") != nil {
					return
				}
				continue
			}
			if write("250 2.0.0 DSN processed") != nil {
				return
			}
		case "RSET":
			resetTransaction()
			if write("250 2.0.0 Reset state") != nil {
				return
			}
		case "NOOP":
			if write("250 2.0.0 OK") != nil {
				return
			}
		case "QUIT":
			_ = write("221 2.0.0 Bye")
			return
		default:
			if write("502 5.5.1 Command not implemented by DSN-only receiver") != nil {
				return
			}
		}
	}
}

func validateListenerTrust(address string, trustedIPs []*net.IPNet, tlsConfig *tls.Config) error {
	if isLoopbackListenAddress(address) {
		return nil
	}
	if len(trustedIPs) == 0 {
		return errors.New("non-loopback DSN listener requires TrustedPeerCIDRs; place a trusted MTA in front and restrict its source addresses")
	}
	if tlsConfig == nil || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.ClientCAs == nil {
		return errors.New("non-loopback DSN listener requires mutual TLS with a client CA (RequireAndVerifyClientCert) in addition to TrustedPeerCIDRs")
	}
	return nil
}

type peerRate struct {
	windowStart time.Time
	count       int
}

func remoteIP(address net.Addr) net.IP {
	if address == nil {
		return nil
	}
	host, _, err := net.SplitHostPort(address.String())
	if err != nil {
		return nil
	}
	return net.ParseIP(strings.Trim(host, "[]"))
}

func isLoopbackListenAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.EqualFold(host, "localhost") {
		return err == nil && strings.EqualFold(host, "localhost")
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) isTrustedPeer(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if len(s.trustedIPs) == 0 {
		return false
	}
	for _, network := range s.trustedIPs {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// allowDSN implements a per-source, per-process fixed-window limit. The map is
// opportunistically pruned so expired peer entries do not grow without bound.
func (s *Server) allowDSN(peer string, now time.Time) bool {
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	window := time.Minute
	state, ok := s.peerRates[peer]
	if !ok || now.Sub(state.windowStart) >= window || now.Before(state.windowStart) {
		state = peerRate{windowStart: now}
	}
	if state.count >= s.maxDSNRate {
		s.peerRates[peer] = state
		return false
	}
	state.count++
	s.peerRates[peer] = state
	if len(s.peerRates) > 4096 {
		for address, entry := range s.peerRates {
			if now.Sub(entry.windowStart) >= 2*window || now.Before(entry.windowStart) {
				delete(s.peerRates, address)
			}
		}
	}
	return true
}

func readSMTPLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(line) > smtpMaxLineBytes {
		return "", fmt.Errorf("SMTP line exceeds %d bytes", smtpMaxLineBytes)
	}
	if err != nil {
		return "", err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("SMTP line must end in CRLF")
	}
	return string(line[:len(line)-2]), nil
}

func readSMTPData(reader *bufio.Reader, maxBytes int) ([]byte, bool, error) {
	if maxBytes <= 0 {
		return nil, false, errors.New("maximum DSN size must be positive")
	}
	data := make([]byte, 0, min(maxBytes, 32*1024))
	tooLarge := false
	for {
		line, err := readSMTPLine(reader)
		if err != nil {
			return nil, false, err
		}
		if line == "." {
			return data, tooLarge, nil
		}
		if strings.HasPrefix(line, ".") {
			line = line[1:] // SMTP dot transparency.
		}
		if !tooLarge {
			needed := len(line) + 2
			if len(data)+needed > maxBytes {
				tooLarge = true
				data = nil
			} else {
				data = append(data, line...)
				data = append(data, '\r', '\n')
			}
		}
	}
}

func parseSMTPPath(argument, keyword string) (string, error) {
	prefix := keyword + ":"
	if len(argument) < len(prefix) || !strings.EqualFold(argument[:len(prefix)], prefix) {
		return "", errors.New("unexpected SMTP path command")
	}
	rest := strings.TrimSpace(argument[len(prefix):])
	if len(rest) < 2 || rest[0] != '<' {
		return "", errors.New("SMTP path must be enclosed in angle brackets")
	}
	end := strings.IndexByte(rest, '>')
	if end < 1 {
		return "", errors.New("SMTP path has no closing angle bracket")
	}
	path := rest[1:end]
	parameterSuffix := rest[end+1:]
	parameters := strings.TrimSpace(parameterSuffix)
	if parameters != "" {
		if parameterSuffix[0] != ' ' && parameterSuffix[0] != '\t' {
			return "", errors.New("SMTP path parameters must be separated by whitespace")
		}
		if strings.ContainsAny(parameters, "\r\n\x00<>") {
			return "", errors.New("invalid SMTP path parameters")
		}
	}
	return path, nil
}
