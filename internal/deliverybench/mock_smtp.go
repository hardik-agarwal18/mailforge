// Package deliverybench contains local-only infrastructure used by MailForge's
// end-to-end delivery benchmark. It is not intended to be exposed as a public
// SMTP service.
package deliverybench

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultMockMaxMessageBytes int64 = 15 << 20

// MockSMTPConfig controls deterministic failures in the local SMTP receiver.
type MockSMTPConfig struct {
	Delay time.Duration
	// RejectFirst rejects the first N RCPT TO commands. It is useful for
	// deterministic temporary-failure/retry tests where a later attempt must
	// succeed without changing the server configuration.
	RejectFirst           int64
	RejectEvery           int64
	RejectCode            int
	DropAfterDATAEvery    int64
	MaxMessageBytes       int64
	ConnectionReadTimeout time.Duration
}

// MockSMTPStats is a point-in-time snapshot of the receiver's counters.
type MockSMTPStats struct {
	Connections        int64 `json:"connections"`
	ActiveConnections  int64 `json:"active_connections"`
	AcceptedRecipients int64 `json:"accepted_recipients"`
	RejectedRecipients int64 `json:"rejected_recipients"`
	AcceptedMessages   int64 `json:"accepted_messages"`
	DroppedAfterDATA   int64 `json:"dropped_after_data"`
	MessagesOverLimit  int64 `json:"messages_over_limit"`
	BytesReceived      int64 `json:"bytes_received"`
}

// MockSMTPServer is a small SMTP receiver for isolated benchmark traffic.
// It supports the commands used by MailForge's outbound client and can inject
// deterministic RCPT rejection, delivery delay, and lost final DATA responses.
type MockSMTPServer struct {
	listener net.Listener
	config   MockSMTPConfig

	closeOnce  sync.Once
	acceptDone chan struct{}
	stopCh     chan struct{}
	handlers   sync.WaitGroup
	connMu     sync.Mutex
	conns      map[net.Conn]struct{}

	connections        atomic.Int64
	activeConnections  atomic.Int64
	recipientAttempts  atomic.Int64
	acceptedRecipients atomic.Int64
	rejectedRecipients atomic.Int64
	acceptedMessages   atomic.Int64
	droppedAfterDATA   atomic.Int64
	messagesOverLimit  atomic.Int64
	bytesReceived      atomic.Int64
}

// StartMockSMTPServer starts a mock receiver on addr. Use a loopback address
// such as 127.0.0.1:2526 when MailForge workers run on the host.
func StartMockSMTPServer(addr string, config MockSMTPConfig) (*MockSMTPServer, error) {
	if config.Delay < 0 {
		return nil, fmt.Errorf("mock SMTP delay cannot be negative")
	}
	if config.RejectFirst < 0 || config.RejectEvery < 0 || config.DropAfterDATAEvery < 0 {
		return nil, fmt.Errorf("mock SMTP failure intervals cannot be negative")
	}
	if config.RejectCode == 0 {
		config.RejectCode = 451
	}
	if config.RejectCode != 451 && config.RejectCode != 550 {
		return nil, fmt.Errorf("mock SMTP reject code must be 451 or 550")
	}
	if config.MaxMessageBytes <= 0 {
		config.MaxMessageBytes = defaultMockMaxMessageBytes
	}
	if config.ConnectionReadTimeout <= 0 {
		config.ConnectionReadTimeout = 2 * time.Minute
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for mock SMTP: %w", err)
	}

	server := &MockSMTPServer{
		listener:   listener,
		config:     config,
		acceptDone: make(chan struct{}),
		stopCh:     make(chan struct{}),
		conns:      make(map[net.Conn]struct{}),
	}
	go server.acceptLoop()
	return server, nil
}

// Addr returns the actual bound address, including an OS-selected port when
// the server was started with port zero.
func (s *MockSMTPServer) Addr() net.Addr {
	return s.listener.Addr()
}

// Stats returns a race-safe snapshot of the mock receiver counters.
func (s *MockSMTPServer) Stats() MockSMTPStats {
	return MockSMTPStats{
		Connections:        s.connections.Load(),
		ActiveConnections:  s.activeConnections.Load(),
		AcceptedRecipients: s.acceptedRecipients.Load(),
		RejectedRecipients: s.rejectedRecipients.Load(),
		AcceptedMessages:   s.acceptedMessages.Load(),
		DroppedAfterDATA:   s.droppedAfterDATA.Load(),
		MessagesOverLimit:  s.messagesOverLimit.Load(),
		BytesReceived:      s.bytesReceived.Load(),
	}
}

// Close stops accepting connections, closes active sessions, and waits for all
// handlers to exit.
func (s *MockSMTPServer) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		closeErr = s.listener.Close()
		close(s.stopCh)
		<-s.acceptDone

		s.connMu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.connMu.Unlock()

		s.handlers.Wait()
	})
	if errors.Is(closeErr, net.ErrClosed) {
		return nil
	}
	return closeErr
}

func (s *MockSMTPServer) acceptLoop() {
	defer close(s.acceptDone)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}

		s.connMu.Lock()
		s.conns[conn] = struct{}{}
		s.connMu.Unlock()

		s.connections.Add(1)
		s.activeConnections.Add(1)
		s.handlers.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *MockSMTPServer) handleConnection(conn net.Conn) {
	defer s.handlers.Done()
	defer s.activeConnections.Add(-1)
	defer func() {
		s.connMu.Lock()
		delete(s.conns, conn)
		s.connMu.Unlock()
		_ = conn.Close()
	}()

	_ = conn.SetDeadline(time.Now().Add(s.config.ConnectionReadTimeout))
	reader := bufio.NewReaderSize(conn, 32*1024)
	if _, err := fmt.Fprint(conn, "220 mock.mailforge.test ESMTP ready\r\n"); err != nil {
		return
	}

	for {
		_ = conn.SetDeadline(time.Now().Add(s.config.ConnectionReadTimeout))
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		command := strings.TrimSpace(line)
		upper := strings.ToUpper(command)

		switch {
		case strings.HasPrefix(upper, "EHLO "):
			if _, err := fmt.Fprint(conn, "250-mock.mailforge.test\r\n250 PIPELINING\r\n"); err != nil {
				return
			}
		case strings.HasPrefix(upper, "HELO "):
			if _, err := fmt.Fprint(conn, "250 mock.mailforge.test\r\n"); err != nil {
				return
			}
		case strings.HasPrefix(upper, "MAIL FROM:"):
			if _, err := fmt.Fprint(conn, "250 2.1.0 sender accepted\r\n"); err != nil {
				return
			}
		case strings.HasPrefix(upper, "RCPT TO:"):
			n := s.recipientAttempts.Add(1)
			reject := n <= s.config.RejectFirst ||
				(s.config.RejectEvery > 0 && n%s.config.RejectEvery == 0)
			if reject {
				s.rejectedRecipients.Add(1)
				if s.config.RejectCode == 550 {
					if _, err := fmt.Fprint(conn, "550 5.1.1 simulated permanent recipient rejection\r\n"); err != nil {
						return
					}
				} else if _, err := fmt.Fprint(conn, "451 4.3.0 simulated temporary recipient rejection\r\n"); err != nil {
					return
				}
				continue
			}
			s.acceptedRecipients.Add(1)
			if _, err := fmt.Fprint(conn, "250 2.1.5 recipient accepted\r\n"); err != nil {
				return
			}
		case upper == "DATA":
			if _, err := fmt.Fprint(conn, "354 end data with <CRLF>.<CRLF>\r\n"); err != nil {
				return
			}
			messageBytes, overLimit, readErr := readMockSMTPData(reader, s.config.MaxMessageBytes)
			if readErr != nil {
				return
			}
			s.bytesReceived.Add(messageBytes)
			if overLimit {
				s.messagesOverLimit.Add(1)
				if _, err := fmt.Fprint(conn, "552 5.3.4 message exceeds mock receiver limit\r\n"); err != nil {
					return
				}
				continue
			}

			messageNumber := s.acceptedMessages.Add(1)
			if s.config.DropAfterDATAEvery > 0 && messageNumber%s.config.DropAfterDATAEvery == 0 {
				s.droppedAfterDATA.Add(1)
				// The mock receiver treats the complete DATA body as accepted, then
				// drops the connection before the client can read the final 250.
				return
			}

			if s.config.Delay > 0 {
				timer := time.NewTimer(s.config.Delay)
				select {
				case <-timer.C:
				case <-s.stopCh:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return
				}
			}
			if _, err := fmt.Fprintf(conn, "250 2.0.0 accepted as mock-%d\r\n", messageNumber); err != nil {
				return
			}
		case upper == "RSET":
			if _, err := fmt.Fprint(conn, "250 2.0.0 reset\r\n"); err != nil {
				return
			}
		case upper == "NOOP":
			if _, err := fmt.Fprint(conn, "250 2.0.0 ok\r\n"); err != nil {
				return
			}
		case upper == "QUIT":
			_, _ = fmt.Fprint(conn, "221 2.0.0 bye\r\n")
			return
		default:
			if _, err := fmt.Fprint(conn, "500 5.5.1 unsupported mock command\r\n"); err != nil {
				return
			}
		}
	}
}

func readMockSMTPData(reader *bufio.Reader, maxBytes int64) (int64, bool, error) {
	var total int64
	overLimit := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return total, overLimit, err
		}
		if line == ".\r\n" || line == ".\n" {
			return total, overLimit, nil
		}
		if strings.HasPrefix(line, "..") {
			line = line[1:]
		}
		total += int64(len(line))
		if total > maxBytes {
			overLimit = true
		}
	}
}
