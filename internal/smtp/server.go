package smtp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

const (
	maxLineSize    = 1000
	maxMessageSize = 10 * 1024 * 1024
)

type Server struct {
	Address string
}

func NewServer(address string) *Server {
	return &Server{Address: address}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	log.Println("MailForge listening on", s.Address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

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
				log.Println("Read error:", err)
			}
			return
		}

		cmd, err := ParseCommand(line)
		if err != nil {
			_ = writeReply(conn, "500 Syntax error")
			continue
		}

		log.Printf("Command: %s", cmd.Name)

		switch cmd.Name {

		case "EHLO":
			if cmd.Arg == "" {
				_ = writeReply(conn, "501 EHLO requires a hostname")
				continue
			}

			session.ResetTransaction()
			session.Hostname = cmd.Arg

			_ = writeMultilineReply(conn, []string{
				"250-mailforge.local",
				"250 HELP",
			})

		case "HELO":
			if cmd.Arg == "" {
				_ = writeReply(conn, "501 HELO requires a hostname")
				continue
			}

			session.ResetTransaction()
			session.Hostname = cmd.Arg
			_ = writeReply(conn, "250 mailforge.local")

		case "MAIL":
			if session.State != StateReady {
				_ = writeReply(conn, "503 Bad sequence of commands")
				continue
			}

			address, ok := parsePath(cmd.Arg, "FROM:")
			if !ok {
				_ = writeReply(conn, "501 Invalid sender")
				continue
			}

			session.Sender = address
			session.Recipients = nil
			session.State = StateMail

			_ = writeReply(conn, "250 Sender accepted")

		case "RCPT":
			if !session.CanReceiveRecipient() {
				_ = writeReply(conn, "503 Need MAIL FROM first")
				continue
			}

			address, ok := parsePath(cmd.Arg, "TO:")
			if !ok {
				_ = writeReply(conn, "501 Invalid recipient")
				continue
			}

			session.Recipients = append(session.Recipients, address)
			session.State = StateRecipient

			_ = writeReply(conn, "250 Recipient accepted")

		case "DATA":
			if session.State != StateRecipient {
				_ = writeReply(conn, "503 Need RCPT TO first")
				continue
			}

			if cmd.Arg != "" {
				_ = writeReply(conn, "501 DATA takes no arguments")
				continue
			}

			_ = writeReply(conn,
				"354 End data with <CRLF>.<CRLF>")

			message, err := receiveMessage(reader)
			if err != nil {
				_ = writeReply(conn, "552 Message too large")
				session.ResetTransaction()
				return
			}

			session.MessageData = message

			// Temporary: We have received the message,
			// but have not persisted or delivered it yet.
			log.Printf(
				"Received message from %s for %v (%d bytes)",
				session.Sender,
				session.Recipients,
				len(message),
			)

			session.ResetTransaction()
			_ = writeReply(conn, "451 Message not persisted yet")

		case "RSET":
			if session.State == StateConnected ||
				session.State == StateClosed {
				_ = writeReply(conn, "503 Bad sequence")
				continue
			}

			session.ResetTransaction()
			_ = writeReply(conn, "250 Transaction reset")

		case "NOOP":
			_ = writeReply(conn, "250 OK")

		case "QUIT":
			session.State = StateClosed
			_ = writeReply(conn, "221 Bye")
			return

		default:
			_ = writeReply(conn, "502 Command not implemented")
		}
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
			if len(line) < 2 ||
				line[len(line)-2] != '\r' {
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

		// A single dot terminates DATA.
		if line == "." {
			return message, nil
		}

		// SMTP dot-stuffing:
		// Two leading dots represent one actual dot.
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
	if len(arg) < len(prefix) ||
		!strings.EqualFold(arg[:len(prefix)], prefix) {
		return "", false
	}

	path := strings.TrimSpace(arg[len(prefix):])

	if len(path) < 2 ||
		path[0] != '<' ||
		path[len(path)-1] != '>' {
		return "", false
	}

	address := path[1 : len(path)-1]

	if address == "" {
		return "", prefix == "FROM:"
	}

	if strings.ContainsAny(address, " \t\r\n<>") {
		return "", false
	}

	if !strings.Contains(address, "@") {
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
