package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:2525")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("MailForge SMTP server listening on :2525")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	fmt.Fprint(conn, "220 mailforge.local ESMTP ready\r\n")

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimRight(line, "\r\n")

		log.Println("Received:", line)

		parts := strings.SplitN(line, " ", 2)
		command := strings.ToUpper(parts[0])

		switch command {
		case "EHLO":
			fmt.Fprint(conn,
				"250-mailforge.local\r\n"+
					"250 HELP\r\n")

		case "HELO":
			fmt.Fprint(conn, "250 mailforge.local\r\n")

		case "NOOP":
			fmt.Fprint(conn, "250 OK\r\n")

		case "QUIT":
			fmt.Fprint(conn, "221 Bye\r\n")
			return

		default:
			fmt.Fprint(conn, "502 Command not implemented\r\n")
		}
	}
}
