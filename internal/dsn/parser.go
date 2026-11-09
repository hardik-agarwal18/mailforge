// Package dsn parses RFC 3464 delivery-status notification messages.
// It extracts one recipient block because MailForge assigns a unique VERP
// reverse-path to every recipient delivery.
package dsn

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"net/textproto"
	"regexp"
	"strings"

	"mailforge/internal/emailaddr"
)

const MaxReportBytes = 256 * 1024

var statusPattern = regexp.MustCompile(`^[245]\.(0|[1-9][0-9]{0,2})\.(0|[1-9][0-9]{0,2})$`)

var ErrInvalidReport = errors.New("invalid delivery status notification")

type Report struct {
	MessageID          string
	FinalRecipient     string
	OriginalRecipient  string
	Action             string
	Status             string
	DiagnosticCode     string
	RemoteMTA          string
	ReportingMTA       string
	OriginalEnvelopeID string
}

func Parse(raw []byte) (Report, error) {
	if len(raw) == 0 || len(raw) > MaxReportBytes {
		return Report{}, fmt.Errorf("%w: report size must be between 1 and %d bytes", ErrInvalidReport, MaxReportBytes)
	}
	message, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return Report{}, fmt.Errorf("%w: parse RFC 5322 headers: %v", ErrInvalidReport, err)
	}
	mediaType, params, err := mime.ParseMediaType(message.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "multipart/report") || !strings.EqualFold(params["report-type"], "delivery-status") {
		return Report{}, fmt.Errorf("%w: Content-Type must be multipart/report; report-type=delivery-status", ErrInvalidReport)
	}
	boundary := params["boundary"]
	if boundary == "" {
		return Report{}, fmt.Errorf("%w: multipart/report boundary is missing", ErrInvalidReport)
	}

	reader := multipart.NewReader(message.Body, boundary)
	var statusPart []byte
	for {
		part, nextErr := reader.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return Report{}, fmt.Errorf("%w: read multipart/report: %v", ErrInvalidReport, nextErr)
		}
		partType, _, parseErr := mime.ParseMediaType(part.Header.Get("Content-Type"))
		if parseErr == nil && strings.EqualFold(partType, "message/delivery-status") {
			if statusPart != nil {
				_ = part.Close()
				return Report{}, fmt.Errorf("%w: multiple message/delivery-status parts", ErrInvalidReport)
			}
			statusPart, err = io.ReadAll(io.LimitReader(part, MaxReportBytes+1))
			if err != nil {
				_ = part.Close()
				return Report{}, fmt.Errorf("%w: read status part: %v", ErrInvalidReport, err)
			}
			if len(statusPart) > MaxReportBytes {
				_ = part.Close()
				return Report{}, fmt.Errorf("%w: status part is too large", ErrInvalidReport)
			}
		}
		_ = part.Close()
	}
	if len(statusPart) == 0 {
		return Report{}, fmt.Errorf("%w: message/delivery-status part is missing", ErrInvalidReport)
	}

	blocks, err := parseStatusBlocks(statusPart)
	if err != nil {
		return Report{}, err
	}
	// A per-recipient VERP return path must identify exactly one recipient.
	// Reject multi-recipient reports rather than risk applying one block to the
	// wrong local delivery record.
	if len(blocks) != 2 {
		return Report{}, fmt.Errorf("%w: expected one per-message block and one recipient block, got %d blocks", ErrInvalidReport, len(blocks))
	}
	perMessage, recipient := blocks[0], blocks[1]

	finalRecipient, err := mailboxField(recipient, "Final-Recipient")
	if err != nil {
		return Report{}, err
	}
	action, err := requiredField(recipient, "Action")
	if err != nil {
		return Report{}, err
	}
	action = strings.ToLower(strings.TrimSpace(action))
	switch action {
	case "failed", "delayed", "delivered", "relayed", "expanded":
	default:
		return Report{}, fmt.Errorf("%w: unsupported Action %q", ErrInvalidReport, action)
	}
	status, err := requiredField(recipient, "Status")
	if err != nil {
		return Report{}, err
	}
	status = strings.TrimSpace(status)
	if !statusPattern.MatchString(status) {
		return Report{}, fmt.Errorf("%w: invalid enhanced Status %q", ErrInvalidReport, status)
	}
	class := status[0]
	if (action == "failed" && class != '5') || (action == "delayed" && class != '4') || ((action == "delivered" || action == "relayed" || action == "expanded") && class != '2') {
		return Report{}, fmt.Errorf("%w: Action %q is inconsistent with Status %q", ErrInvalidReport, action, status)
	}

	originalRecipient, err := optionalMailboxField(recipient, "Original-Recipient")
	if err != nil {
		return Report{}, err
	}
	diagnostic, err := optionalField(recipient, "Diagnostic-Code")
	if err != nil {
		return Report{}, err
	}
	if diagnostic != "" {
		if _, value, ok := strings.Cut(diagnostic, ";"); ok {
			diagnostic = strings.TrimSpace(value)
		}
	}
	remoteMTA, err := optionalField(recipient, "Remote-MTA")
	if err != nil {
		return Report{}, err
	}
	if remoteMTA == "" {
		remoteMTA, _ = optionalField(perMessage, "Remote-MTA")
	}
	reportingMTA, err := requiredField(perMessage, "Reporting-MTA")
	if err != nil {
		return Report{}, err
	}
	envelopeID, err := optionalField(perMessage, "Original-Envelope-Id")
	if err != nil {
		return Report{}, err
	}

	return Report{
		MessageID:          message.Header.Get("Message-ID"),
		FinalRecipient:     finalRecipient,
		OriginalRecipient:  originalRecipient,
		Action:             action,
		Status:             status,
		DiagnosticCode:     truncate(diagnostic, 2000),
		RemoteMTA:          truncate(remoteMTA, 255),
		ReportingMTA:       truncate(reportingMTA, 255),
		OriginalEnvelopeID: truncate(envelopeID, 255),
	}, nil
}

func parseStatusBlocks(raw []byte) ([]textproto.MIMEHeader, error) {
	// RFC 3464 separates the per-message fields and each per-recipient field
	// block with an empty line. Normalize the final terminator only; preserve
	// all field values for textproto's continuation handling.
	text := strings.ReplaceAll(string(raw), "\r\n", "\n")
	if strings.Contains(text, "\r") {
		return nil, fmt.Errorf("%w: invalid line endings in status part", ErrInvalidReport)
	}
	blocksText := strings.Split(strings.TrimSpace(text), "\n\n")
	blocks := make([]textproto.MIMEHeader, 0, len(blocksText))
	for _, block := range blocksText {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		reader := textproto.NewReader(bufio.NewReader(strings.NewReader(strings.ReplaceAll(block, "\n", "\r\n") + "\r\n\r\n")))
		header, err := reader.ReadMIMEHeader()
		if err != nil {
			return nil, fmt.Errorf("%w: parse status field block: %v", ErrInvalidReport, err)
		}
		blocks = append(blocks, header)
	}
	return blocks, nil
}

func requiredField(header textproto.MIMEHeader, name string) (string, error) {
	values := header.Values(name)
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
		return "", fmt.Errorf("%w: %s must occur exactly once", ErrInvalidReport, name)
	}
	return strings.TrimSpace(values[0]), nil
}

func optionalField(header textproto.MIMEHeader, name string) (string, error) {
	values := header.Values(name)
	if len(values) > 1 {
		return "", fmt.Errorf("%w: %s must not occur more than once", ErrInvalidReport, name)
	}
	if len(values) == 0 {
		return "", nil
	}
	return strings.TrimSpace(values[0]), nil
}

func mailboxField(header textproto.MIMEHeader, name string) (string, error) {
	value, err := requiredField(header, name)
	if err != nil {
		return "", err
	}
	_, mailbox, ok := strings.Cut(value, ";")
	if !ok || !strings.EqualFold(strings.TrimSpace(value[:strings.Index(value, ";")]), "rfc822") {
		return "", fmt.Errorf("%w: %s must use the rfc822 address type", ErrInvalidReport, name)
	}
	mailbox = strings.TrimSpace(mailbox)
	if err := emailaddr.ValidateMailbox(mailbox); err != nil {
		return "", fmt.Errorf("%w: %s contains an invalid mailbox: %v", ErrInvalidReport, name, err)
	}
	return mailbox, nil
}

func optionalMailboxField(header textproto.MIMEHeader, name string) (string, error) {
	value, err := optionalField(header, name)
	if err != nil || value == "" {
		return "", err
	}
	_, mailbox, ok := strings.Cut(value, ";")
	if !ok || !strings.EqualFold(strings.TrimSpace(value[:strings.Index(value, ";")]), "rfc822") {
		return "", fmt.Errorf("%w: %s must use the rfc822 address type", ErrInvalidReport, name)
	}
	mailbox = strings.TrimSpace(mailbox)
	if err := emailaddr.ValidateMailbox(mailbox); err != nil {
		return "", fmt.Errorf("%w: %s contains an invalid mailbox: %v", ErrInvalidReport, name, err)
	}
	return mailbox, nil
}

func truncate(value string, max int) string {
	if len(value) > max {
		return value[:max]
	}
	return value
}
