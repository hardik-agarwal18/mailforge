// Package mimeparser parses RFC 5322 messages and their MIME entity trees.
// It deliberately preserves the original wire message elsewhere; this package
// produces a bounded, decoded representation for validation and indexing.
package mimeparser

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
	"strings"
	"time"
	"unicode/utf8"

	"mailforge/internal/emailaddr"
)

const (
	DefaultMaxHeaderBytes = 64 * 1024
	DefaultMaxHeaderLine  = 998
	DefaultMaxHeaderCount = 200
	DefaultMaxParts       = 100
	DefaultMaxDepth       = 10
	DefaultMaxDecodedSize = 10 * 1024 * 1024
)

var (
	ErrMalformedMessage = errors.New("malformed RFC 5322 message")
	ErrMIMETooLarge     = errors.New("MIME message exceeds parser limits")
)

type Limits struct {
	MaxHeaderBytes  int
	MaxHeaderLine   int
	MaxHeaderCount  int
	MaxParts        int
	MaxDepth        int
	MaxDecodedBytes int64
}

func DefaultLimits() Limits {
	return Limits{
		MaxHeaderBytes:  DefaultMaxHeaderBytes,
		MaxHeaderLine:   DefaultMaxHeaderLine,
		MaxHeaderCount:  DefaultMaxHeaderCount,
		MaxParts:        DefaultMaxParts,
		MaxDepth:        DefaultMaxDepth,
		MaxDecodedBytes: DefaultMaxDecodedSize,
	}
}

type Address struct {
	Name    string `json:"name,omitempty"`
	Address string `json:"address"`
}

type Part struct {
	Path             string         `json:"path"`
	MediaType        string         `json:"media_type"`
	Charset          string         `json:"charset,omitempty"`
	Disposition      string         `json:"disposition,omitempty"`
	Filename         string         `json:"filename,omitempty"`
	ContentID        string         `json:"content_id,omitempty"`
	TransferEncoding string         `json:"transfer_encoding"`
	Size             int64          `json:"size"`
	IsAttachment     bool           `json:"is_attachment"`
	IsInline         bool           `json:"is_inline"`
	SHA256           string         `json:"sha256"`
	Embedded         *ParsedMessage `json:"embedded,omitempty"`
	Content          []byte         `json:"-"`
}

type ParsedMessage struct {
	Subject      string     `json:"subject,omitempty"`
	From         []Address  `json:"from,omitempty"`
	Sender       []Address  `json:"sender,omitempty"`
	ReplyTo      []Address  `json:"reply_to,omitempty"`
	To           []Address  `json:"to,omitempty"`
	Cc           []Address  `json:"cc,omitempty"`
	Bcc          []Address  `json:"bcc,omitempty"`
	Date         *time.Time `json:"date,omitempty"`
	MessageID    string     `json:"message_id,omitempty"`
	MIMEVersion  string     `json:"mime_version,omitempty"`
	ContentType  string     `json:"content_type"`
	TextBody     string     `json:"text_body,omitempty"`
	HTMLBody     string     `json:"html_body,omitempty"`
	Parts        []Part     `json:"parts,omitempty"`
	Warnings     []string   `json:"warnings,omitempty"`
	DecodedBytes int64      `json:"decoded_bytes"`
}

type parser struct {
	limits       Limits
	partCount    int
	decodedBytes int64
	result       ParsedMessage
}

// Parse validates and decodes a message without changing its original bytes.
// It accepts messages with missing Date/From fields for interoperability, but
// records warnings because RFC 5322 defines those origination fields as required.
func Parse(raw []byte) (*ParsedMessage, error) {
	return ParseWithLimits(raw, DefaultLimits())
}

func ParseWithLimits(raw []byte, limits Limits) (*ParsedMessage, error) {
	p := &parser{limits: normalizeLimits(limits)}
	if err := p.parseMessageBytes(raw, 0); err != nil {
		return nil, err
	}
	p.result.DecodedBytes = p.decodedBytes
	return &p.result, nil
}

// parseMessageBytes parses an encapsulated RFC 5322 message using the same
// entity-count budget as its parent. rootDepth carries MIME nesting through
// message/rfc822 boundaries instead of resetting the recursion limit.
func (p *parser) parseMessageBytes(raw []byte, rootDepth int) error {
	if len(raw) == 0 {
		return fmt.Errorf("%w: empty message", ErrMalformedMessage)
	}
	if err := validateCRLF(raw); err != nil {
		return err
	}
	headerEnd := bytes.Index(raw, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return fmt.Errorf("%w: missing header/body separator", ErrMalformedMessage)
	}
	if headerEnd+4 > p.limits.MaxHeaderBytes {
		return fmt.Errorf("%w: header section too large", ErrMIMETooLarge)
	}
	if err := validateHeaderSection(raw[:headerEnd], p.limits); err != nil {
		return err
	}

	msg, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("%w: parse headers: %v", ErrMalformedMessage, err)
	}
	if err := p.parseMessageHeaders(msg.Header); err != nil {
		return err
	}
	return p.parseEntity(textproto.MIMEHeader(msg.Header), msg.Body, "1", rootDepth)
}

func normalizeLimits(l Limits) Limits {
	d := DefaultLimits()
	if l.MaxHeaderBytes > 0 {
		d.MaxHeaderBytes = l.MaxHeaderBytes
	}
	if l.MaxHeaderLine > 0 {
		d.MaxHeaderLine = l.MaxHeaderLine
	}
	if l.MaxHeaderCount > 0 {
		d.MaxHeaderCount = l.MaxHeaderCount
	}
	if l.MaxParts > 0 {
		d.MaxParts = l.MaxParts
	}
	if l.MaxDepth > 0 {
		d.MaxDepth = l.MaxDepth
	}
	if l.MaxDecodedBytes > 0 {
		d.MaxDecodedBytes = l.MaxDecodedBytes
	}
	return d
}

func validateCRLF(raw []byte) error {
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\n' && (i == 0 || raw[i-1] != '\r') {
			return fmt.Errorf("%w: bare LF at byte %d", ErrMalformedMessage, i)
		}
		if raw[i] == '\r' && (i+1 >= len(raw) || raw[i+1] != '\n') {
			return fmt.Errorf("%w: bare CR at byte %d", ErrMalformedMessage, i)
		}
	}
	return nil
}

func validateHeaderSection(raw []byte, limits Limits) error {
	lines := bytes.Split(raw, []byte("\r\n"))
	count := 0
	for i, line := range lines {
		if len(line) > limits.MaxHeaderLine {
			return fmt.Errorf("%w: header line %d exceeds %d bytes", ErrMIMETooLarge, i+1, limits.MaxHeaderLine)
		}
		if len(line) == 0 {
			return fmt.Errorf("%w: unexpected empty header line", ErrMalformedMessage)
		}
		if line[0] == ' ' || line[0] == '\t' {
			if i == 0 {
				return fmt.Errorf("%w: header begins with a continuation line", ErrMalformedMessage)
			}
			continue
		}
		count++
		if count > limits.MaxHeaderCount {
			return fmt.Errorf("%w: too many header fields", ErrMIMETooLarge)
		}
		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			return fmt.Errorf("%w: malformed header field on line %d", ErrMalformedMessage, i+1)
		}
		for _, c := range line[:colon] {
			if c < 33 || c > 126 {
				return fmt.Errorf("%w: invalid header field name", ErrMalformedMessage)
			}
		}
		for _, c := range line[colon+1:] {
			if c == '\t' {
				continue
			}
			if c < 32 || c > 126 {
				return fmt.Errorf("%w: non-ASCII or control octet in header field value", ErrMalformedMessage)
			}
		}
	}
	return nil
}

func (p *parser) parseMessageHeaders(h mail.Header) error {
	subjectValue, err := oneHeader(textproto.MIMEHeader(h), "Subject")
	if err != nil {
		return err
	}
	if p.result.Subject, err = decodeHeader(subjectValue); err != nil {
		p.result.Warnings = append(p.result.Warnings, "Subject contains an invalid encoded-word; raw value preserved")
		p.result.Subject = subjectValue
	}

	// Keep this order deterministic so malformed messages always produce the
	// same first validation error. Header addresses are parsed for mailbox
	// metadata; the SMTP RCPT TO envelope remains authoritative for delivery.
	addressHeaders := []struct {
		name string
		dst  *[]Address
	}{
		{name: "From", dst: &p.result.From},
		{name: "Sender", dst: &p.result.Sender},
		{name: "Reply-To", dst: &p.result.ReplyTo},
		{name: "To", dst: &p.result.To},
		{name: "Cc", dst: &p.result.Cc},
		{name: "Bcc", dst: &p.result.Bcc},
	}
	for _, field := range addressHeaders {
		values := h[textproto.CanonicalMIMEHeaderKey(field.name)]
		if len(values) == 0 {
			continue
		}
		if len(values) > 1 {
			return fmt.Errorf("%w: duplicate %s header", ErrMalformedMessage, field.name)
		}
		joined := strings.Join(values, ",")
		// RFC 5322 permits an empty Bcc field. It carries no recipient and is
		// retained only as an empty parsed list; outbound copies still strip it.
		if field.name == "Bcc" && strings.TrimSpace(joined) == "" {
			continue
		}
		addresses, parseErr := (&mail.AddressParser{WordDecoder: newWordDecoder()}).ParseList(joined)
		if parseErr != nil {
			return fmt.Errorf("%w: invalid %s header: %v", ErrMalformedMessage, field.name, parseErr)
		}
		for _, address := range addresses {
			addrSpec := formatAddrSpec(address.Address)
			if validateErr := emailaddr.ValidateMailbox(addrSpec); validateErr != nil {
				return fmt.Errorf("%w: invalid mailbox in %s header: %v", ErrMalformedMessage, field.name, validateErr)
			}
			*field.dst = append(*field.dst, Address{Name: address.Name, Address: addrSpec})
		}
	}

	if values := h["Date"]; len(values) > 1 {
		return fmt.Errorf("%w: duplicate Date header", ErrMalformedMessage)
	} else if len(values) == 1 {
		parsedDate, dateErr := mail.ParseDate(values[0])
		if dateErr != nil {
			return fmt.Errorf("%w: invalid Date header: %v", ErrMalformedMessage, dateErr)
		}
		p.result.Date = &parsedDate
	} else {
		p.result.Warnings = append(p.result.Warnings, "Date header is missing")
	}
	if len(p.result.From) == 0 {
		p.result.Warnings = append(p.result.Warnings, "From header is missing")
	} else if len(p.result.From) > 1 && len(p.result.Sender) == 0 {
		return fmt.Errorf("%w: Sender header is required when From contains multiple mailboxes", ErrMalformedMessage)
	}
	if len(p.result.Sender) > 1 {
		return fmt.Errorf("%w: Sender header must contain exactly one mailbox", ErrMalformedMessage)
	}

	messageID, err := oneHeader(textproto.MIMEHeader(h), "Message-ID")
	if err != nil {
		return err
	}
	p.result.MessageID = strings.TrimSpace(messageID)
	mimeVersion, err := oneHeader(textproto.MIMEHeader(h), "MIME-Version")
	if err != nil {
		return err
	}
	p.result.MIMEVersion = strings.TrimSpace(mimeVersion)
	if p.result.MIMEVersion != "" && p.result.MIMEVersion != "1.0" {
		return fmt.Errorf("%w: unsupported MIME-Version %q", ErrMalformedMessage, p.result.MIMEVersion)
	}
	if p.result.MIMEVersion == "" && (h.Get("Content-Type") != "" || h.Get("Content-Transfer-Encoding") != "") {
		p.result.Warnings = append(p.result.Warnings, "MIME-Version header is missing while MIME headers are present")
	}
	return nil
}

func formatAddrSpec(address string) string {
	// net/mail intentionally removes quotes from parsed local-parts. Reformat
	// through Address.String so metadata retains a valid mailbox addr-spec.
	formatted := (&mail.Address{Address: address}).String()
	if strings.HasPrefix(formatted, "<") && strings.HasSuffix(formatted, ">") {
		return formatted[1 : len(formatted)-1]
	}
	return address
}

func newWordDecoder() *mime.WordDecoder {
	return &mime.WordDecoder{
		CharsetReader: func(charset string, input io.Reader) (io.Reader, error) {
			encoded, err := io.ReadAll(io.LimitReader(input, DefaultMaxHeaderBytes+1))
			if err != nil {
				return nil, err
			}
			if len(encoded) > DefaultMaxHeaderBytes {
				return nil, ErrMIMETooLarge
			}
			decoded, err := decodeText(encoded, charset)
			if err != nil {
				return nil, err
			}
			return strings.NewReader(decoded), nil
		},
	}
}

func decodeHeader(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	return newWordDecoder().DecodeHeader(value)
}

func (p *parser) parseEntity(h textproto.MIMEHeader, body io.Reader, path string, depth int) error {
	if depth > p.limits.MaxDepth {
		return fmt.Errorf("%w: MIME nesting exceeds %d levels", ErrMIMETooLarge, p.limits.MaxDepth)
	}

	p.partCount++
	if p.partCount > p.limits.MaxParts {
		return fmt.Errorf("%w: more than %d MIME entities", ErrMIMETooLarge, p.limits.MaxParts)
	}

	contentTypeHeader, err := oneHeader(h, "Content-Type")
	if err != nil {
		return err
	}
	if contentTypeHeader == "" {
		contentTypeHeader = "text/plain; charset=us-ascii"
	}
	mediaType, params, err := mime.ParseMediaType(contentTypeHeader)
	if err != nil {
		return fmt.Errorf("%w: invalid Content-Type: %v", ErrMalformedMessage, err)
	}
	mediaType = strings.ToLower(mediaType)
	if !validMediaType(mediaType) {
		return fmt.Errorf("%w: invalid media type %q", ErrMalformedMessage, mediaType)
	}
	if path == "1" {
		p.result.ContentType = mediaType
	}

	cte, err := oneHeader(h, "Content-Transfer-Encoding")
	if err != nil {
		return err
	}
	cte = strings.ToLower(strings.TrimSpace(cte))
	if cte == "" {
		cte = "7bit"
	}

	dispositionHeader, err := oneHeader(h, "Content-Disposition")
	if err != nil {
		return err
	}
	disposition := ""
	filename := ""
	if dispositionHeader != "" {
		disposition, paramsDisposition, dispErr := mime.ParseMediaType(dispositionHeader)
		if dispErr != nil {
			return fmt.Errorf("%w: invalid Content-Disposition: %v", ErrMalformedMessage, dispErr)
		}
		disposition = strings.ToLower(disposition)
		if !validMIMEToken(disposition) {
			return fmt.Errorf("%w: invalid Content-Disposition %q", ErrMalformedMessage, disposition)
		}
		filename = paramsDisposition["filename"]
	}
	if filename == "" {
		filename = params["name"]
	}

	contentID, err := oneHeader(h, "Content-ID")
	if err != nil {
		return err
	}
	contentID = strings.Trim(strings.TrimSpace(contentID), "<>")

	if strings.HasPrefix(mediaType, "multipart/") {
		if cte != "7bit" && cte != "8bit" {
			return fmt.Errorf("%w: multipart entity cannot use %q transfer encoding", ErrMalformedMessage, cte)
		}
		boundary := params["boundary"]
		if boundary == "" || len(boundary) > 70 {
			return fmt.Errorf("%w: multipart boundary is missing or invalid", ErrMalformedMessage)
		}
		encoded, readErr := readBounded(body, p.limits.MaxDecodedBytes)
		if readErr != nil {
			return readErr
		}
		if err := validateIdentityEncoding(encoded, cte); err != nil {
			return err
		}
		reader := multipart.NewReader(bytes.NewReader(encoded), boundary)
		childIndex := 0
		for {
			part, nextErr := reader.NextRawPart()
			if nextErr == io.EOF {
				break
			}
			if nextErr != nil {
				return fmt.Errorf("%w: malformed multipart body at %s: %v", ErrMalformedMessage, path, nextErr)
			}
			childIndex++
			if err := validateMIMEPartHeader(part.Header, p.limits, childIndex, path); err != nil {
				return err
			}
			if mediaType == "multipart/digest" && len(part.Header[textproto.CanonicalMIMEHeaderKey("Content-Type")]) == 0 {
				part.Header.Set("Content-Type", "message/rfc822")
			}
			childPath := fmt.Sprintf("%s.%d", path, childIndex)
			if err := p.parseEntity(part.Header, part, childPath, depth+1); err != nil {
				return err
			}
		}
		if childIndex == 0 {
			return fmt.Errorf("%w: multipart entity contains no parts", ErrMalformedMessage)
		}
		return nil
	}

	if mediaType == "message/rfc822" {
		if cte != "7bit" && cte != "8bit" {
			return fmt.Errorf("%w: message/rfc822 cannot use %q transfer encoding", ErrMalformedMessage, cte)
		}
	}

	decoded, err := p.decodeBody(body, cte)
	if err != nil {
		return err
	}
	isInline := disposition == "inline" || (contentID != "" && disposition != "attachment")
	isBodyMediaType := mediaType == "text/plain" || mediaType == "text/html"
	isAttachment := disposition == "attachment" || (!isInline && filename != "") || (!isInline && !isBodyMediaType && mediaType != "message/rfc822")
	part := Part{
		Path: path, MediaType: mediaType, Charset: strings.ToLower(params["charset"]),
		Disposition: disposition, Filename: filename, ContentID: contentID,
		TransferEncoding: cte, Size: int64(len(decoded)),
		IsAttachment: isAttachment,
		IsInline:     isInline,
		Content:      decoded,
	}
	part.SHA256 = sha256Hex(decoded)
	if mediaType == "message/rfc822" {
		nestedParser := &parser{limits: p.limits, partCount: p.partCount}
		if nestedErr := nestedParser.parseMessageBytes(decoded, depth+1); nestedErr != nil {
			return fmt.Errorf("parse encapsulated message at %s: %w", path, nestedErr)
		}
		p.partCount = nestedParser.partCount
		nestedParser.result.DecodedBytes = nestedParser.decodedBytes
		part.Embedded = &nestedParser.result
	}
	p.result.Parts = append(p.result.Parts, part)

	if part.IsAttachment || mediaType == "message/rfc822" {
		return nil
	}
	if mediaType == "text/plain" {
		text, charsetErr := decodeText(decoded, part.Charset)
		if charsetErr != nil {
			p.result.Warnings = append(p.result.Warnings, fmt.Sprintf("part %s: %v", path, charsetErr))
		} else if p.result.TextBody == "" {
			p.result.TextBody = text
		}
	} else if mediaType == "text/html" {
		text, charsetErr := decodeText(decoded, part.Charset)
		if charsetErr != nil {
			p.result.Warnings = append(p.result.Warnings, fmt.Sprintf("part %s: %v", path, charsetErr))
		} else if p.result.HTMLBody == "" {
			p.result.HTMLBody = text
		}
	}
	return nil
}

func validateMIMEPartHeader(h textproto.MIMEHeader, limits Limits, childIndex int, parentPath string) error {
	fieldCount := 0
	headerBytes := 0
	for key, values := range h {
		for _, value := range values {
			fieldCount++
			headerBytes += len(key) + len(value) + 2 // colon and CRLF
		}
	}
	partPath := fmt.Sprintf("%s.%d", parentPath, childIndex)
	if fieldCount > limits.MaxHeaderCount {
		return fmt.Errorf("%w: too many headers in MIME part %s", ErrMIMETooLarge, partPath)
	}
	if headerBytes > limits.MaxHeaderBytes {
		return fmt.Errorf("%w: headers in MIME part %s exceed %d bytes", ErrMIMETooLarge, partPath, limits.MaxHeaderBytes)
	}
	return nil
}

func validMediaType(value string) bool {
	typeName, subtype, ok := strings.Cut(value, "/")
	if !ok || typeName == "" || subtype == "" || strings.Contains(subtype, "/") {
		return false
	}
	return validMIMEToken(typeName) && validMIMEToken(subtype)
}

func validMIMEToken(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		default:
			return false
		}
	}
	return true
}

func oneHeader(h textproto.MIMEHeader, key string) (string, error) {
	values := h[textproto.CanonicalMIMEHeaderKey(key)]
	if len(values) > 1 {
		return "", fmt.Errorf("%w: duplicate %s header", ErrMalformedMessage, key)
	}
	if len(values) == 0 {
		return "", nil
	}
	return strings.TrimSpace(values[0]), nil
}

func (p *parser) remainingDecodedLimit() int64 {
	remaining := p.limits.MaxDecodedBytes - p.decodedBytes
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (p *parser) readLimited(reader io.Reader, limit int64) ([]byte, error) {
	if limit < 0 {
		return nil, ErrMIMETooLarge
	}
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read MIME entity: %v", ErrMalformedMessage, err)
	}
	if int64(len(data)) > limit {
		return nil, ErrMIMETooLarge
	}
	p.decodedBytes += int64(len(data))
	return data, nil
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read MIME entity: %v", ErrMalformedMessage, err)
	}
	if int64(len(data)) > limit {
		return nil, ErrMIMETooLarge
	}
	return data, nil
}

func (p *parser) decodeBody(body io.Reader, encoding string) ([]byte, error) {
	var decoder io.Reader
	switch encoding {
	case "7bit", "8bit":
		decoder = body
	case "base64":
		decoder = base64.NewDecoder(base64.StdEncoding.Strict(), body)
	case "quoted-printable":
		decoder = quotedprintable.NewReader(body)
	default:
		return nil, fmt.Errorf("%w: unsupported Content-Transfer-Encoding %q", ErrMalformedMessage, encoding)
	}
	decoded, err := p.readLimited(decoder, p.remainingDecodedLimit())
	if err != nil {
		return nil, err
	}
	if err := validateIdentityEncoding(decoded, encoding); err != nil {
		return nil, err
	}
	return decoded, nil
}

func validateIdentityEncoding(body []byte, encoding string) error {
	switch encoding {
	case "7bit", "8bit":
		for _, b := range body {
			if b == 0 || b == 0x7f || (b < 0x20 && b != '\r' && b != '\n' && b != '\t') {
				return fmt.Errorf("%w: control octet in %s body", ErrMalformedMessage, encoding)
			}
			if encoding == "7bit" && b > 0x7f {
				return fmt.Errorf("%w: non-7-bit octet in 7bit body", ErrMalformedMessage)
			}
		}
	}
	return nil
}

func decodeText(body []byte, charset string) (string, error) {
	if charset == "" {
		charset = "us-ascii"
	}
	switch strings.ToLower(charset) {
	case "utf-8", "utf8":
		if !utf8.Valid(body) {
			return "", fmt.Errorf("invalid UTF-8 text body")
		}
		return string(body), nil
	case "us-ascii", "ascii":
		for _, b := range body {
			if b > 0x7f {
				return "", fmt.Errorf("non-ASCII octet in US-ASCII text body")
			}
		}
		return string(body), nil
	case "iso-8859-1", "latin1", "latin-1":
		runes := make([]rune, len(body))
		for i, b := range body {
			runes[i] = rune(b)
		}
		return string(runes), nil
	case "windows-1252", "cp1252":
		return decodeWindows1252(body), nil
	default:
		return "", fmt.Errorf("unsupported text charset %q; decoded bytes remain available in MIME part", charset)
	}
}

var windows1252Table = []rune("€�‚ƒ„…†‡ˆ‰Š‹Œ�Ž��‘’“”•–—˜™š›œ�žŸ")

func decodeWindows1252(body []byte) string {
	runes := make([]rune, len(body))
	for i, b := range body {
		if b >= 0x80 && b <= 0x9f {
			runes[i] = windows1252Table[int(b-0x80)]
		} else {
			runes[i] = rune(b)
		}
	}
	return string(runes)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}
