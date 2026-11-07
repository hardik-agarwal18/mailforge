package mimeparser

import (
	"bytes"
	"fmt"
	"strings"
)

// StripBccHeaders returns a copy of an RFC 5322 message with Bcc and
// Resent-Bcc header fields (including folded continuation lines) removed.
// The original message must be retained in storage; this sanitized copy is
// intended for SMTP DATA so blind-copy recipients are not disclosed to other
// recipients. The body and all non-Bcc header bytes are preserved verbatim.
func StripBccHeaders(raw []byte) ([]byte, error) {
	separator := []byte("\r\n\r\n")
	headerEnd := bytes.Index(raw, separator)
	if headerEnd < 0 {
		return nil, fmt.Errorf("%w: missing header/body separator", ErrMalformedMessage)
	}

	headerLines := bytes.Split(raw[:headerEnd], []byte("\r\n"))
	kept := make([][]byte, 0, len(headerLines))
	removeCurrentField := false
	removed := false

	for lineNumber, line := range headerLines {
		if len(line) == 0 {
			return nil, fmt.Errorf("%w: unexpected empty header line %d", ErrMalformedMessage, lineNumber+1)
		}

		// Continuation lines belong to the immediately preceding header field.
		if line[0] == ' ' || line[0] == '\t' {
			if !removeCurrentField {
				kept = append(kept, line)
			}
			continue
		}

		colon := bytes.IndexByte(line, ':')
		if colon <= 0 {
			return nil, fmt.Errorf("%w: malformed header field on line %d", ErrMalformedMessage, lineNumber+1)
		}
		fieldName := strings.TrimSpace(string(line[:colon]))
		removeCurrentField = strings.EqualFold(fieldName, "Bcc") ||
			strings.EqualFold(fieldName, "Resent-Bcc")
		if removeCurrentField {
			removed = true
			continue
		}
		kept = append(kept, line)
	}

	if !removed {
		return append([]byte(nil), raw...), nil
	}

	var sanitized bytes.Buffer
	sanitized.Grow(len(raw))
	for i, line := range kept {
		if i > 0 {
			sanitized.WriteString("\r\n")
		}
		sanitized.Write(line)
	}
	sanitized.Write(separator)
	sanitized.Write(raw[headerEnd+len(separator):])
	return sanitized.Bytes(), nil
}
