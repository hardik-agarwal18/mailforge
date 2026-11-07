package mimeparser

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParsePlainTextMessage(t *testing.T) {
	raw := []byte("From: Alice Example <alice@example.com>\r\n" +
		"To: Bob <bob@example.net>\r\n" +
		"Date: Sat, 07 Nov 2026 12:00:00 +0530\r\n" +
		"Subject: Hello\r\n" +
		"Message-ID: <one@example.com>\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"\r\n" +
		"Hello, world!\r\n")

	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.Subject != "Hello" {
		t.Fatalf("subject = %q", parsed.Subject)
	}
	if len(parsed.From) != 1 || parsed.From[0].Address != "alice@example.com" {
		t.Fatalf("from = %#v", parsed.From)
	}
	if len(parsed.To) != 1 || parsed.To[0].Address != "bob@example.net" {
		t.Fatalf("to = %#v", parsed.To)
	}
	if parsed.TextBody != "Hello, world!\r\n" {
		t.Fatalf("text body = %q", parsed.TextBody)
	}
	if parsed.ContentType != "text/plain" {
		t.Fatalf("content type = %q", parsed.ContentType)
	}
	if len(parsed.Parts) != 1 || parsed.Parts[0].Size == 0 {
		t.Fatalf("parts = %#v", parsed.Parts)
	}
	if parsed.Date == nil {
		t.Fatal("expected parsed Date header")
	}
}

func TestParseRFC2047Subject(t *testing.T) {
	raw := []byte("Subject: =?UTF-8?B?SGVsbMOz?=\r\n" +
		"From: a@example.com\r\n" +
		"Date: Sat, 07 Nov 2026 12:00:00 +0000\r\n" +
		"\r\ntext\r\n")
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.Subject != "Helló" {
		t.Fatalf("subject = %q, want Helló", parsed.Subject)
	}
}

func TestParseRFC2047LegacyCharset(t *testing.T) {
	raw := []byte("From: =?ISO-8859-1?Q?Andr=E9?= <andre@example.com>\r\n" +
		"Subject: =?ISO-8859-1?Q?caf=E9?=\r\n\r\nbody\r\n")
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.Subject != "café" {
		t.Fatalf("subject = %q, want café", parsed.Subject)
	}
	if len(parsed.From) != 1 || parsed.From[0].Name != "André" {
		t.Fatalf("From = %#v, want decoded display name", parsed.From)
	}
}

func TestParseMultipartAlternative(t *testing.T) {
	raw := []byte("From: sender@example.com\r\n" +
		"Date: Sat, 07 Nov 2026 12:00:00 +0000\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: multipart/alternative; boundary=alt-123\r\n" +
		"\r\n" +
		"--alt-123\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"Content-Transfer-Encoding: quoted-printable\r\n" +
		"\r\nHello=2C plain text\r\n" +
		"--alt-123\r\n" +
		"Content-Type: text/html; charset=utf-8\r\n" +
		"\r\n<b>Hello HTML</b>\r\n" +
		"--alt-123--\r\n")

	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.TextBody != "Hello, plain text" {
		t.Fatalf("text body = %q", parsed.TextBody)
	}
	if parsed.HTMLBody != "<b>Hello HTML</b>" {
		t.Fatalf("HTML body = %q", parsed.HTMLBody)
	}
	if len(parsed.Parts) != 2 {
		t.Fatalf("part count = %d, want 2", len(parsed.Parts))
	}
	if parsed.Parts[0].Path != "1.1" || parsed.Parts[1].Path != "1.2" {
		t.Fatalf("part paths = %q, %q", parsed.Parts[0].Path, parsed.Parts[1].Path)
	}
}

func TestParseMultipartAttachment(t *testing.T) {
	raw := []byte("From: sender@example.com\r\n" +
		"Date: Sat, 07 Nov 2026 12:00:00 +0000\r\n" +
		"MIME-Version: 1.0\r\n" +
		"Content-Type: multipart/mixed; boundary=mix-123\r\n" +
		"\r\n" +
		"--mix-123\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n\r\nSee attached.\r\n" +
		"--mix-123\r\n" +
		"Content-Type: application/octet-stream\r\n" +
		"Content-Disposition: attachment; filename=hello.txt\r\n" +
		"Content-Transfer-Encoding: base64\r\n\r\n" +
		"SGVsbG8gZnJvbSBNYWlsRm9yZ2Uh\r\n" +
		"--mix-123--\r\n")

	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.TextBody != "See attached." {
		t.Fatalf("text body = %q", parsed.TextBody)
	}
	if len(parsed.Parts) != 2 {
		t.Fatalf("part count = %d, want 2", len(parsed.Parts))
	}
	attachment := parsed.Parts[1]
	if !attachment.IsAttachment || attachment.Filename != "hello.txt" {
		t.Fatalf("attachment metadata = %#v", attachment)
	}
	if string(attachment.Content) != "Hello from MailForge!" {
		t.Fatalf("attachment bytes = %q", attachment.Content)
	}
}

func TestParseRFC2231Filename(t *testing.T) {
	raw := []byte("Content-Type: application/octet-stream\r\n" +
		"Content-Disposition: attachment; filename*=utf-8''caf%C3%A9.txt\r\n" +
		"\r\nabc\r\n")
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(parsed.Parts) != 1 || parsed.Parts[0].Filename != "café.txt" {
		t.Fatalf("parts = %#v", parsed.Parts)
	}
}

func TestParseAllowsMissingOriginationFieldsButWarns(t *testing.T) {
	parsed, err := Parse([]byte("Subject: minimal\r\n\r\nbody\r\n"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(parsed.Warnings) != 2 {
		t.Fatalf("warnings = %#v, want missing From and Date", parsed.Warnings)
	}
}

func TestParseRejectsMalformedMessages(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"missing separator", "Subject: no body separator\r\n"},
		{"bare LF", "Subject: bad\n\nbody\n"},
		{"malformed header", "Not-A-Header\r\n\r\nbody\r\n"},
		{"invalid date", "Date: yesterday\r\n\r\nbody\r\n"},
		{"invalid From", "From: not-an-address\r\n\r\nbody\r\n"},
		{"invalid Content-Type", "Content-Type: not-a-media-type\r\n\r\nbody\r\n"},
		{"missing multipart boundary", "Content-Type: multipart/mixed\r\n\r\n--x--\r\n"},
		{"unsupported CTE", "Content-Transfer-Encoding: x-rot13\r\n\r\nhello\r\n"},
		{"invalid base64", "Content-Transfer-Encoding: base64\r\n\r\n%%%\r\n"},
		{"invalid multipart boundary", "Content-Type: multipart/mixed; boundary=x\r\n\r\nnot a multipart\r\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Parse([]byte(test.raw)); err == nil {
				t.Fatal("Parse() succeeded, want error")
			}
		})
	}
}

func TestParseEnforcesLimits(t *testing.T) {
	t.Run("header bytes", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxHeaderBytes = 12
		_, err := ParseWithLimits([]byte("Subject: a long subject\r\n\r\nbody"), limits)
		if !errors.Is(err, ErrMIMETooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("header count", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxHeaderCount = 1
		_, err := ParseWithLimits([]byte("Subject: x\r\nX-Test: y\r\n\r\nbody"), limits)
		if !errors.Is(err, ErrMIMETooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("decoded bytes", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxDecodedBytes = 3
		_, err := ParseWithLimits([]byte("Content-Type: text/plain; charset=utf-8\r\n\r\nhello"), limits)
		if !errors.Is(err, ErrMIMETooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("parts", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxParts = 1
		raw := "Content-Type: multipart/mixed; boundary=b\r\n\r\n" +
			"--b\r\nContent-Type: text/plain\r\n\r\na\r\n" +
			"--b\r\nContent-Type: text/plain\r\n\r\nb\r\n--b--\r\n"
		_, err := ParseWithLimits([]byte(raw), limits)
		if !errors.Is(err, ErrMIMETooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("nesting", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaxDepth = 0 // zero selects the default; use an explicit tiny limit below
		limits.MaxDepth = 1
		raw := "Content-Type: multipart/mixed; boundary=outer\r\n\r\n" +
			"--outer\r\nContent-Type: multipart/mixed; boundary=inner\r\n\r\n" +
			"--inner\r\nContent-Type: text/plain\r\n\r\nhello\r\n--inner--\r\n" +
			"--outer--\r\n"
		_, err := ParseWithLimits([]byte(raw), limits)
		if !errors.Is(err, ErrMIMETooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestParseWindowsAndLatin1Handling(t *testing.T) {
	parsed, err := Parse([]byte("Content-Type: text/plain; charset=iso-8859-1\r\n" + "Content-Transfer-Encoding: 8bit\r\n\r\ncaf\xe9"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if parsed.TextBody != "café" {
		t.Fatalf("text body = %q", parsed.TextBody)
	}
}

func ExampleParse() {
	parsed, err := Parse([]byte("Subject: hello\r\n\r\nworld\r\n"))
	if err != nil {
		panic(err)
	}
	fmt.Println(parsed.Subject, strings.TrimSpace(parsed.TextBody), len(parsed.Parts))
	// Output: hello world 1
}

func TestParseEncapsulatedMessage(t *testing.T) {
	raw := []byte("Content-Type: message/rfc822\r\n\r\n" +
		"From: nested@example.com\r\n" +
		"Date: Sat, 07 Nov 2026 12:00:00 +0000\r\n" +
		"Subject: nested message\r\n\r\n" +
		"nested body\r\n")
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(parsed.Parts) != 1 || parsed.Parts[0].Embedded == nil {
		t.Fatalf("expected parsed message/rfc822 part, got %#v", parsed.Parts)
	}
	if parsed.Parts[0].Embedded.Subject != "nested message" {
		t.Fatalf("embedded subject = %q", parsed.Parts[0].Embedded.Subject)
	}
}

func TestParseRejectsDuplicateSingletonHeaders(t *testing.T) {
	raw := []byte("Subject: first\r\nSubject: second\r\n\r\nbody\r\n")
	if _, err := Parse(raw); err == nil {
		t.Fatal("Parse() accepted duplicate Subject headers")
	}
}

func TestParseRequiresSenderWhenFromHasMultipleMailboxes(t *testing.T) {
	raw := []byte("From: first@example.com, second@example.com\r\n" +
		"Date: Sat, 07 Nov 2026 12:00:00 +0000\r\n\r\nbody\r\n")
	if _, err := Parse(raw); err == nil {
		t.Fatal("Parse() accepted multiple From mailboxes without Sender")
	}
}

func TestParseEnforcesNestingAcrossEncapsulatedMessages(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxDepth = 1
	nestedMessage := "Content-Type: message/rfc822\r\n\r\n" +
		"Subject: level two\r\n\r\n" +
		"body\r\n"
	raw := "Content-Type: message/rfc822\r\n\r\n" + nestedMessage
	_, err := ParseWithLimits([]byte(raw), limits)
	if !errors.Is(err, ErrMIMETooLarge) {
		t.Fatalf("ParseWithLimits() error = %v, want ErrMIMETooLarge", err)
	}
}

func TestParseRejectsInvalidMailboxInAddressHeaders(t *testing.T) {
	raw := []byte("From: sender@example.com\r\n" +
		"To: recipient@@example.net\r\n\r\nbody\r\n")
	if _, err := Parse(raw); err == nil {
		t.Fatal("Parse() accepted an invalid mailbox in To header")
	}
}

func TestParsePreservesQuotedLocalPartsInAddressHeaders(t *testing.T) {
	raw := []byte("From: \"foo bar\"@example.com\r\n" +
		"To: Recipient <recipient@example.net>\r\n\r\nbody\r\n")
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(parsed.From) != 1 || parsed.From[0].Address != `"foo bar"@example.com` {
		t.Fatalf("From = %#v, quoted local-part was not preserved", parsed.From)
	}
}

func TestParseToCcBccAndReplyToHeaders(t *testing.T) {
	raw := []byte("From: Sender <sender@example.com>\r\n" +
		"To: Primary <primary@example.net>, second@example.net\r\n" +
		"Cc: Copy <copy@example.org>\r\n" +
		"Bcc: Blind <blind@example.com>\r\n" +
		"Reply-To: Replies <replies@example.net>\r\n" +
		"Subject: Address fields\r\n\r\nhello\r\n")

	parsed, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(parsed.To) != 2 || parsed.To[0].Address != "primary@example.net" || parsed.To[1].Address != "second@example.net" {
		t.Fatalf("To = %#v", parsed.To)
	}
	if len(parsed.Cc) != 1 || parsed.Cc[0].Address != "copy@example.org" {
		t.Fatalf("Cc = %#v", parsed.Cc)
	}
	if len(parsed.Bcc) != 1 || parsed.Bcc[0].Address != "blind@example.com" {
		t.Fatalf("Bcc = %#v", parsed.Bcc)
	}
	if len(parsed.ReplyTo) != 1 || parsed.ReplyTo[0].Address != "replies@example.net" {
		t.Fatalf("Reply-To = %#v", parsed.ReplyTo)
	}
}

func TestParseAllowsEmptyBccHeader(t *testing.T) {
	parsed, err := Parse([]byte("From: sender@example.com\r\nBcc:\r\n\r\nbody\r\n"))
	if err != nil {
		t.Fatalf("Parse() rejected an empty Bcc header: %v", err)
	}
	if len(parsed.Bcc) != 0 {
		t.Fatalf("Bcc = %#v, want no addresses", parsed.Bcc)
	}
}

func TestParseRejectsInvalidMailboxInEachDestinationHeader(t *testing.T) {
	for _, header := range []string{"To", "Cc", "Bcc", "Reply-To"} {
		t.Run(header, func(t *testing.T) {
			raw := []byte("From: sender@example.com\r\n" + header + ": invalid@@example.net\r\n\r\nbody\r\n")
			if _, err := Parse(raw); err == nil {
				t.Fatalf("Parse() accepted invalid mailbox in %s", header)
			}
		})
	}
}

func TestStripBccHeadersRemovesFieldsAndFoldedLines(t *testing.T) {
	raw := []byte("From: sender@example.com\r\n" +
		"To: visible@example.net\r\n" +
		"Bcc: hidden-one@example.com,\r\n\t hidden-two@example.com\r\n" +
		"Subject: retained\r\n" +
		"Resent-Bcc: resent-hidden@example.org\r\n" +
		"Reply-To: replies@example.net\r\n\r\n" +
		"Body mentions Bcc: but this is not a header.\r\n")

	got, err := StripBccHeaders(raw)
	if err != nil {
		t.Fatalf("StripBccHeaders() error = %v", err)
	}
	want := []byte("From: sender@example.com\r\n" +
		"To: visible@example.net\r\n" +
		"Subject: retained\r\n" +
		"Reply-To: replies@example.net\r\n\r\n" +
		"Body mentions Bcc: but this is not a header.\r\n")
	if string(got) != string(want) {
		t.Fatalf("sanitized message = %q, want %q", got, want)
	}
	if strings.Contains(string(got), "hidden-one@example.com") || strings.Contains(string(got), "resent-hidden@example.org") {
		t.Fatal("sanitized message leaked a blind-copy address")
	}
}

func TestStripBccHeadersPreservesMessageWithoutBcc(t *testing.T) {
	raw := []byte("From: sender@example.com\r\nTo: recipient@example.net\r\n\r\nbody\r\n")
	got, err := StripBccHeaders(raw)
	if err != nil {
		t.Fatalf("StripBccHeaders() error = %v", err)
	}
	if string(got) != string(raw) {
		t.Fatalf("message changed without Bcc header: got %q want %q", got, raw)
	}
}

func TestStripBccHeadersRejectsMissingSeparator(t *testing.T) {
	if _, err := StripBccHeaders([]byte("Bcc: hidden@example.com\r\n")); err == nil {
		t.Fatal("StripBccHeaders() accepted a message without header/body separator")
	}
}
