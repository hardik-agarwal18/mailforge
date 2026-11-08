package message

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type recordingCreator struct {
	calls int
	msg   StoredMessage
}

func (r *recordingCreator) Create(_ context.Context, msg StoredMessage) (string, error) {
	r.calls++
	r.msg = msg
	return "stored-message-id", nil
}

func TestAcceptValidatesSenderAndRecipientsBeforePersistence(t *testing.T) {
	tests := []struct {
		name      string
		input     NewMessage
		wantError error
	}{
		{
			name: "invalid sender",
			input: NewMessage{
				Sender:     "sender@@example.com",
				Recipients: []string{"recipient@example.net"},
			},
			wantError: ErrInvalidSender,
		},
		{
			name: "invalid recipient",
			input: NewMessage{
				Sender:     "sender@example.com",
				Recipients: []string{"recipient@"},
			},
			wantError: ErrInvalidRecipient,
		},
		{
			name: "whitespace is not silently trimmed",
			input: NewMessage{
				Sender:     "sender@example.com",
				Recipients: []string{" recipient@example.net "},
			},
			wantError: ErrInvalidRecipient,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			creator := &recordingCreator{}
			service := NewService(creator)
			_, err := service.Accept(context.Background(), test.input)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("Accept() error = %v, want %v", err, test.wantError)
			}
			if creator.calls != 0 {
				t.Fatalf("repository Create called %d times for invalid envelope", creator.calls)
			}
		})
	}
}

func TestAcceptAllowsNullReversePathForDSN(t *testing.T) {
	creator := &recordingCreator{}
	service := NewService(creator)

	_, err := service.Accept(context.Background(), NewMessage{
		Sender:     "",
		Recipients: []string{"postmaster@example.net"},
		RawMessage: []byte("Subject: Delivery status\r\n\r\nFailure\r\n"),
	})
	if err != nil {
		t.Fatalf("Accept() rejected a valid null reverse-path: %v", err)
	}
	if creator.calls != 1 {
		t.Fatalf("repository Create calls = %d, want 1", creator.calls)
	}
	if creator.msg.Sender != "" {
		t.Fatalf("stored sender = %q, want null reverse-path", creator.msg.Sender)
	}
}

func TestAcceptPersistsValidEnvelope(t *testing.T) {
	creator := &recordingCreator{}
	service := NewService(creator)

	_, err := service.Accept(context.Background(), NewMessage{
		Sender:     `"sender name"@example.com`,
		Recipients: []string{"recipient+tag@example.net"},
		RawMessage: []byte("Subject: test\r\n\r\nhello\r\n"),
	})
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if creator.calls != 1 {
		t.Fatalf("repository Create calls = %d, want 1", creator.calls)
	}
}

func TestAcceptParsesMIMEBeforePersistence(t *testing.T) {
	creator := &recordingCreator{}
	service := NewService(creator)
	raw := []byte("From: Alice <alice@example.com>\r\n" +
		"To: Bob <bob@example.net>\r\n" +
		"Subject: Parsed message\r\n" +
		"Content-Type: text/plain; charset=utf-8\r\n" +
		"\r\nHello MailForge\r\n")

	stored, err := service.Accept(context.Background(), NewMessage{
		Sender: "alice@example.com", Recipients: []string{"bob@example.net"}, RawMessage: raw,
	})
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if creator.calls != 1 {
		t.Fatalf("repository Create calls = %d, want 1", creator.calls)
	}
	if creator.msg.Parsed == nil {
		t.Fatal("repository did not receive parsed MIME metadata")
	}
	if creator.msg.Parsed.Subject != "Parsed message" {
		t.Fatalf("parsed subject = %q", creator.msg.Parsed.Subject)
	}
	if creator.msg.Parsed.TextBody != "Hello MailForge\r\n" {
		t.Fatalf("parsed text body = %q", creator.msg.Parsed.TextBody)
	}
	if string(creator.msg.RawMessage) != string(raw) {
		t.Fatal("raw message was modified during MIME parsing")
	}
	if stored.Parsed == nil {
		t.Fatal("returned StoredMessage has no parsed MIME metadata")
	}
}

func TestAcceptRejectsMalformedMIMEBeforePersistence(t *testing.T) {
	creator := &recordingCreator{}
	service := NewService(creator)
	_, err := service.Accept(context.Background(), NewMessage{
		Sender:     "sender@example.com",
		Recipients: []string{"recipient@example.net"},
		RawMessage: []byte("Content-Type: invalid-type\r\n\r\nbody\r\n"),
	})
	if !errors.Is(err, ErrInvalidMessageFormat) {
		t.Fatalf("Accept() error = %v, want ErrInvalidMessageFormat", err)
	}
	if creator.calls != 0 {
		t.Fatalf("repository Create called %d times for invalid MIME", creator.calls)
	}
}

func TestAcceptRejectsMIMELimitViolation(t *testing.T) {
	creator := &recordingCreator{}
	service := NewService(creator)
	service.maxMessageSize = 12 << 20
	_, err := service.Accept(context.Background(), NewMessage{
		Sender:     "sender@example.com",
		Recipients: []string{"recipient@example.net"},
		RawMessage: []byte("Content-Type: text/plain; charset=utf-8\r\n\r\n" + strings.Repeat("x", 10*1024*1024+1)),
	})
	if !errors.Is(err, ErrMIMEMessageTooLarge) {
		t.Fatalf("Accept() error = %v, want ErrMIMEMessageTooLarge", err)
	}
	if creator.calls != 0 {
		t.Fatalf("repository Create called %d times for oversized MIME", creator.calls)
	}
}

func TestAcceptStoresToCcBccAndReplyToMetadata(t *testing.T) {
	creator := &recordingCreator{}
	service := NewService(creator)
	raw := []byte("From: sender@example.com\r\n" +
		"To: primary@example.net\r\n" +
		"Cc: copy@example.org\r\n" +
		"Bcc: blind@example.com\r\n" +
		"Reply-To: replies@example.net\r\n\r\nbody\r\n")

	stored, err := service.Accept(context.Background(), NewMessage{
		Sender: "sender@example.com", Recipients: []string{"primary@example.net", "copy@example.org", "blind@example.com"}, RawMessage: raw,
	})
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if stored.Parsed == nil {
		t.Fatal("stored message has no parsed metadata")
	}
	if len(stored.Parsed.To) != 1 || stored.Parsed.To[0].Address != "primary@example.net" {
		t.Fatalf("To metadata = %#v", stored.Parsed.To)
	}
	if len(stored.Parsed.Cc) != 1 || stored.Parsed.Cc[0].Address != "copy@example.org" {
		t.Fatalf("Cc metadata = %#v", stored.Parsed.Cc)
	}
	if len(stored.Parsed.Bcc) != 1 || stored.Parsed.Bcc[0].Address != "blind@example.com" {
		t.Fatalf("Bcc metadata = %#v", stored.Parsed.Bcc)
	}
	if len(stored.Parsed.ReplyTo) != 1 || stored.Parsed.ReplyTo[0].Address != "replies@example.net" {
		t.Fatalf("Reply-To metadata = %#v", stored.Parsed.ReplyTo)
	}
	if string(stored.RawMessage) != string(raw) {
		t.Fatal("original message was modified; raw storage must retain the Bcc header")
	}
}

func TestAcceptEnforcesAuthenticatedSubmissionIdentity(t *testing.T) {
	tests := []struct {
		name         string
		envelopeFrom string
		message      string
		wantError    bool
	}{
		{
			name:         "matching From",
			envelopeFrom: "sender@example.com",
			message:      "From: Sender <sender@EXAMPLE.COM>\r\n\r\nhello\r\n",
		},
		{
			name:         "missing From",
			envelopeFrom: "sender@example.com",
			message:      "Subject: no visible author\r\n\r\nhello\r\n",
			wantError:    true,
		},
		{
			name:         "spoofed From",
			envelopeFrom: "sender@example.com",
			message:      "From: ceo@another-company.com\r\n\r\nhello\r\n",
			wantError:    true,
		},
		{
			name:         "multiple From mailboxes",
			envelopeFrom: "sender@example.com",
			message:      "From: sender@example.com, attacker@example.net\r\nSender: sender@example.com\r\n\r\nhello\r\n",
			wantError:    true,
		},
		{
			name:         "spoofed Sender header",
			envelopeFrom: "sender@example.com",
			message:      "From: sender@example.com\r\nSender: attacker@example.net\r\n\r\nhello\r\n",
			wantError:    true,
		},
		{
			name:         "spoofed envelope sender",
			envelopeFrom: "attacker@example.net",
			message:      "From: sender@example.com\r\n\r\nhello\r\n",
			wantError:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			creator := &recordingCreator{}
			service := NewService(creator)
			_, err := service.Accept(context.Background(), NewMessage{
				Sender:             test.envelopeFrom,
				AuthenticatedEmail: "sender@example.com",
				Recipients:         []string{"recipient@example.net"},
				RawMessage:         []byte(test.message),
			})

			if test.wantError {
				if !errors.Is(err, ErrUnauthorizedSubmissionIdentity) {
					t.Fatalf("Accept() error = %v, want ErrUnauthorizedSubmissionIdentity", err)
				}
				if creator.calls != 0 {
					t.Fatalf("repository Create called %d times for unauthorized submission", creator.calls)
				}
				return
			}

			if err != nil {
				t.Fatalf("Accept() rejected authorized submission: %v", err)
			}
			if creator.calls != 1 {
				t.Fatalf("repository Create calls = %d, want 1", creator.calls)
			}
		})
	}
}
