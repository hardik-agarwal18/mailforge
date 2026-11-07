package message

import (
	"context"
	"errors"
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
