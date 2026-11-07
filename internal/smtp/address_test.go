package smtp

import "testing"

func TestParsePathValidatesSenderAndRecipientMailboxes(t *testing.T) {
	tests := []struct {
		name   string
		arg    string
		prefix string
		want   string
		wantOK bool
	}{
		{name: "normal sender", arg: "FROM:<sender@example.com>", prefix: "FROM:", want: "sender@example.com", wantOK: true},
		{name: "plus-address sender", arg: "FROM:<sender+tag@example.co.uk>", prefix: "FROM:", want: "sender+tag@example.co.uk", wantOK: true},
		{name: "quoted local sender", arg: `FROM:<"user@name"@example.com>`, prefix: "FROM:", want: `"user@name"@example.com`, wantOK: true},
		{name: "null sender", arg: "FROM:<>", prefix: "FROM:", want: "", wantOK: true},
		{name: "normal recipient", arg: "TO:<recipient@example.net>", prefix: "TO:", want: "recipient@example.net", wantOK: true},
		{name: "null recipient rejected", arg: "TO:<>", prefix: "TO:", want: "", wantOK: false},
		{name: "multiple at sender", arg: "FROM:<sender@@example.com>", prefix: "FROM:", want: "", wantOK: false},
		{name: "empty domain recipient", arg: "TO:<recipient@>", prefix: "TO:", want: "", wantOK: false},
		{name: "consecutive dots recipient", arg: "TO:<first..last@example.net>", prefix: "TO:", want: "", wantOK: false},
		{name: "extra SMTP parameters not advertised", arg: "FROM:<sender@example.com> SIZE=100", prefix: "FROM:", want: "", wantOK: false},
		{name: "display name rejected", arg: "TO:<Name <recipient@example.net>>", prefix: "TO:", want: "", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := parsePath(test.arg, test.prefix)
			if got != test.want || ok != test.wantOK {
				t.Fatalf("parsePath(%q, %q) = (%q, %v), want (%q, %v)", test.arg, test.prefix, got, ok, test.want, test.wantOK)
			}
		})
	}
}
