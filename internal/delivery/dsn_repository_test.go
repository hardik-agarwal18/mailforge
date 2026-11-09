package delivery

import "testing"

func TestSuppressionKeyPreservesLocalPartCaseAndNormalizesDomain(t *testing.T) {
	got, err := SuppressionKey(`Alice+tag@EXAMPLE.COM`)
	if err != nil {
		t.Fatal(err)
	}
	if got != `Alice+tag@example.com` {
		t.Fatalf("SuppressionKey() = %q", got)
	}
	lower, err := SuppressionKey(`alice+tag@example.com`)
	if err != nil {
		t.Fatal(err)
	}
	if got == lower {
		t.Fatal("case-sensitive local-parts collapsed")
	}
}
