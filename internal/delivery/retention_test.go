package delivery

import "testing"

func TestCompareStreamIDs(t *testing.T) {
	tests := []struct {
		a        string
		b        string
		expected int
	}{
		{"100-0", "101-0", -1},
		{"101-0", "100-0", 1},
		{"100-1", "100-2", -1},
		{"100-2", "100-1", 1},
		{"100-1", "100-1", 0},
		{"0-0", "1-0", -1},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			got, err := compareStreamIDs(tt.a, tt.b)
			if err != nil {
				t.Fatal(err)
			}

			if got != tt.expected {
				t.Fatalf("expected %d, got %d", tt.expected, got)
			}
		})
	}
}

func TestCompareStreamIDsRejectsInvalidIDs(t *testing.T) {
	invalid := []string{
		"",
		"100",
		"abc-1",
		"100-abc",
		"100-1-extra",
	}

	for _, id := range invalid {
		t.Run(id, func(t *testing.T) {
			_, err := compareStreamIDs(id, "101-0")
			if err == nil {
				t.Fatalf("expected error for invalid ID %q", id)
			}
		})
	}
}
