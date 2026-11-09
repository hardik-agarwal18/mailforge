package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunRequiresExplicitReconciliationFields(t *testing.T) {
	var stdout, stderr bytes.Buffer
	getenv := func(string) string { return "" }
	err := run(context.Background(), nil, &stdout, &stderr, getenv)
	if err == nil || !strings.Contains(err.Error(), "--recipient, --decision, --actor and --reason are required") {
		t.Fatalf("run() error = %v, want required flags error", err)
	}
}

func TestRunRequiresDatabaseURLAfterRequiredFields(t *testing.T) {
	var stdout, stderr bytes.Buffer
	getenv := func(string) string { return "" }
	args := []string{"--recipient", "8a4fd9a1-9d66-4b9a-9e46-58c4b06e7350", "--decision", "accepted", "--actor", "operator@example.test", "--reason", "Confirmed in remote MTA logs"}
	err := run(context.Background(), args, &stdout, &stderr, getenv)
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("run() error = %v, want DATABASE_URL error", err)
	}
}
