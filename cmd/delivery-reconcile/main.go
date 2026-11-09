// Command delivery-reconcile records an auditable operator decision for an
// ambiguous recipient delivery outcome. It is intentionally separate from
// the read-only delivery-status command.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/delivery"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	flags := flag.NewFlagSet("delivery-reconcile", flag.ContinueOnError)
	flags.SetOutput(stderr)
	recipient := flags.String("recipient", "", "recipient UUID whose status is unknown (required)")
	decision := flags.String("decision", "", "operator decision: accepted or retry (required)")
	actor := flags.String("actor", "", "operator identity or ticket owner (required)")
	reason := flags.String("reason", "", "why this decision is justified (required)")
	evidence := flags.String("evidence", "", "ticket, remote MTA log reference, or other supporting evidence")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage:")
		fmt.Fprintln(stderr, "  DATABASE_URL=... go run ./cmd/delivery-reconcile --recipient UUID --decision accepted|retry --actor operator --reason '...' [--evidence '...']")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "accepted: record that the remote next-hop accepted the message.")
		fmt.Fprintln(stderr, "retry: authorize a new attempt only after verifying the remote server did not accept it.")
		fmt.Fprintln(stderr, "Both decisions require an audit identity and reason. Only recipients currently in unknown status can be changed.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments; use --recipient")
	}
	if strings.TrimSpace(*recipient) == "" || strings.TrimSpace(*decision) == "" || strings.TrimSpace(*actor) == "" || strings.TrimSpace(*reason) == "" {
		flags.Usage()
		return errors.New("--recipient, --decision, --actor and --reason are required")
	}
	databaseURL := strings.TrimSpace(getenv("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pool, err := database.NewPostgresPool(queryCtx, database.PostgresConfig{DatabaseURL: databaseURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer pool.Close()

	err = delivery.NewRepository(pool).ReconcileUnknownRecipient(queryCtx, delivery.UnknownReconciliation{
		RecipientID: *recipient,
		Decision:    *decision,
		Actor:       *actor,
		Reason:      *reason,
		Evidence:    *evidence,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Recipient %s reconciled: decision=%s actor=%s\n", strings.TrimSpace(*recipient), strings.TrimSpace(*decision), strings.TrimSpace(*actor))
	if strings.EqualFold(strings.TrimSpace(*decision), "retry") {
		fmt.Fprintln(stdout, "A new delivery event has been committed to the outbox. Delivery is not yet confirmed.")
	} else {
		fmt.Fprintln(stdout, "Recipient recorded as delivered based on operator-confirmed next-hop acceptance.")
	}
	return nil
}
