// Command delivery-status displays the durable delivery state and event history
// for a message. The identifier may be the internal message UUID or the RFC
// Message-ID returned by the SMTP submission server.
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
	"text/tabwriter"
	"time"

	"mailforge/internal/database"
	"mailforge/internal/delivery"
)

const defaultEventLimit = 10

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	getenv func(string) string,
) error {
	flags := flag.NewFlagSet("delivery-status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	eventLimit := flags.Int("events", defaultEventLimit, "maximum recent delivery events to show per recipient (0 disables history; max 1000)")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage:")
		fmt.Fprintln(stderr, "  DATABASE_URL=... go run ./cmd/delivery-status [--events 10] <message-uuid-or-message-id>")
		fmt.Fprintln(stderr)
		fmt.Fprintln(stderr, "The identifier can be the internal message UUID or the RFC Message-ID returned by SMTP submission.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *eventLimit < 0 || *eventLimit > 1000 {
		return fmt.Errorf("--events must be between 0 and 1000")
	}
	if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" {
		flags.Usage()
		return fmt.Errorf("exactly one message identifier is required")
	}

	databaseURL := strings.TrimSpace(getenv("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pool, err := database.NewPostgresPool(queryCtx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    4,
		MinConns:    1,
	})
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer pool.Close()

	repository := delivery.NewRepository(pool)
	messageID, err := repository.ResolveMessageID(queryCtx, flags.Arg(0))
	if err != nil {
		if errors.Is(err, delivery.ErrMessageNotFound) {
			return fmt.Errorf("message %q was not found (check the UUID or Message-ID)", flags.Arg(0))
		}
		return err
	}

	summary, err := repository.GetMessageDeliverySummary(queryCtx, messageID)
	if err != nil {
		if errors.Is(err, delivery.ErrMessageNotFound) {
			return fmt.Errorf("message %q has no recipient delivery records", flags.Arg(0))
		}
		return err
	}
	recipients, err := repository.ListMessageRecipientDeliveryStatuses(queryCtx, messageID)
	if err != nil {
		return err
	}

	reports := make([]recipientReport, 0, len(recipients))
	for _, recipient := range recipients {
		report := recipientReport{Status: recipient, HistoryLoaded: *eventLimit > 0}
		if *eventLimit > 0 {
			events, err := repository.ListRecipientDeliveryEvents(queryCtx, recipient.RecipientID, *eventLimit)
			if err != nil {
				return fmt.Errorf("load delivery history for %s: %w", recipient.Email, err)
			}
			report.Events = events
		}
		reports = append(reports, report)
	}

	return writeReport(stdout, flags.Arg(0), summary, reports, time.Now())
}

type recipientReport struct {
	Status        delivery.RecipientDeliveryStatus
	Events        []delivery.DeliveryStatusEvent
	HistoryLoaded bool
}

func writeReport(
	out io.Writer,
	requestedIdentifier string,
	summary *delivery.MessageDeliverySummary,
	recipients []recipientReport,
	now time.Time,
) error {
	if summary == nil {
		return errors.New("delivery summary is missing")
	}
	fmt.Fprintln(out, "MAILFORGE DELIVERY REPORT")
	fmt.Fprintln(out, strings.Repeat("=", 78))
	fmt.Fprintf(out, "Lookup:          %s\n", requestedIdentifier)
	fmt.Fprintf(out, "Message UUID:    %s\n", summary.MessageID)
	fmt.Fprintf(out, "Overall status:  %s\n", overallStatus(summary))
	fmt.Fprintf(out, "Recipients:      %d total | %d delivered | %d deferred | %d pending | %d delivering | %d bounced | %d unknown | %d suppressed\n",
		summary.Total,
		summary.Delivered,
		summary.Deferred,
		summary.Pending,
		summary.Delivering,
		summary.Bounced,
		summary.Unknown,
		summary.Suppressed,
	)

	fmt.Fprintln(out)
	fmt.Fprintln(out, "RECIPIENT STATUS")
	writer := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "RECIPIENT\tSTATUS\tATTEMPTS\tLAST SMTP\tNEXT RETRY")
	for _, recipient := range recipients {
		status := recipient.Status
		fmt.Fprintf(writer, "%s\t%s\t%d\t%s\t%s\n",
			status.Email,
			strings.ToUpper(status.Status),
			status.Attempts,
			formatSMTP(status),
			formatTimestamp(status.NextAttemptAt, now.Location()),
		)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("write recipient table: %w", err)
	}

	for _, recipient := range recipients {
		status := recipient.Status
		if status.LastError != "" {
			fmt.Fprintf(out, "\nLast error for %s: %s\n", status.Email, oneLine(status.LastError, 240))
		}
		if status.LastEnhancedStatus != "" || status.LastSMTPStage != "" {
			fmt.Fprintf(out, "Last SMTP detail for %s: enhanced=%s stage=%s\n",
				status.Email,
				valueOrDash(status.LastEnhancedStatus),
				valueOrDash(status.LastSMTPStage),
			)
		}
		if status.LastAttemptAt != nil {
			fmt.Fprintf(out, "Last attempt for %s: %s\n", status.Email, status.LastAttemptAt.In(now.Location()).Format(time.RFC3339))
		}
		if status.DeliveredAt != nil {
			fmt.Fprintf(out, "Delivered at for %s: %s\n", status.Email, status.DeliveredAt.In(now.Location()).Format(time.RFC3339))
		}
		if status.BouncedAt != nil {
			fmt.Fprintf(out, "Bounced at for %s: %s\n", status.Email, status.BouncedAt.In(now.Location()).Format(time.RFC3339))
		}
	}

	fmt.Fprintln(out)
	fmt.Fprintln(out, "DELIVERY HISTORY")
	for _, recipient := range recipients {
		fmt.Fprintf(out, "\n%s\n", recipient.Status.Email)
		if !recipient.HistoryLoaded {
			fmt.Fprintln(out, "  Delivery history not requested (--events 0).")
			continue
		}
		if len(recipient.Events) == 0 {
			fmt.Fprintln(out, "  No delivery history recorded.")
			continue
		}
		for _, event := range recipient.Events {
			from := valueOrDash(event.FromStatus)
			line := fmt.Sprintf("  %s  %s -> %s  [%s]",
				event.CreatedAt.In(now.Location()).Format("2006-01-02 15:04:05"),
				from,
				event.ToStatus,
				valueOrDash(event.EventType),
			)
			if event.SMTPCode != nil {
				line += fmt.Sprintf(" SMTP=%d", *event.SMTPCode)
			}
			if event.EnhancedStatusCode != "" {
				line += " enhanced=" + event.EnhancedStatusCode
			}
			if event.SMTPStage != "" {
				line += " stage=" + event.SMTPStage
			}
			if event.RemoteHost != "" {
				line += " host=" + event.RemoteHost
			}
			if event.TLSUsed {
				line += " TLS=" + valueOrDash(event.TLSVersion)
			}
			if event.Diagnostic != "" {
				line += " — " + oneLine(event.Diagnostic, 200)
			}
			fmt.Fprintln(out, line)
		}
	}
	return nil
}

func overallStatus(summary *delivery.MessageDeliverySummary) string {
	switch {
	case summary == nil || summary.Total == 0:
		return "NO RECIPIENTS"
	case summary.Delivered == summary.Total:
		return "DELIVERED"
	case summary.Delivered > 0:
		return "PARTIALLY DELIVERED"
	case summary.Delivering > 0:
		return "DELIVERING"
	case summary.Pending > 0:
		return "PENDING"
	case summary.Deferred > 0:
		return "DEFERRED"
	case summary.Unknown > 0:
		return "UNKNOWN — INVESTIGATION REQUIRED"
	case summary.Bounced+summary.Suppressed == summary.Total:
		return "FAILED"
	default:
		return "MIXED"
	}
}

func formatSMTP(status delivery.RecipientDeliveryStatus) string {
	if status.LastSMTPCode == nil {
		return "-"
	}
	if status.LastEnhancedStatus != "" {
		return fmt.Sprintf("%d (%s)", *status.LastSMTPCode, status.LastEnhancedStatus)
	}
	return fmt.Sprintf("%d", *status.LastSMTPCode)
}

func formatTimestamp(value *time.Time, location *time.Location) string {
	if value == nil {
		return "-"
	}
	return value.In(location).Format("2006-01-02 15:04:05 MST")
}

func valueOrDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return oneLine(value, 100)
}

func oneLine(value string, maxRunes int) string {
	value = strings.Join(strings.Fields(value), " ")
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	if maxRunes <= 3 {
		return string(runes[:maxRunes])
	}
	return string(runes[:maxRunes-3]) + "..."
}
