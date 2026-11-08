// Command delivery-benchmark runs an end-to-end benchmark against a local mock
// SMTP receiver. It submits messages through MailForge's authenticated SMTP
// submission service, then observes recipient outcomes in PostgreSQL.
//
// It deliberately uses a reserved .test recipient domain and the delivery
// worker's SMTP relay override so benchmark traffic never reaches real MX hosts.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"mailforge/internal/database"
	"mailforge/internal/deliverybench"
	"mailforge/internal/emailaddr"
)

type config struct {
	SubmissionHost     string
	SubmissionPort     int
	Total              int
	Concurrency        int
	DeliveryWorkers    int
	AuthParallelism    int
	From               string
	Username           string
	Password           string
	TLSCAFile          string
	RecipientDomain    string
	MockAddr           string
	MockDelay          time.Duration
	RejectEvery        int64
	RejectCode         int
	DropAfterDATAEvery int64
	MessageBytes       int
	Timeout            time.Duration
	DeliveryTimeout    time.Duration
	PollInterval       time.Duration
	JSONOut            string
}

type submissionResult struct {
	ID                     int
	Duration               time.Duration
	ConnectionDuration     time.Duration
	AuthenticationDuration time.Duration
	TransactionDuration    time.Duration
	Err                    error
}

type submissionSummary struct {
	Requested        int
	Accepted         int
	Failed           int
	Duration         time.Duration
	Latencies        []time.Duration
	ConnectionTimes  []time.Duration
	AuthTimes        []time.Duration
	TransactionTimes []time.Duration
	Errors           []string
}

type deliverySnapshot struct {
	Total      int64 `json:"total"`
	Pending    int64 `json:"pending"`
	Delivering int64 `json:"delivering"`
	Deferred   int64 `json:"deferred"`
	Delivered  int64 `json:"delivered"`
	Bounced    int64 `json:"bounced"`
	Unknown    int64 `json:"unknown"`
}

func (s deliverySnapshot) complete(expected int) bool {
	if s.Total < int64(expected) {
		return false
	}
	return s.Pending == 0 && s.Delivering == 0 && s.Deferred == 0
}

type latencySummary struct {
	Samples int     `json:"samples"`
	Average float64 `json:"average_ms"`
	P50     float64 `json:"p50_ms"`
	P95     float64 `json:"p95_ms"`
	P99     float64 `json:"p99_ms"`
}

type benchmarkReport struct {
	RunID                  string                      `json:"run_id"`
	StartedAt              time.Time                   `json:"started_at"`
	RecipientPattern       string                      `json:"recipient_pattern"`
	Requested              int                         `json:"requested"`
	SubmissionConcurrency  int                         `json:"submission_concurrency"`
	DeliveryWorkerCount    int                         `json:"delivery_worker_count_label"`
	MessageBodyBytes       int                         `json:"message_body_bytes"`
	MockDelay              string                      `json:"mock_delay"`
	MockRejectEvery        int64                       `json:"mock_reject_every"`
	MockRejectCode         int                         `json:"mock_reject_code"`
	MockDropAfterDATAEvery int64                       `json:"mock_drop_after_data_every"`
	Submitted              int                         `json:"submitted"`
	SubmissionFailed       int                         `json:"submission_failed"`
	SubmissionSeconds      float64                     `json:"submission_seconds"`
	SubmissionRate         float64                     `json:"submission_messages_per_second"`
	SubmissionLatency      latencySummary              `json:"submission_latency"`
	ConnectionLatency      latencySummary              `json:"connection_latency"`
	AuthenticationLatency  latencySummary              `json:"authentication_latency"`
	SMTPTransactionLatency latencySummary              `json:"smtp_transaction_latency"`
	Delivery               deliverySnapshot            `json:"delivery"`
	DeliverySeconds        float64                     `json:"delivery_seconds"`
	DeliveryRate           float64                     `json:"delivered_messages_per_second"`
	EndToEndLatency        latencySummary              `json:"end_to_end_latency"`
	MockSMTP               deliverybench.MockSMTPStats `json:"mock_smtp"`
	MockTrafficObserved    bool                        `json:"mock_traffic_observed"`
	DeliveryComplete       bool                        `json:"delivery_complete"`
}

type smtpSession struct {
	client    *smtp.Client
	conn      net.Conn
	stopClose func() bool
}

func (s *smtpSession) close(quit bool) {
	if s == nil {
		return
	}
	if s.stopClose != nil {
		s.stopClose()
	}
	if s.client != nil {
		if quit {
			_ = s.conn.SetDeadline(time.Now().Add(2 * time.Second))
			_ = s.client.Quit()
		} else {
			_ = s.client.Close()
		}
		return
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func main() {
	os.Exit(run())
}

func run() int {
	cfg := config{}
	cfg.Username = os.Getenv("SMTP_USERNAME")
	cfg.Password = os.Getenv("SMTP_PASSWORD")
	cfg.From = os.Getenv("SMTP_FROM")
	if cfg.From == "" {
		cfg.From = "sender@example.com"
	}
	cfg.TLSCAFile = os.Getenv("SMTP_TLS_CA_FILE")

	flag.StringVar(&cfg.SubmissionHost, "host", "localhost", "MailForge SMTP submission hostname (must match TLS certificate)")
	flag.IntVar(&cfg.SubmissionPort, "port", 587, "MailForge SMTP submission port")
	flag.StringVar(&cfg.Username, "username", cfg.Username, "SMTP AUTH username (or SMTP_USERNAME)")
	flag.IntVar(&cfg.Total, "total", 100, "number of messages to submit")
	flag.IntVar(&cfg.Concurrency, "concurrency", 10, "number of persistent authenticated SMTP submission sessions")
	flag.IntVar(&cfg.DeliveryWorkers, "workers", 10, "reported total delivery worker concurrency; must match running workers")
	flag.IntVar(&cfg.AuthParallelism, "auth-parallelism", 0, "maximum concurrent SMTP AUTH handshakes (0 selects min(concurrency, 10))")
	flag.StringVar(&cfg.From, "from", cfg.From, "envelope sender; must match the SMTP credential's authorized email")
	flag.StringVar(&cfg.Password, "password", cfg.Password, "SMTP AUTH password (prefer SMTP_PASSWORD environment variable)")
	flag.StringVar(&cfg.TLSCAFile, "tls-ca", cfg.TLSCAFile, "PEM CA certificate for the submission server")
	flag.StringVar(&cfg.RecipientDomain, "recipient-domain", "bench.mailforge.test", "reserved .test domain for synthetic recipients")
	flag.StringVar(&cfg.MockAddr, "mock-addr", "127.0.0.1:2526", "local mock SMTP bind address")
	flag.DurationVar(&cfg.MockDelay, "mock-delay", 0, "delay before mock SMTP returns final DATA 250 response")
	flag.Int64Var(&cfg.RejectEvery, "mock-reject-every", 0, "reject every Nth RCPT (0 disables rejection)")
	flag.IntVar(&cfg.RejectCode, "mock-reject-code", 451, "mock RCPT rejection code: 451 temporary or 550 permanent")
	flag.Int64Var(&cfg.DropAfterDATAEvery, "mock-drop-after-data-every", 0, "accept DATA then drop connection before final 250 every Nth message (0 disables)")
	flag.IntVar(&cfg.MessageBytes, "message-bytes", 1024, "synthetic message body size in bytes")
	flag.DurationVar(&cfg.Timeout, "timeout", 15*time.Second, "SMTP connection and command timeout")
	flag.DurationVar(&cfg.DeliveryTimeout, "delivery-timeout", 2*time.Minute, "maximum time to wait for recipient delivery outcomes")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", 500*time.Millisecond, "PostgreSQL delivery status polling interval")
	flag.StringVar(&cfg.JSONOut, "json-out", "", "optional path to write the benchmark report as JSON")
	flag.Parse()
	if cfg.AuthParallelism == 0 {
		cfg.AuthParallelism = min(cfg.Concurrency, 10)
	}

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		return 2
	}

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "DATABASE_URL is required; delivery outcomes are read from PostgreSQL")
		return 1
	}
	if cfg.Username == "" || cfg.Password == "" {
		fmt.Fprintln(os.Stderr, "SMTP_USERNAME and SMTP_PASSWORD are required for authenticated submission")
		return 1
	}
	if err := emailaddr.ValidateMailbox(cfg.From); err != nil {
		fmt.Fprintf(os.Stderr, "invalid -from address: %v\n", err)
		return 1
	}
	if !strings.HasSuffix(strings.ToLower(cfg.RecipientDomain), ".test") {
		fmt.Fprintln(os.Stderr, "-recipient-domain must end in .test so benchmark messages cannot be routed to real MX hosts")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbCtx, dbCancel := context.WithTimeout(ctx, 10*time.Second)
	pool, err := database.NewPostgresPool(dbCtx, database.PostgresConfig{
		DatabaseURL: databaseURL,
		MaxConns:    4,
		MinConns:    1,
	})
	dbCancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to PostgreSQL: %v\n", err)
		return 1
	}
	defer pool.Close()

	mock, err := deliverybench.StartMockSMTPServer(cfg.MockAddr, deliverybench.MockSMTPConfig{
		Delay:              cfg.MockDelay,
		RejectEvery:        cfg.RejectEvery,
		RejectCode:         cfg.RejectCode,
		DropAfterDATAEvery: cfg.DropAfterDATAEvery,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start local mock SMTP server: %v\n", err)
		return 1
	}
	defer func() {
		if err := mock.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: close mock SMTP server: %v\n", err)
		}
	}()

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	pattern := fmt.Sprintf("mfbench-%s-%%@%s", runID, strings.ToLower(cfg.RecipientDomain))
	startedAt := time.Now().UTC()
	benchmarkStarted := time.Now()

	fmt.Println("============================================================")
	fmt.Println("             MAILFORGE END-TO-END DELIVERY BENCHMARK")
	fmt.Println("============================================================")
	fmt.Printf("Run ID:                 %s\n", runID)
	fmt.Printf("Submission endpoint:    %s:%d (STARTTLS + AUTH)\n", cfg.SubmissionHost, cfg.SubmissionPort)
	fmt.Printf("Mock SMTP receiver:     %s\n", mock.Addr())
	fmt.Printf("Recipient pattern:      %s\n", pattern)
	fmt.Printf("Messages:               %d\n", cfg.Total)
	fmt.Printf("Submission sessions:    %d\n", cfg.Concurrency)
	fmt.Printf("Delivery worker label:  %d\n", cfg.DeliveryWorkers)
	fmt.Printf("AUTH parallelism:       %d\n", cfg.AuthParallelism)
	fmt.Printf("Message body size:      %d bytes\n", cfg.MessageBytes)
	fmt.Printf("Mock DATA delay:        %s\n", cfg.MockDelay)
	fmt.Printf("Mock RCPT rejection:    every=%d code=%d\n", cfg.RejectEvery, cfg.RejectCode)
	fmt.Printf("Mock final DATA drops:  every=%d\n", cfg.DropAfterDATAEvery)
	fmt.Println("Safety:                 recipient domain is reserved .test; worker relay must point to this mock server")
	fmt.Println("------------------------------------------------------------")

	submitCtx, submitCancel := context.WithCancel(ctx)
	submission := runSubmissions(submitCtx, cfg, runID)
	submitCancel()

	fmt.Printf("\nSubmission finished: accepted=%d failed=%d duration=%s rate=%.2f messages/sec\n",
		submission.Accepted,
		submission.Failed,
		submission.Duration.Round(time.Millisecond),
		float64(submission.Accepted)/max(submission.Duration.Seconds(), 0.001),
	)
	if len(submission.Errors) > 0 {
		fmt.Println("First submission errors:")
		for _, message := range submission.Errors {
			fmt.Printf("  - %s\n", message)
		}
	}

	var snapshot deliverySnapshot
	var deliveryComplete bool
	if submission.Accepted > 0 && ctx.Err() == nil {
		fmt.Printf("\nWaiting up to %s for recipient outcomes...\n", cfg.DeliveryTimeout)
		snapshot, deliveryComplete = waitForDelivery(ctx, pool, pattern, submission.Accepted, cfg.DeliveryTimeout, cfg.PollInterval)
	} else if submission.Accepted == 0 {
		fmt.Println("No messages were accepted by the submission server; skipping delivery wait.")
	}

	latencies, latencyErr := readEndToEndLatencies(ctx, pool, pattern)
	if latencyErr != nil {
		fmt.Fprintf(os.Stderr, "warning: query delivery latencies: %v\n", latencyErr)
	}

	elapsed := time.Since(benchmarkStarted)
	mockStats := mock.Stats()
	mockTrafficObserved := mockStats.AcceptedRecipients+mockStats.RejectedRecipients > 0 || mockStats.MessagesOverLimit > 0
	report := benchmarkReport{
		RunID:                  runID,
		StartedAt:              startedAt,
		RecipientPattern:       pattern,
		Requested:              cfg.Total,
		SubmissionConcurrency:  cfg.Concurrency,
		DeliveryWorkerCount:    cfg.DeliveryWorkers,
		MessageBodyBytes:       cfg.MessageBytes,
		MockDelay:              cfg.MockDelay.String(),
		MockRejectEvery:        cfg.RejectEvery,
		MockRejectCode:         cfg.RejectCode,
		MockDropAfterDATAEvery: cfg.DropAfterDATAEvery,
		Submitted:              submission.Accepted,
		SubmissionFailed:       submission.Failed,
		SubmissionSeconds:      submission.Duration.Seconds(),
		SubmissionRate:         float64(submission.Accepted) / max(submission.Duration.Seconds(), 0.001),
		SubmissionLatency:      summarizeDurations(submission.Latencies),
		ConnectionLatency:      summarizeDurations(submission.ConnectionTimes),
		AuthenticationLatency:  summarizeDurations(submission.AuthTimes),
		SMTPTransactionLatency: summarizeDurations(submission.TransactionTimes),
		Delivery:               snapshot,
		DeliverySeconds:        elapsed.Seconds(),
		DeliveryRate:           float64(snapshot.Delivered) / max(elapsed.Seconds(), 0.001),
		EndToEndLatency:        summarizeDurations(latencies),
		MockSMTP:               mockStats,
		MockTrafficObserved:    mockTrafficObserved,
		DeliveryComplete:       deliveryComplete,
	}

	printReport(report, submission)
	if cfg.JSONOut != "" {
		if err := writeJSONReport(cfg.JSONOut, report); err != nil {
			fmt.Fprintf(os.Stderr, "warning: write JSON report: %v\n", err)
		} else {
			fmt.Printf("\nJSON report written to %s\n", cfg.JSONOut)
		}
	}

	if ctx.Err() != nil {
		return 1
	}
	if submission.Accepted > 0 && !mockTrafficObserved {
		fmt.Fprintln(os.Stderr, "no recipient traffic reached the mock SMTP server; check SMTP_RELAY_HOST, SMTP_RELAY_PORT, and SMTP_RELAY_TLS_MODE on the delivery workers")
		return 1
	}
	if submission.Failed > 0 || !deliveryComplete {
		return 1
	}
	return 0
}

func validateConfig(cfg config) error {
	if strings.TrimSpace(cfg.SubmissionHost) == "" || cfg.SubmissionPort < 1 || cfg.SubmissionPort > 65535 {
		return fmt.Errorf("submission host and a valid port are required")
	}
	if cfg.Total < 1 || cfg.Total > 1_000_000 {
		return fmt.Errorf("-total must be between 1 and 1000000")
	}
	if cfg.Concurrency < 1 || cfg.Concurrency > 1000 {
		return fmt.Errorf("-concurrency must be between 1 and 1000")
	}
	if cfg.DeliveryWorkers < 1 || cfg.DeliveryWorkers > 10000 {
		return fmt.Errorf("-workers must be between 1 and 10000")
	}
	if cfg.AuthParallelism < 1 || cfg.AuthParallelism > cfg.Concurrency {
		return fmt.Errorf("-auth-parallelism must be between 1 and -concurrency")
	}
	if cfg.MessageBytes < 1 || cfg.MessageBytes > 1<<20 {
		return fmt.Errorf("-message-bytes must be between 1 and 1048576")
	}
	if cfg.Timeout <= 0 || cfg.DeliveryTimeout <= 0 || cfg.PollInterval <= 0 {
		return fmt.Errorf("timeouts and poll interval must be positive")
	}
	if cfg.MockAddr == "" || cfg.RecipientDomain == "" {
		return fmt.Errorf("mock address and recipient domain are required")
	}
	if cfg.RejectEvery < 0 || cfg.DropAfterDATAEvery < 0 || cfg.MockDelay < 0 {
		return fmt.Errorf("mock failure intervals and delays cannot be negative")
	}
	if cfg.RejectCode != 451 && cfg.RejectCode != 550 {
		return fmt.Errorf("-mock-reject-code must be 451 or 550")
	}
	return nil
}

func runSubmissions(ctx context.Context, cfg config, runID string) submissionSummary {
	jobs := make(chan int)
	results := make(chan submissionResult, cfg.Total)
	authGate := make(chan struct{}, cfg.AuthParallelism)
	var workers sync.WaitGroup

	started := time.Now()
	workers.Add(cfg.Concurrency)
	for worker := 0; worker < cfg.Concurrency; worker++ {
		go func() {
			defer workers.Done()
			var session *smtpSession
			defer func() { session.close(true) }()

			for id := range jobs {
				result := submissionResult{ID: id}
				messageStarted := time.Now()
				if session == nil {
					var err error
					connectStarted := time.Now()
					session, result.AuthenticationDuration, err = openSubmissionSession(ctx, cfg, authGate)
					result.ConnectionDuration = time.Since(connectStarted) - result.AuthenticationDuration
					if err != nil {
						result.Err = err
						session = nil
					}
				}
				if result.Err == nil {
					transactionStarted := time.Now()
					result.Err = submitOne(ctx, session, cfg, runID, id)
					result.TransactionDuration = time.Since(transactionStarted)
					if result.Err != nil {
						session.close(false)
						session = nil
					}
				}
				result.Duration = time.Since(messageStarted)
				select {
				case results <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for id := 1; id <= cfg.Total; id++ {
			select {
			case jobs <- id:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	summary := submissionSummary{Requested: cfg.Total}
	for result := range results {
		summary.Latencies = append(summary.Latencies, result.Duration)
		if result.ConnectionDuration > 0 {
			summary.ConnectionTimes = append(summary.ConnectionTimes, result.ConnectionDuration)
		}
		if result.AuthenticationDuration > 0 {
			summary.AuthTimes = append(summary.AuthTimes, result.AuthenticationDuration)
		}
		if result.TransactionDuration > 0 {
			summary.TransactionTimes = append(summary.TransactionTimes, result.TransactionDuration)
		}
		if result.Err != nil {
			summary.Failed++
			if len(summary.Errors) < 20 {
				summary.Errors = append(summary.Errors, fmt.Sprintf("message %d: %v", result.ID, result.Err))
			}
		} else {
			summary.Accepted++
		}
	}
	summary.Duration = time.Since(started)
	return summary
}

func openSubmissionSession(ctx context.Context, cfg config, authGate chan struct{}) (*smtpSession, time.Duration, error) {
	var authDuration time.Duration
	address := net.JoinHostPort(cfg.SubmissionHost, fmt.Sprint(cfg.SubmissionPort))
	dialer := net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, authDuration, fmt.Errorf("connect to submission server: %w", err)
	}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	fail := func(err error) (*smtpSession, time.Duration, error) {
		stopClose()
		_ = conn.Close()
		return nil, authDuration, err
	}
	if err := conn.SetDeadline(time.Now().Add(cfg.Timeout)); err != nil {
		return fail(fmt.Errorf("set SMTP deadline: %w", err))
	}
	client, err := smtp.NewClient(conn, cfg.SubmissionHost)
	if err != nil {
		return fail(fmt.Errorf("SMTP greeting: %w", err))
	}
	if err := client.Hello("delivery-benchmark.mailforge.test"); err != nil {
		_ = client.Close()
		stopClose()
		return nil, authDuration, fmt.Errorf("EHLO: %w", err)
	}
	if ok, _ := client.Extension("STARTTLS"); !ok {
		_ = client.Close()
		stopClose()
		return nil, authDuration, errors.New("submission server did not advertise STARTTLS; refusing plaintext authentication")
	}
	tlsConfig, err := benchmarkTLSConfig(cfg)
	if err != nil {
		_ = client.Close()
		stopClose()
		return nil, authDuration, err
	}
	if err := client.StartTLS(tlsConfig); err != nil {
		_ = client.Close()
		stopClose()
		return nil, authDuration, fmt.Errorf("STARTTLS: %w", err)
	}

	select {
	case authGate <- struct{}{}:
	case <-ctx.Done():
		_ = client.Close()
		stopClose()
		return nil, authDuration, ctx.Err()
	}
	authStarted := time.Now()
	auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.SubmissionHost)
	authErr := client.Auth(auth)
	authDuration = time.Since(authStarted)
	<-authGate
	if authErr != nil {
		_ = client.Close()
		stopClose()
		return nil, authDuration, fmt.Errorf("SMTP AUTH: %w", authErr)
	}
	if err := conn.SetDeadline(time.Now().Add(cfg.Timeout)); err != nil {
		_ = client.Close()
		stopClose()
		return nil, authDuration, fmt.Errorf("set SMTP deadline after AUTH: %w", err)
	}
	return &smtpSession{client: client, conn: conn, stopClose: stopClose}, authDuration, nil
}

func benchmarkTLSConfig(cfg config) (*tls.Config, error) {
	result := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.SubmissionHost,
	}
	if cfg.TLSCAFile == "" {
		return result, nil
	}
	certPEM, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA file: %w", err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(certPEM) {
		return nil, fmt.Errorf("no certificates found in TLS CA file %q", cfg.TLSCAFile)
	}
	result.RootCAs = roots
	return result, nil
}

func submitOne(ctx context.Context, session *smtpSession, cfg config, runID string, id int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := session.conn.SetDeadline(time.Now().Add(cfg.Timeout)); err != nil {
		return fmt.Errorf("set message deadline: %w", err)
	}
	recipient := fmt.Sprintf("mfbench-%s-%06d@%s", runID, id, strings.ToLower(cfg.RecipientDomain))
	if err := session.client.Mail(cfg.From); err != nil {
		return fmt.Errorf("MAIL FROM: %w", err)
	}
	if err := session.client.Rcpt(recipient); err != nil {
		return fmt.Errorf("RCPT TO: %w", err)
	}
	writer, err := session.client.Data()
	if err != nil {
		return fmt.Errorf("DATA: %w", err)
	}
	body := wrapBenchmarkBody(strings.Repeat("x", cfg.MessageBytes))
	message := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: MailForge delivery benchmark %s/%06d\r\nMessage-ID: <mfbench-%s-%06d@mailforge.local>\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n",
		cfg.From,
		recipient,
		runID,
		id,
		runID,
		id,
		body,
	)
	if _, err := io.WriteString(writer, message); err != nil {
		return fmt.Errorf("write DATA: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("finish DATA: %w", err)
	}
	return nil
}

// wrapBenchmarkBody keeps generated SMTP body lines below the protocol line
// limit. MailForge includes CRLF in its 1,000-byte line limit, while RFC-style
// MIME text commonly wraps at 76 characters. The benchmark body is ASCII, so
// byte-based wrapping preserves the requested body byte count exactly.
func wrapBenchmarkBody(body string) string {
	const lineWidth = 76
	if len(body) <= lineWidth {
		return body
	}

	var wrapped strings.Builder
	wrapped.Grow(len(body) + (len(body)/lineWidth)*2)
	for start := 0; start < len(body); start += lineWidth {
		if start > 0 {
			wrapped.WriteString("\r\n")
		}
		end := min(start+lineWidth, len(body))
		wrapped.WriteString(body[start:end])
	}
	return wrapped.String()
}

func waitForDelivery(
	ctx context.Context,
	pool *pgxpool.Pool,
	pattern string,
	expected int,
	timeout time.Duration,
	pollInterval time.Duration,
) (deliverySnapshot, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	lastLog := time.Now()
	var latest deliverySnapshot
	for {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := pool.QueryRow(queryCtx, `
			SELECT
				COUNT(*)::bigint,
				COUNT(*) FILTER (WHERE status = 'pending')::bigint,
				COUNT(*) FILTER (WHERE status = 'delivering')::bigint,
				COUNT(*) FILTER (WHERE status = 'deferred')::bigint,
				COUNT(*) FILTER (WHERE status = 'delivered')::bigint,
				COUNT(*) FILTER (WHERE status = 'bounced')::bigint,
				COUNT(*) FILTER (WHERE status = 'unknown')::bigint
			FROM recipients
			WHERE email LIKE $1
		`, pattern).Scan(
			&latest.Total,
			&latest.Pending,
			&latest.Delivering,
			&latest.Deferred,
			&latest.Delivered,
			&latest.Bounced,
			&latest.Unknown,
		)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: poll delivery outcomes: %v\n", err)
		} else if latest.complete(expected) {
			return latest, true
		}
		if time.Since(lastLog) >= 5*time.Second {
			fmt.Printf("  observed=%d delivered=%d bounced=%d unknown=%d pending=%d deferred=%d\n",
				latest.Total, latest.Delivered, latest.Bounced, latest.Unknown, latest.Pending, latest.Deferred)
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return latest, false
		case <-deadline.C:
			return latest, false
		case <-ticker.C:
		}
	}
}

func readEndToEndLatencies(
	ctx context.Context,
	pool *pgxpool.Pool,
	pattern string,
) ([]time.Duration, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := pool.Query(queryCtx, `
		SELECT (EXTRACT(EPOCH FROM (r.delivered_at - m.created_at)) * 1000.0)::double precision
		FROM recipients r
		JOIN messages m ON m.id = r.message_id
		WHERE r.email LIKE $1
		  AND r.status = 'delivered'
		  AND r.delivered_at IS NOT NULL
		ORDER BY r.delivered_at
	`, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	latencies := make([]time.Duration, 0)
	for rows.Next() {
		var milliseconds float64
		if err := rows.Scan(&milliseconds); err != nil {
			return nil, err
		}
		latencies = append(latencies, time.Duration(milliseconds*float64(time.Millisecond)))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return latencies, nil
}

func summarizeDurations(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, value := range sorted {
		total += value
	}
	return latencySummary{
		Samples: len(sorted),
		Average: float64(total) / float64(len(sorted)) / float64(time.Millisecond),
		P50:     durationAt(sorted, 0.50).Seconds() * 1000,
		P95:     durationAt(sorted, 0.95).Seconds() * 1000,
		P99:     durationAt(sorted, 0.99).Seconds() * 1000,
	}
}

func durationAt(sorted []time.Duration, percentile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted))*percentile+0.999999) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func printReport(report benchmarkReport, submission submissionSummary) {
	fmt.Println("\n============================================================")
	fmt.Println("                    BENCHMARK RESULTS")
	fmt.Println("============================================================")
	fmt.Printf("Submitted:                 %d / %d\n", report.Submitted, report.Requested)
	fmt.Printf("Submission failures:       %d\n", report.SubmissionFailed)
	fmt.Printf("Submission duration:       %.3f sec\n", report.SubmissionSeconds)
	fmt.Printf("Submission throughput:     %.2f messages/sec\n", report.SubmissionRate)
	printLatency("Per-message submission", report.SubmissionLatency)
	printLatency("Connection setup", report.ConnectionLatency)
	printLatency("SMTP authentication", report.AuthenticationLatency)
	printLatency("SMTP transaction", report.SMTPTransactionLatency)

	fmt.Println("\nDelivery outcomes (PostgreSQL)")
	fmt.Printf("Observed recipients:       %d\n", report.Delivery.Total)
	fmt.Printf("Delivered:                 %d\n", report.Delivery.Delivered)
	fmt.Printf("Bounced:                   %d\n", report.Delivery.Bounced)
	fmt.Printf("Unknown:                   %d\n", report.Delivery.Unknown)
	fmt.Printf("Pending:                   %d\n", report.Delivery.Pending)
	fmt.Printf("Delivering:                %d\n", report.Delivery.Delivering)
	fmt.Printf("Deferred:                  %d\n", report.Delivery.Deferred)
	fmt.Printf("End-to-end throughput:     %.2f delivered/sec\n", report.DeliveryRate)
	printLatency("Message stored to delivered", report.EndToEndLatency)
	fmt.Printf("Delivery completion:       %t\n", report.DeliveryComplete)

	fmt.Println("\nMock SMTP receiver")
	fmt.Printf("Connections:               %d (active %d)\n", report.MockSMTP.Connections, report.MockSMTP.ActiveConnections)
	fmt.Printf("Accepted messages:         %d\n", report.MockSMTP.AcceptedMessages)
	fmt.Printf("Accepted recipients:       %d\n", report.MockSMTP.AcceptedRecipients)
	fmt.Printf("Rejected recipients:       %d\n", report.MockSMTP.RejectedRecipients)
	fmt.Printf("Dropped after DATA:        %d\n", report.MockSMTP.DroppedAfterDATA)
	fmt.Printf("Bytes received:            %d\n", report.MockSMTP.BytesReceived)
	fmt.Printf("Mock traffic observed:     %t\n", report.MockTrafficObserved)
	if len(submission.Errors) > 0 {
		fmt.Println("\nFirst submission errors:")
		for _, message := range submission.Errors {
			fmt.Printf("  - %s\n", message)
		}
	}
	fmt.Println("============================================================")
}

func printLatency(name string, summary latencySummary) {
	if summary.Samples == 0 {
		fmt.Printf("%-28s no samples\n", name+":")
		return
	}
	fmt.Printf("%-28s avg=%8.2fms p50=%8.2fms p95=%8.2fms p99=%8.2fms n=%d\n",
		name+":", summary.Average, summary.P50, summary.P95, summary.P99, summary.Samples)
}

func writeJSONReport(path string, report benchmarkReport) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
