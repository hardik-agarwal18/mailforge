package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Host        string
	Port        int
	Total       int
	Concurrency int
	From        string
	Username    string
	Password    string
	StartTLS    bool
	TLSCAFile   string
	Timeout     time.Duration
}

type Result struct {
	ID                 int
	Duration           time.Duration
	AuthDuration       time.Duration
	SubmissionDuration time.Duration
	AuthAttempted      bool
	Authenticated      bool
	Err                error
}

type Metrics struct {
	Total                   int
	Successful              int64
	Failed                  int64
	AuthenticationAttempts  int64
	AuthenticationSuccesses int64
	Duration                time.Duration
	Latencies               []time.Duration
	AuthLatencies           []time.Duration
	SubmissionLatencies     []time.Duration
	Errors                  []error
}

func newTLSConfig(cfg Config) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.Host,
	}

	if cfg.TLSCAFile == "" {
		return tlsConfig, nil
	}

	certPEM, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA certificate: %w", err)
	}

	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}

	if !roots.AppendCertsFromPEM(certPEM) {
		return nil, fmt.Errorf("no certificates found in %s", cfg.TLSCAFile)
	}

	tlsConfig.RootCAs = roots
	return tlsConfig, nil
}

func sendEmail(ctx context.Context, cfg Config, id int) (result Result) {
	start := time.Now()
	result.ID = id

	var submissionStart time.Time

	defer func() {
		result.Duration = time.Since(start)

		if !submissionStart.IsZero() && result.SubmissionDuration == 0 {
			result.SubmissionDuration = time.Since(submissionStart)
		}
	}()

	recipient := fmt.Sprintf("loadtest-%04d@example.com", id)

	address := net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port))

	dialer := net.Dialer{
		Timeout: cfg.Timeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		result.Err = fmt.Errorf("connect: %w", err)
		return
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(cfg.Timeout)); err != nil {
		result.Err = fmt.Errorf("set deadline: %w", err)
		return
	}

	client, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		result.Err = fmt.Errorf("SMTP greeting: %w", err)
		return
	}
	defer client.Close()

	if err := client.Hello("loadtest.local"); err != nil {
		result.Err = fmt.Errorf("EHLO: %w", err)
		return
	}

	// Enable TLS before attempting authentication.
	if cfg.StartTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			result.Err = fmt.Errorf("server does not advertise STARTTLS")
			return
		}

		tlsConfig, err := newTLSConfig(cfg)
		if err != nil {
			result.Err = err
			return
		}

		if err := client.StartTLS(tlsConfig); err != nil {
			result.Err = fmt.Errorf("STARTTLS: %w", err)
			return
		}

	}

	// Authenticate when credentials are configured.
	if cfg.Username != "" && cfg.Password != "" {
		result.AuthAttempted = true

		authStart := time.Now()

		auth := smtp.PlainAuth(
			"",
			cfg.Username,
			cfg.Password,
			cfg.Host,
		)

		err := client.Auth(auth)
		result.AuthDuration = time.Since(authStart)

		if err != nil {
			result.Err = fmt.Errorf("AUTH PLAIN: %w", err)
			return
		}

		result.Authenticated = true
	}

	// Start measuring the SMTP message submission separately.
	submissionStart = time.Now()

	if err := client.Mail(cfg.From); err != nil {
		result.Err = fmt.Errorf("MAIL FROM: %w", err)
		return
	}

	if err := client.Rcpt(recipient); err != nil {
		result.Err = fmt.Errorf("RCPT TO: %w", err)
		return
	}

	writer, err := client.Data()
	if err != nil {
		result.Err = fmt.Errorf("DATA: %w", err)
		return
	}

	message := fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: MailForge Authenticated Load Test %04d\r\n"+
			"Message-ID: <loadtest-%04d@mailforge.local>\r\n"+
			"MIME-Version: 1.0\r\n"+
			"Content-Type: text/plain; charset=UTF-8\r\n"+
			"\r\n"+
			"Hello from MailForge load tester.\r\n"+
			"Test message number: %d\r\n",
		cfg.From,
		recipient,
		id,
		id,
		id,
	)

	if _, err := io.Copy(writer, bytes.NewBufferString(message)); err != nil {
		_ = writer.Close()
		result.Err = fmt.Errorf("write DATA: %w", err)
		return
	}

	// Closing DATA waits for the server's final acceptance response.
	if err := writer.Close(); err != nil {
		result.Err = fmt.Errorf("finish DATA: %w", err)
		return
	}

	result.SubmissionDuration = time.Since(submissionStart)

	// The message is accepted. QUIT is best-effort.
	_ = client.Quit()

	return
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}

	sorted := append([]time.Duration(nil), values...)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	index := int(float64(len(sorted))*p+0.999999) - 1

	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

func printLatencyDistribution(name string, values []time.Duration) {
	if len(values) == 0 {
		return
	}

	var total time.Duration

	for _, value := range values {
		total += value
	}

	fmt.Println()
	fmt.Println(name)
	fmt.Println("------------------------------------")
	fmt.Printf("Samples:            %d\n", len(values))
	fmt.Printf("P50:                %s\n", percentile(values, 0.50))
	fmt.Printf("P90:                %s\n", percentile(values, 0.90))
	fmt.Printf("P95:                %s\n", percentile(values, 0.95))
	fmt.Printf("P99:                %s\n", percentile(values, 0.99))
	fmt.Printf("Average:            %s\n", total/time.Duration(len(values)))
}

func runLoadTest(ctx context.Context, cfg Config) Metrics {
	results := make(chan Result, cfg.Total)
	jobs := make(chan int)

	var wg sync.WaitGroup
	var successful atomic.Int64
	var failed atomic.Int64

	start := time.Now()

	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for id := range jobs {
				result := sendEmail(ctx, cfg, id)

				if result.Err != nil {
					failed.Add(1)
				} else {
					successful.Add(1)
				}

				results <- result
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
		wg.Wait()
		close(results)
	}()

	metrics := Metrics{
		Total: cfg.Total,
	}

	for result := range results {
		metrics.Latencies = append(metrics.Latencies, result.Duration)

		if result.AuthAttempted {
			metrics.AuthenticationAttempts++
			metrics.AuthLatencies = append(
				metrics.AuthLatencies,
				result.AuthDuration,
			)
		}

		if result.Authenticated {
			metrics.AuthenticationSuccesses++
		}

		if result.SubmissionDuration > 0 {
			metrics.SubmissionLatencies = append(
				metrics.SubmissionLatencies,
				result.SubmissionDuration,
			)
		}

		if result.Err != nil {
			metrics.Errors = append(
				metrics.Errors,
				fmt.Errorf("email %d: %w", result.ID, result.Err),
			)
		}
	}

	metrics.Duration = time.Since(start)
	metrics.Successful = successful.Load()
	metrics.Failed = failed.Load()

	return metrics
}

func printMetrics(metrics Metrics) {
	fmt.Println()
	fmt.Println("====================================")
	fmt.Println("       MAILFORGE LOAD TEST")
	fmt.Println("====================================")

	fmt.Printf("Total emails:             %d\n", metrics.Total)
	fmt.Printf("Successful:               %d\n", metrics.Successful)
	fmt.Printf("Failed:                   %d\n", metrics.Failed)
	fmt.Printf("Authentication attempts: %d\n", metrics.AuthenticationAttempts)
	fmt.Printf("Authentication successes:%d\n", metrics.AuthenticationSuccesses)

	fmt.Printf("Total duration:           %s\n",
		metrics.Duration.Round(time.Millisecond))

	seconds := metrics.Duration.Seconds()

	if seconds > 0 {
		fmt.Printf("Throughput:               %.2f emails/sec\n",
			float64(metrics.Total)/seconds)

		fmt.Printf("Accepted throughput:      %.2f emails/sec\n",
			float64(metrics.Successful)/seconds)
	}

	printLatencyDistribution("AUTHENTICATION LATENCY", metrics.AuthLatencies)
	printLatencyDistribution("SMTP SUBMISSION LATENCY", metrics.SubmissionLatencies)
	printLatencyDistribution("END-TO-END LATENCY", metrics.Latencies)

	if len(metrics.Errors) > 0 {
		fmt.Println()
		fmt.Println("FAILURES")
		fmt.Println("------------------------------------")

		limit := len(metrics.Errors)
		if limit > 20 {
			limit = 20
		}

		for _, err := range metrics.Errors[:limit] {
			fmt.Println(err)
		}

		if len(metrics.Errors) > limit {
			fmt.Printf("... and %d more errors\n", len(metrics.Errors)-limit)
		}
	}

	fmt.Println("====================================")
}

func main() {
	cfg := Config{}

	defaultCA := os.Getenv("SMTP_TLS_CA_FILE")
	if defaultCA == "" {
		defaultCA = os.Getenv("SMTP_TLS_CERT_FILE")
	}

	flag.StringVar(&cfg.Host, "host", "127.0.0.1", "SMTP server host")
	flag.IntVar(&cfg.Port, "port", 2525, "SMTP server port")
	flag.IntVar(&cfg.Total, "total", 100, "Total emails")
	flag.IntVar(&cfg.Concurrency, "concurrency", 10, "Concurrent SMTP connections")
	flag.StringVar(&cfg.From, "from", "sender@example.com", "Envelope sender")
	flag.StringVar(&cfg.Username, "username", os.Getenv("SMTP_USERNAME"), "SMTP AUTH username")
	flag.StringVar(&cfg.Password, "password", os.Getenv("SMTP_PASSWORD"), "SMTP AUTH password (prefer environment variable)")
	flag.BoolVar(&cfg.StartTLS, "starttls", false, "Upgrade SMTP connection using STARTTLS")
	flag.StringVar(&cfg.TLSCAFile, "tls-ca", defaultCA, "PEM certificate used to verify the SMTP server")
	flag.DurationVar(&cfg.Timeout, "timeout", 15*time.Second, "Timeout per SMTP operation")

	flag.Parse()

	if cfg.Total < 1 || cfg.Concurrency < 1 || cfg.Timeout <= 0 {
		fmt.Println("Total, concurrency and timeout must be positive")
		os.Exit(1)
	}

	if (cfg.Username == "") != (cfg.Password == "") {
		fmt.Println("Both SMTP username and password must be provided")
		os.Exit(1)
	}

	if cfg.Username != "" && !cfg.StartTLS {
		fmt.Println("SMTP authentication requires -starttls")
		os.Exit(1)
	}

	fmt.Println("Starting MailForge load test...")
	fmt.Printf("SMTP server:       %s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Total emails:      %d\n", cfg.Total)
	fmt.Printf("Concurrency:       %d\n", cfg.Concurrency)
	fmt.Printf("STARTTLS:          %t\n", cfg.StartTLS)

	if cfg.Username != "" {
		fmt.Printf("Authentication:    enabled (username: %s)\n", cfg.Username)
	} else {
		fmt.Println("Authentication:    disabled")
	}

	ctx := context.Background()

	metrics := runLoadTest(ctx, cfg)
	printMetrics(metrics)
}
