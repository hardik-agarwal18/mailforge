package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/smtp"
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
	Timeout     time.Duration
}

type Result struct {
	ID       int
	Duration time.Duration
	Err      error
}

type Metrics struct {
	Total      int
	Successful int64
	Failed     int64
	Duration   time.Duration
	Latencies  []time.Duration
	Errors     []error
}

func sendEmail(
	ctx context.Context,
	cfg Config,
	id int,
) Result {
	start := time.Now()

	result := Result{ID: id}

	defer func() {
		result.Duration = time.Since(start)
	}()

	recipient := fmt.Sprintf(
		"loadtest-%04d@example.com",
		id,
	)

	address := net.JoinHostPort(
		cfg.Host,
		fmt.Sprint(cfg.Port),
	)

	dialer := net.Dialer{
		Timeout: cfg.Timeout,
	}

	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		result.Err = fmt.Errorf("connect: %w", err)
		result.Duration = time.Since(start)
		return result
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(cfg.Timeout)); err != nil {
		result.Err = fmt.Errorf("set deadline: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	client, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		result.Err = fmt.Errorf("SMTP greeting: %w", err)
		result.Duration = time.Since(start)
		return result
	}
	defer client.Close()

	if err := client.Hello("loadtest.local"); err != nil {
		result.Err = fmt.Errorf("EHLO: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	if err := client.Mail(cfg.From); err != nil {
		result.Err = fmt.Errorf("MAIL FROM: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	if err := client.Rcpt(recipient); err != nil {
		result.Err = fmt.Errorf("RCPT TO: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	writer, err := client.Data()
	if err != nil {
		result.Err = fmt.Errorf("DATA: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	message := fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: MailForge Load Test %04d\r\n"+
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

	if _, err := io.Copy(
		writer,
		bytes.NewBufferString(message),
	); err != nil {
		_ = writer.Close()
		result.Err = fmt.Errorf("write DATA: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	// Closing DATA waits for the SMTP server's final response.
	// A nil error means the server accepted this message.
	if err := writer.Close(); err != nil {
		result.Err = fmt.Errorf("finish DATA: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	result.Duration = time.Since(start)

	// The message has already been accepted.
	// QUIT is best-effort and must not turn success into failure.
	_ = client.Quit()

	return result
}

func percentile(
	values []time.Duration,
	p float64,
) time.Duration {
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

func runLoadTest(
	ctx context.Context,
	cfg Config,
) Metrics {
	results := make(chan Result, cfg.Total)
	jobs := make(chan int)

	var wg sync.WaitGroup

	var successful atomic.Int64
	var failed atomic.Int64

	start := time.Now()

	// Start a fixed number of concurrent workers.
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

	// Dispatch email jobs.
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

	// Wait until all workers finish.
	go func() {
		wg.Wait()
		close(results)
	}()

	metrics := Metrics{
		Total: cfg.Total,
	}

	for result := range results {
		metrics.Latencies = append(
			metrics.Latencies,
			result.Duration,
		)

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

	fmt.Printf("Total emails:       %d\n", metrics.Total)
	fmt.Printf("Successful:         %d\n", metrics.Successful)
	fmt.Printf("Failed:             %d\n", metrics.Failed)
	fmt.Printf("Total duration:     %s\n", metrics.Duration.Round(time.Millisecond))

	seconds := metrics.Duration.Seconds()

	if seconds > 0 {
		fmt.Printf(
			"Throughput:         %.2f emails/sec\n",
			float64(metrics.Total)/seconds,
		)

		fmt.Printf(
			"Accepted throughput: %.2f emails/sec\n",
			float64(metrics.Successful)/seconds,
		)
	}

	fmt.Println()
	fmt.Println("LATENCY DISTRIBUTION")
	fmt.Println("------------------------------------")

	fmt.Printf("P50:                %s\n", percentile(metrics.Latencies, 0.50))
	fmt.Printf("P90:                %s\n", percentile(metrics.Latencies, 0.90))
	fmt.Printf("P95:                %s\n", percentile(metrics.Latencies, 0.95))
	fmt.Printf("P99:                %s\n", percentile(metrics.Latencies, 0.99))

	if len(metrics.Latencies) > 0 {
		var total time.Duration

		for _, latency := range metrics.Latencies {
			total += latency
		}

		average := total / time.Duration(len(metrics.Latencies))

		fmt.Printf("Average:            %s\n", average)
	}

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

	flag.StringVar(&cfg.Host, "host", "127.0.0.1", "SMTP server host")
	flag.IntVar(&cfg.Port, "port", 2525, "SMTP server port")
	flag.IntVar(&cfg.Total, "total", 100, "Total emails")
	flag.IntVar(&cfg.Concurrency, "concurrency", 10, "Concurrent SMTP connections")
	flag.StringVar(&cfg.From, "from", "sender@example.com", "Envelope sender")
	flag.DurationVar(&cfg.Timeout, "timeout", 15*time.Second, "Timeout per SMTP operation")

	flag.Parse()

	if cfg.Total < 1 || cfg.Concurrency < 1 || cfg.Timeout <= 0 {
		fmt.Println("Total, concurrency and timeout must be positive")
		return
	}

	fmt.Println("Starting MailForge load test...")
	fmt.Printf("SMTP server:  %s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Total emails: %d\n", cfg.Total)
	fmt.Printf("Concurrency:  %d\n", cfg.Concurrency)

	ctx := context.Background()

	metrics := runLoadTest(ctx, cfg)

	printMetrics(metrics)
}
