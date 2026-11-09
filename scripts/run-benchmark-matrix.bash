#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

required_vars=(DATABASE_URL SMTP_USERNAME SMTP_PASSWORD SMTP_FROM)
for name in "${required_vars[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf 'Required environment variable %s is not set. Load your local .env first.\n' "$name" >&2
    exit 2
  fi
done

benchmark_total="${BENCHMARK_TOTAL:-100}"
worker_label="${BENCHMARK_WORKERS:-${DELIVERY_WORKER_CONCURRENCY:-3}}"
concurrency_input="${BENCHMARK_CONCURRENCIES:-1 5 10 25}"
message_bytes="${BENCHMARK_MESSAGE_BYTES:-1024}"
mock_delay="${BENCHMARK_MOCK_DELAY:-0s}"
delivery_timeout="${BENCHMARK_DELIVERY_TIMEOUT:-3m}"
poll_interval="${BENCHMARK_POLL_INTERVAL:-250ms}"
submission_host="${BENCHMARK_HOST:-localhost}"
submission_port="${BENCHMARK_PORT:-1587}"
mock_addr="${BENCHMARK_MOCK_ADDR:-127.0.0.1:2526}"
recipient_domain="${BENCHMARK_RECIPIENT_DOMAIN:-bench.mailforge.test}"

if [[ ! "$recipient_domain" == *.test ]]; then
  printf 'BENCHMARK_RECIPIENT_DOMAIN must end in .test; refusing non-test recipient domain.\n' >&2
  exit 2
fi

read -r -a concurrency_levels <<< "$concurrency_input"
if [[ ${#concurrency_levels[@]} -eq 0 ]]; then
  printf 'BENCHMARK_CONCURRENCIES must contain at least one positive integer.\n' >&2
  exit 2
fi

stamp="$(date +%Y%m%d-%H%M%S)"
output_dir="${BENCHMARK_OUTPUT_DIR:-artifacts/benchmarks/${stamp}}"
mkdir -p "$output_dir"

printf 'MailForge benchmark matrix\n'
printf '  output directory: %s\n' "$output_dir"
printf '  messages/profile: %s\n' "$benchmark_total"
printf '  submission concurrency: %s\n' "$concurrency_input"
printf '  worker concurrency label: %s\n' "$worker_label"
printf '\nIMPORTANT: --workers is a report label only. The already-running\n'
printf 'delivery-worker must have been started with DELIVERY_WORKER_CONCURRENCY=%s.\n\n' "$worker_label"

for concurrency in "${concurrency_levels[@]}"; do
  if [[ ! "$concurrency" =~ ^[1-9][0-9]*$ ]]; then
    printf 'Invalid concurrency value: %s\n' "$concurrency" >&2
    exit 2
  fi

  json_path="${output_dir}/submission-c${concurrency}.json"
  log_path="${output_dir}/submission-c${concurrency}.log"
  args=(
    go run ./cmd/delivery-benchmark
    --host "$submission_host"
    --port "$submission_port"
    --username "$SMTP_USERNAME"
    --password "$SMTP_PASSWORD"
    --from "$SMTP_FROM"
    --total "$benchmark_total"
    --concurrency "$concurrency"
    --workers "$worker_label"
    --message-bytes "$message_bytes"
    --mock-addr "$mock_addr"
    --recipient-domain "$recipient_domain"
    --mock-delay "$mock_delay"
    --delivery-timeout "$delivery_timeout"
    --poll-interval "$poll_interval"
    --json-out "$json_path"
  )

  if [[ -n "${SMTP_TLS_CA_FILE:-}" ]]; then
    args+=(--tls-ca "$SMTP_TLS_CA_FILE")
  fi

  printf '\n============================================================\n'
  printf 'Running submission concurrency %s\n' "$concurrency"
  printf 'JSON: %s\nLog:  %s\n' "$json_path" "$log_path"
  printf '============================================================\n'
  "${args[@]}" 2>&1 | tee "$log_path"
done

printf '\nBenchmark matrix completed. JSON and logs are in: %s\n' "$output_dir"
printf 'Keep the JSON files together; they contain run settings and measured results.\n'
