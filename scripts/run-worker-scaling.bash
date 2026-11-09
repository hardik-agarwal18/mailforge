#!/usr/bin/env bash
# Run a controlled MailForge worker-scaling experiment from Git Bash on Windows.
# The benchmark binary starts its own mock SMTP receiver; this script starts the
# delivery worker first, verifies the queue is idle, and routes it to that mock.
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

# Load local configuration automatically. Environment variables exported by the
# caller can still be used for BENCHMARK_* overrides after sourcing .env manually.
if [[ -f .env && "${BENCHMARK_LOAD_DOTENV:-1}" == "1" ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

required_vars=(DATABASE_URL REDIS_ADDR REDIS_PASSWORD SMTP_USERNAME SMTP_PASSWORD SMTP_FROM POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD)
for name in "${required_vars[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf 'Required variable %s is missing. Configure it in .env.\n' "$name" >&2
    exit 2
  fi
done

# This orchestrator is deliberately restricted to the local development stack.
# That keeps synthetic .test traffic and the queue-drain checks away from production.
if [[ ! "$DATABASE_URL" =~ @(127\.0\.0\.1|localhost)(:|/) ]]; then
  printf 'DATABASE_URL must point to a loopback PostgreSQL host for this local benchmark.\n' >&2
  exit 2
fi
redis_host="${REDIS_ADDR%:*}"
if [[ "$redis_host" != "127.0.0.1" && "$redis_host" != "localhost" ]]; then
  printf 'REDIS_ADDR must point to loopback for this local benchmark.\n' >&2
  exit 2
fi

for command_name in go docker curl powershell.exe cygpath; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Required command not found: %s (run this from Git Bash on Windows).\n' "$command_name" >&2
    exit 2
  fi
done

worker_levels="${BENCHMARK_WORKER_CONCURRENCIES:-1 3 5}"
submission_concurrency="${BENCHMARK_SUBMISSION_CONCURRENCY:-3}"
benchmark_total="${BENCHMARK_TOTAL:-100}"
repeats="${BENCHMARK_REPEATS:-1}"
message_bytes="${BENCHMARK_MESSAGE_BYTES:-1024}"
mock_delay="${BENCHMARK_MOCK_DELAY:-0s}"
delivery_timeout="${BENCHMARK_DELIVERY_TIMEOUT:-3m}"
poll_interval="${BENCHMARK_POLL_INTERVAL:-250ms}"
submission_host="${BENCHMARK_HOST:-127.0.0.1}"
submission_port="${BENCHMARK_PORT:-1587}"
mock_addr="${BENCHMARK_MOCK_ADDR:-127.0.0.1:2526}"
recipient_domain="${BENCHMARK_RECIPIENT_DOMAIN:-bench.mailforge.test}"
worker_metrics_addr="${BENCHMARK_WORKER_METRICS_ADDR:-127.0.0.1:9091}"
outbox_metrics_url="${BENCHMARK_OUTBOX_METRICS_URL:-http://127.0.0.1:9096/metrics}"
submission_metrics_url="${BENCHMARK_SUBMISSION_METRICS_URL:-http://127.0.0.1:9095/metrics}"
postgres_container="${BENCHMARK_POSTGRES_CONTAINER:-mailforge-postgres}"
redis_container="${BENCHMARK_REDIS_CONTAINER:-mailforge-redis}"
queue_idle_timeout="${BENCHMARK_QUEUE_IDLE_TIMEOUT_SECONDS:-90}"
worker_start_timeout="${BENCHMARK_WORKER_START_TIMEOUT_SECONDS:-45}"
resource_interval="${BENCHMARK_RESOURCE_INTERVAL_SECONDS:-2}"

if [[ "$submission_host" != "127.0.0.1" ]]; then
  printf 'BENCHMARK_HOST must be 127.0.0.1 for this local TLS benchmark.\n' >&2
  exit 2
fi
if [[ ! "$recipient_domain" == *.test ]]; then
  printf 'BENCHMARK_RECIPIENT_DOMAIN must end in .test. Refusing non-test recipients.\n' >&2
  exit 2
fi
if [[ ! "$mock_addr" =~ ^127\.0\.0\.1:([0-9]{1,5})$ ]]; then
  printf 'BENCHMARK_MOCK_ADDR must be a loopback host and port, e.g. 127.0.0.1:2526.\n' >&2
  exit 2
fi
mock_host="${mock_addr%:*}"
mock_port="${mock_addr##*:}"
if (( mock_port < 1 || mock_port > 65535 )); then
  printf 'Invalid mock SMTP port: %s\n' "$mock_port" >&2
  exit 2
fi
if [[ ! "$worker_metrics_addr" =~ ^127\.0\.0\.1:([0-9]{1,5})$ ]]; then
  printf 'BENCHMARK_WORKER_METRICS_ADDR must be loopback host:port.\n' >&2
  exit 2
fi
worker_metrics_port="${worker_metrics_addr##*:}"
worker_metrics_url="http://${worker_metrics_addr}/metrics"

for numeric_value in "$submission_concurrency" "$benchmark_total" "$repeats" "$queue_idle_timeout" "$worker_start_timeout" "$resource_interval"; do
  if [[ ! "$numeric_value" =~ ^[1-9][0-9]*$ ]]; then
    printf 'Expected a positive integer, got: %s\n' "$numeric_value" >&2
    exit 2
  fi
done

read -r -a worker_levels_array <<< "$worker_levels"
if [[ ${#worker_levels_array[@]} -eq 0 ]]; then
  printf 'BENCHMARK_WORKER_CONCURRENCIES must contain worker counts, e.g. "1 3 5".\n' >&2
  exit 2
fi
for level in "${worker_levels_array[@]}"; do
  if [[ ! "$level" =~ ^[1-9][0-9]*$ ]] || (( level > 512 )); then
    printf 'Invalid worker concurrency: %s (allowed 1..512).\n' "$level" >&2
    exit 2
  fi
done

if [[ -n "${SMTP_TLS_CA_FILE:-}" && ! -f "$SMTP_TLS_CA_FILE" ]]; then
  printf 'SMTP_TLS_CA_FILE is set but does not exist: %s\n' "$SMTP_TLS_CA_FILE" >&2
  exit 2
fi

worker_metrics_port_url="http://${worker_metrics_host}:${worker_metrics_port}/metrics"

# Find an available worker executable extension for Git Bash/Windows.
worker_binary="bin/mailforge-delivery-worker.exe"
benchmark_binary="bin/mailforge-delivery-benchmark.exe"
resource_sampler="${SCRIPT_DIR}/sample-windows-resources.ps1"
if [[ ! -f "$resource_sampler" ]]; then
  printf 'Missing resource sampler: %s\n' "$resource_sampler" >&2
  exit 2
fi

stamp="$(date +%Y%m%d-%H%M%S)"
output_dir="${BENCHMARK_OUTPUT_DIR:-artifacts/benchmarks/worker-scaling-${stamp}}"
mkdir -p "$output_dir" bin
summary_csv="${output_dir}/summary.csv"
run_info="${output_dir}/run-info.txt"

WORKER_PID=""
METRICS_SAMPLER_PID=""
DOCKER_SAMPLER_PID=""
RESOURCE_SAMPLER_PID=""
STOP_FILE=""
WORKER_LOG=""

log() { printf '[worker-scaling] %s\n' "$*"; }

is_listening() {
  local port="$1"
  powershell.exe -NoProfile -NonInteractive -Command \
    "if (Get-NetTCPConnection -State Listen -LocalPort ${port} -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }" \
    >/dev/null 2>&1
}

check_loopback_tcp() {
  local host="$1" port="$2"
  powershell.exe -NoProfile -NonInteractive -Command \
    "\$client = New-Object System.Net.Sockets.TcpClient; try { \$client.Connect('${host}', ${port}); exit 0 } catch { exit 1 } finally { \$client.Dispose() }" \
    >/dev/null 2>&1
}

queue_counts() {
  local sql result
  sql="SELECT (SELECT COUNT(*) FROM recipients WHERE status IN ('pending','delivering','deferred'))::text || '|' || (SELECT COUNT(*) FROM outbox_events WHERE status IN ('pending','publishing'))::text;"
  result="$(docker exec -e "PGPASSWORD=${POSTGRES_PASSWORD}" "$postgres_container" \
    psql -X -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "$sql" 2>/dev/null | tr -d '\r')" || return 1
  [[ "$result" =~ ^[0-9]+\|[0-9]+$ ]] || return 1
  printf '%s\n' "$result"
}

wait_for_queue_idle() {
  local description="$1" elapsed=0 counts recipient_active outbox_active
  while (( elapsed <= queue_idle_timeout )); do
    if ! counts="$(queue_counts)"; then
      printf 'Could not query PostgreSQL queue state in container %s.\n' "$postgres_container" >&2
      return 1
    fi
    recipient_active="${counts%%|*}"
    outbox_active="${counts##*|}"
    if (( recipient_active == 0 && outbox_active == 0 )); then
      log "Database/outbox is idle ${description:+($description)}."
      return 0
    fi
    log "Waiting for queue to drain: active recipients=${recipient_active}, pending/publishing outbox=${outbox_active}."
    sleep 2
    elapsed=$((elapsed + 2))
  done
  printf 'Queue did not drain within %ss. Refusing to mix old work into the benchmark.\n' "$queue_idle_timeout" >&2
  return 1
}

stop_sampler_processes() {
  [[ -n "$STOP_FILE" ]] && : > "$STOP_FILE"
  for sampler_pid in "$METRICS_SAMPLER_PID" "$DOCKER_SAMPLER_PID" "$RESOURCE_SAMPLER_PID"; do
    if [[ -n "$sampler_pid" ]]; then
      wait "$sampler_pid" 2>/dev/null || true
    fi
  done
  METRICS_SAMPLER_PID=""
  DOCKER_SAMPLER_PID=""
  RESOURCE_SAMPLER_PID=""
}

stop_worker() {
  if [[ -z "$WORKER_PID" ]]; then
    return 0
  fi
  if kill -0 "$WORKER_PID" 2>/dev/null; then
    log "Stopping delivery worker PID ${WORKER_PID} gracefully."
    kill -INT "$WORKER_PID" 2>/dev/null || true
    for _ in $(seq 1 15); do
      if ! kill -0 "$WORKER_PID" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$WORKER_PID" 2>/dev/null; then
      log "Graceful stop timed out; sending TERM to worker PID ${WORKER_PID}."
      kill -TERM "$WORKER_PID" 2>/dev/null || true
      for _ in $(seq 1 5); do
        if ! kill -0 "$WORKER_PID" 2>/dev/null; then
          break
        fi
        sleep 1
      done
    fi
    if kill -0 "$WORKER_PID" 2>/dev/null && command -v taskkill.exe >/dev/null 2>&1; then
      log "Worker did not stop after TERM; using taskkill as a final cleanup fallback."
      taskkill.exe //F //T //PID "$WORKER_PID" >/dev/null 2>&1 || true
    fi
  fi
  wait "$WORKER_PID" 2>/dev/null || true
  WORKER_PID=""
}

cleanup() {
  local exit_code=$?
  set +e
  stop_sampler_processes
  stop_worker
  if [[ $exit_code -ne 0 && -n "$WORKER_LOG" && -f "$WORKER_LOG" ]]; then
    printf '\n--- Last worker log lines ---\n' >&2
    tail -n 50 "$WORKER_LOG" >&2
  fi
  return "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

log "Checking local dependencies and service readiness."
if ! docker inspect "$postgres_container" >/dev/null 2>&1; then
  printf 'PostgreSQL container %s is not running. Start Docker Compose first.\n' "$postgres_container" >&2
  exit 2
fi
if ! docker inspect "$redis_container" >/dev/null 2>&1; then
  printf 'Redis container %s is not running. Start Docker Compose first.\n' "$redis_container" >&2
  exit 2
fi
if ! check_loopback_tcp "$submission_host" "$submission_port"; then
  printf 'SMTP submission is not reachable at %s:%s. Start cmd/smtp-submission first.\n' "$submission_host" "$submission_port" >&2
  exit 2
fi
if ! curl -fsS --max-time 2 "$outbox_metrics_url" >/dev/null 2>&1; then
  printf 'Outbox publisher metrics are not reachable at %s. Start cmd/outbox-publisher and verify OUTBOX_METRICS_ADDR.\n' "$outbox_metrics_url" >&2
  exit 2
fi
if is_listening "$mock_port"; then
  printf 'Mock SMTP port %s is already occupied. Stop the bounce receiver/other listener on that port.\n' "$mock_port" >&2
  exit 2
fi
if is_listening "$worker_metrics_port" || curl -fsS --max-time 1 "$worker_metrics_url" >/dev/null 2>&1; then
  printf 'Worker metrics port %s is already occupied. Stop the existing delivery worker before running this orchestrator.\n' "$worker_metrics_port" >&2
  exit 2
fi

# Redis reachability is checked without flushing or modifying its data.
if ! docker exec -e "REDISCLI_AUTH=${REDIS_PASSWORD:-}" "$redis_container" redis-cli PING 2>/dev/null | grep -q PONG; then
  printf 'Redis did not respond to PING in container %s.\n' "$redis_container" >&2
  exit 2
fi

wait_for_queue_idle "before the first profile"

log "Building worker and benchmark executables once."
go build -trimpath -o "$worker_binary" ./cmd/delivery-worker
go build -trimpath -o "$benchmark_binary" ./cmd/delivery-benchmark

{
  printf 'created_at=%s\n' "$(date -Iseconds 2>/dev/null || date)"
  printf 'go_version=%s\n' "$(go version)"
  printf 'os=%s\n' "$(uname -srm)"
  printf 'worker_concurrencies=%s\n' "$worker_levels"
  printf 'submission_concurrency=%s\n' "$submission_concurrency"
  printf 'messages_per_profile=%s\n' "$benchmark_total"
  printf 'repeats=%s\n' "$repeats"
  printf 'submission_endpoint=%s:%s\n' "$submission_host" "$submission_port"
  printf 'mock_smtp=%s\n' "$mock_addr"
  printf 'worker_metrics=%s\n' "$worker_metrics_url"
  printf 'outbox_metrics=%s\n' "$outbox_metrics_url"
  printf 'submission_metrics=%s\n' "$submission_metrics_url"
} > "$run_info"

printf 'worker_concurrency,repeat,submission_concurrency,messages,submitted,delivered,bounced,unknown,pending,delivering,submission_throughput,end_to_end_throughput,delivery_latency_summary,json_report,benchmark_log,worker_log,resource_csv,worker_metrics,outbox_metrics,submission_metrics,docker_stats\n' > "$summary_csv"

extract_after_label() {
  local label="$1" file="$2" line value
  line="$(grep -m1 -F "$label" "$file" 2>/dev/null || true)"
  value="${line#*${label}}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value//,/;}"
  printf '%s' "$value"
}

start_worker() {
  local concurrency="$1" profile_dir="$2" expected_relay="Using SMTP relay ${mock_host}:${mock_port}"
  WORKER_LOG="${profile_dir}/worker.log"
  : > "$WORKER_LOG"
  log "Starting worker concurrency=${concurrency}, relay=${mock_addr}."
  (
    export DELIVERY_WORKER_CONCURRENCY="$concurrency"
    export SMTP_RELAY_HOST="$mock_host"
    export SMTP_RELAY_PORT="$mock_port"
    export SMTP_RELAY_TLS_MODE=none
    export SMTP_REQUIRE_STARTTLS=false
    export METRICS_ADDR="$worker_metrics_addr"
    exec "$worker_binary"
  ) > "$WORKER_LOG" 2>&1 &
  WORKER_PID=$!

  local waited=0
  while (( waited <= worker_start_timeout )); do
    if grep -Fq "$expected_relay" "$WORKER_LOG" && grep -Fq "Delivery worker started with concurrency=${concurrency}" "$WORKER_LOG"; then
      if curl -fsS --max-time 2 "$worker_metrics_url" >/dev/null 2>&1; then
        log "Worker ready: PID=${WORKER_PID}, concurrency=${concurrency}, relay=${mock_addr}."
        return 0
      fi
    fi
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
      printf 'Delivery worker exited during startup. See %s\n' "$WORKER_LOG" >&2
      tail -n 80 "$WORKER_LOG" >&2 || true
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
  printf 'Worker did not become ready within %ss. See %s\n' "$worker_start_timeout" "$WORKER_LOG" >&2
  tail -n 80 "$WORKER_LOG" >&2 || true
  return 1
}

start_samplers() {
  local profile_dir="$1"
  STOP_FILE="${profile_dir}/.stop-samplers"
  rm -f "$STOP_FILE"

  local resource_csv="${profile_dir}/windows-resources.csv"
  local resource_log="${profile_dir}/resource-sampler.log"
  local ps1_win out_win stop_win
  ps1_win="$(cygpath -w "$resource_sampler")"
  out_win="$(cygpath -w "$resource_csv")"
  stop_win="$(cygpath -w "$STOP_FILE")"

  powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass \
    -File "$ps1_win" -OutputPath "$out_win" -StopFile "$stop_win" \
    -WorkerProcessName "mailforge-delivery-worker" -IntervalSeconds "$resource_interval" \
    > "$resource_log" 2>&1 </dev/null &
  RESOURCE_SAMPLER_PID=$!

  local worker_metrics_file="${profile_dir}/worker-metrics-snapshots.txt"
  local outbox_metrics_file="${profile_dir}/outbox-metrics-snapshots.txt"
  local submission_metrics_file="${profile_dir}/submission-metrics-snapshots.txt"
  (
    while [[ ! -f "$STOP_FILE" ]]; do
      local scraped_at
      scraped_at="$(date -Iseconds 2>/dev/null || date)"
      printf '\n# scraped_at=%s\n' "$scraped_at" >> "$worker_metrics_file"
      curl -fsS --max-time 2 "$worker_metrics_url" >> "$worker_metrics_file" 2>/dev/null || printf '# scrape_failed\n' >> "$worker_metrics_file"
      printf '\n# scraped_at=%s\n' "$scraped_at" >> "$outbox_metrics_file"
      curl -fsS --max-time 2 "$outbox_metrics_url" >> "$outbox_metrics_file" 2>/dev/null || printf '# scrape_failed\n' >> "$outbox_metrics_file"
      if [[ -n "$submission_metrics_url" ]]; then
        printf '\n# scraped_at=%s\n' "$scraped_at" >> "$submission_metrics_file"
        curl -fsS --max-time 2 "$submission_metrics_url" >> "$submission_metrics_file" 2>/dev/null || printf '# scrape_failed\n' >> "$submission_metrics_file"
      fi
      sleep "$resource_interval"
    done
  ) &
  METRICS_SAMPLER_PID=$!

  local docker_stats_file="${profile_dir}/docker-stats.csv"
  printf 'timestamp,container,cpu_percent,memory_usage,memory_percent,network_io,block_io\n' > "$docker_stats_file"
  (
    while [[ ! -f "$STOP_FILE" ]]; do
      local now
      now="$(date -Iseconds 2>/dev/null || date)"
      docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}' \
        "$postgres_container" "$redis_container" 2>/dev/null \
        | while IFS= read -r row; do printf '%s,%s\n' "$now" "$row" >> "$docker_stats_file"; done
      sleep "$resource_interval"
    done
  ) &
  DOCKER_SAMPLER_PID=$!
}

for worker_concurrency in "${worker_levels_array[@]}"; do
  for ((repeat_index = 1; repeat_index <= repeats; repeat_index++)); do
    profile_dir="${output_dir}/worker-${worker_concurrency}/repeat-${repeat_index}"
    mkdir -p "$profile_dir"

    wait_for_queue_idle "before worker=${worker_concurrency}, repeat=${repeat_index}"
    if is_listening "$mock_port"; then
      printf 'Mock SMTP port %s became occupied before profile start.\n' "$mock_port" >&2
      exit 2
    fi
    if is_listening "$worker_metrics_port"; then
      printf 'Worker metrics port %s is occupied before profile start.\n' "$worker_metrics_port" >&2
      exit 2
    fi

    start_worker "$worker_concurrency" "$profile_dir"
    start_samplers "$profile_dir"

    benchmark_json="${profile_dir}/benchmark.json"
    benchmark_log="${profile_dir}/benchmark.log"
    benchmark_args=(
      "$benchmark_binary"
      --host "$submission_host"
      --port "$submission_port"
      --total "$benchmark_total"
      --concurrency "$submission_concurrency"
      --workers "$worker_concurrency"
      --message-bytes "$message_bytes"
      --mock-addr "$mock_addr"
      --recipient-domain "$recipient_domain"
      --mock-delay "$mock_delay"
      --delivery-timeout "$delivery_timeout"
      --poll-interval "$poll_interval"
      --json-out "$benchmark_json"
    )
    if [[ -n "${SMTP_TLS_CA_FILE:-}" ]]; then
      benchmark_args+=(--tls-ca "$SMTP_TLS_CA_FILE")
    fi

    log "Running profile: worker=${worker_concurrency}, submission=${submission_concurrency}, messages=${benchmark_total}, repeat=${repeat_index}/${repeats}."
    set +e
    "${benchmark_args[@]}" 2>&1 | tee "$benchmark_log"
    benchmark_status=${PIPESTATUS[0]}
    set -e

    stop_sampler_processes
    stop_worker

    if (( benchmark_status != 0 )); then
      printf 'Benchmark failed for worker=%s repeat=%s (exit=%s). See %s\n' \
        "$worker_concurrency" "$repeat_index" "$benchmark_status" "$benchmark_log" >&2
      exit "$benchmark_status"
    fi

    if ! wait_for_queue_idle "after worker=${worker_concurrency}, repeat=${repeat_index}"; then
      exit 1
    fi

    # A successful benchmark must prove both full recipient delivery and mock traffic.
    if ! grep -Eq "Submitted:[[:space:]]+${benchmark_total}[[:space:]]*/[[:space:]]*${benchmark_total}" "$benchmark_log" \
      || ! grep -Eq "Delivered:[[:space:]]+${benchmark_total}$" "$benchmark_log" \
      || ! grep -Fq 'Mock traffic observed:     true' "$benchmark_log"; then
      printf 'Profile did not meet the full-delivery/mock-traffic acceptance criteria. See %s\n' "$benchmark_log" >&2
      exit 1
    fi

    submitted="$(extract_after_label 'Submitted:' "$benchmark_log")"
    delivered="$(extract_after_label 'Delivered:' "$benchmark_log")"
    bounced="$(extract_after_label 'Bounced:' "$benchmark_log")"
    unknown="$(extract_after_label 'Unknown:' "$benchmark_log")"
    pending="$(extract_after_label 'Pending:' "$benchmark_log")"
    delivering="$(extract_after_label 'Delivering:' "$benchmark_log")"
    submission_rate="$(extract_after_label 'Submission throughput:' "$benchmark_log")"
    delivery_rate="$(extract_after_label 'End-to-end throughput:' "$benchmark_log")"
    delivery_latency="$(extract_after_label 'Message stored to delivered:' "$benchmark_log")"
    resource_csv="${profile_dir}/windows-resources.csv"
    worker_metrics_file="${profile_dir}/worker-metrics-snapshots.txt"
    outbox_metrics_file="${profile_dir}/outbox-metrics-snapshots.txt"
    submission_metrics_file="${profile_dir}/submission-metrics-snapshots.txt"
    docker_stats_file="${profile_dir}/docker-stats.csv"

    printf '%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s,"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
      "$worker_concurrency" "$repeat_index" "$submission_concurrency" "$benchmark_total" \
      "$submitted" "$delivered" "$bounced" "$unknown" "$pending" "$delivering" \
      "$submission_rate" "$delivery_rate" "$delivery_latency" \
      "$benchmark_json" "$benchmark_log" "$WORKER_LOG" "$resource_csv" "$worker_metrics_file" "$outbox_metrics_file" "$submission_metrics_file" "$docker_stats_file" \
      >> "$summary_csv"

    log "Profile complete: worker=${worker_concurrency}, delivered=${delivered}, end-to-end=${delivery_rate}."
  done
done

log "Worker-scaling experiment completed successfully."
printf '\nResults directory: %s\n' "$output_dir"
printf 'Summary CSV:       %s\n' "$summary_csv"
printf 'Run metadata:      %s\n' "$run_info"
printf '\nEach profile contains benchmark JSON/log, worker log, Prometheus metric snapshots, Windows resource CSV, and Docker stats CSV.\n'
