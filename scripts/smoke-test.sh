#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KAFKA_READER_HEALTH="http://localhost:8080/actuator/health"
BUSINESS_HEALTH="http://localhost:8081/actuator/health"
BURST_ENDPOINT="http://localhost:8080/api/v1/burst-producer/burst"

GENERATED_METRIC="http://localhost:8080/actuator/metrics/reader.kafka.generated"
CONSUMED_METRIC="http://localhost:8080/actuator/metrics/reader.kafka.consumed"
FORWARDED_METRIC="http://localhost:8080/actuator/metrics/reader.http.forwarded"
FAILED_METRIC="http://localhost:8080/actuator/metrics/reader.http.failed"
DLQ_SENT_METRIC="http://localhost:8080/actuator/metrics/reader.kafka.dlq.sent"
PROCESSED_METRIC="http://localhost:8081/actuator/metrics/business.requests.processed"
REJECTED_METRIC="http://localhost:8081/actuator/metrics/business.requests.rejected"

wait_for_health() {
  local name="$1"
  local url="$2"
  local max_attempts=60

  for ((i = 1; i <= max_attempts; i++)); do
    if curl -fsS "$url" >/dev/null; then
      echo "[OK] $name is healthy"
      return 0
    fi
    sleep 1
  done

  echo "[FAIL] $name healthcheck timeout: $url" >&2
  return 1
}

metric_value() {
  local url="$1"
  local payload

  payload="$(curl -fsS "$url")"
  python3 - "$payload" <<'PY'
import json
import sys

raw = sys.argv[1]
doc = json.loads(raw)
measurements = doc.get("measurements", [])
if not measurements:
    print(-1)
    raise SystemExit(0)
print(measurements[0].get("value", -1))
PY
}

trigger_burst() {
  curl -fsS -X POST "$BURST_ENDPOINT" >/dev/null
  echo "[OK] burst triggered via $BURST_ENDPOINT"
}

metrics_ready() {
  generated=-1
  consumed=-1
  forwarded=-1
  failed=-1
  dlq_sent=-1
  processed=-1
  rejected=-1
  local max_attempts=60

  for ((i = 1; i <= max_attempts; i++)); do
    generated="$(metric_value "$GENERATED_METRIC")"
    consumed="$(metric_value "$CONSUMED_METRIC")"
    forwarded="$(metric_value "$FORWARDED_METRIC")"
    failed="$(metric_value "$FAILED_METRIC")"
    dlq_sent="$(metric_value "$DLQ_SENT_METRIC")"
    processed="$(metric_value "$PROCESSED_METRIC")"
    rejected="$(metric_value "$REJECTED_METRIC")"

    if python3 - "$generated" "$consumed" "$failed" "$dlq_sent" "$rejected" <<'PY'
import sys
generated = float(sys.argv[1])
consumed = float(sys.argv[2])
failed = float(sys.argv[3])
dlq_sent = float(sys.argv[4])
rejected = float(sys.argv[5])

# We test overload behavior, so at least one drop/reject signal must be positive.
has_drop_signal = failed > 0 or dlq_sent > 0 or rejected > 0
raise SystemExit(0 if generated > 0 and consumed > 0 and has_drop_signal else 1)
PY
    then
      echo "generated=$generated consumed=$consumed forwarded=$forwarded failed=$failed dlq_sent=$dlq_sent processed=$processed rejected=$rejected"
      return 0
    fi

    sleep 1
  done

  echo "generated=$generated consumed=$consumed forwarded=$forwarded failed=$failed dlq_sent=$dlq_sent processed=$processed rejected=$rejected"
  echo "[FAIL] overload drop metrics did not become positive in time" >&2
  return 1
}

echo "[1/4] Starting stack"
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d --build

echo "[2/4] Waiting for health"
wait_for_health "kafka-reader" "$KAFKA_READER_HEALTH"
wait_for_health "business-service" "$BUSINESS_HEALTH"

echo "[3/4] Triggering multiple bursts to induce overload"
for ((i = 1; i <= 20; i++)); do
  trigger_burst
done
echo "[3/4] Waiting for overload/drop metrics"
metrics_ready

echo "[4/4] Verifying data flow"
python3 - "$generated" "$consumed" "$forwarded" "$failed" "$dlq_sent" "$processed" "$rejected" <<'PY'
import sys

generated = float(sys.argv[1])
consumed = float(sys.argv[2])
forwarded = float(sys.argv[3])
failed = float(sys.argv[4])
dlq_sent = float(sys.argv[5])
processed = float(sys.argv[6])
rejected = float(sys.argv[7])

if generated <= 0:
    raise SystemExit("FAIL: generated metric is not positive")
if consumed <= 0:
    raise SystemExit("FAIL: consumed metric is not positive")
if failed <= 0 and dlq_sent <= 0 and rejected <= 0:
    raise SystemExit("FAIL: expected overload drops/rejections, but failed/dlq/rejected are all zero")
if forwarded < 0:
    raise SystemExit("FAIL: forwarded metric is negative")
if processed < 0:
    raise SystemExit("FAIL: processed metric is negative")

print("PASS: overload behavior observed (drop/reject signal is positive)")
PY

echo "Smoke test completed successfully."
