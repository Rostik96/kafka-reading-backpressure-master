#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KAFKA_READER_HEALTH="http://localhost:8080/actuator/health"
BUSINESS_HEALTH="http://localhost:8081/actuator/health"
BURST_ENDPOINT="http://localhost:8080/api/v1/burst-producer/burst"

GENERATED_METRIC="http://localhost:8080/actuator/metrics/reader.kafka.generated"
CONSUMED_METRIC="http://localhost:8080/actuator/metrics/reader.kafka.consumed"
FORWARDED_METRIC="http://localhost:8080/actuator/metrics/reader.http.forwarded"
PROCESSED_METRIC="http://localhost:8081/actuator/metrics/business.requests.processed"

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
  processed=-1
  local max_attempts=60

  for ((i = 1; i <= max_attempts; i++)); do
    generated="$(metric_value "$GENERATED_METRIC")"
    consumed="$(metric_value "$CONSUMED_METRIC")"
    forwarded="$(metric_value "$FORWARDED_METRIC")"
    processed="$(metric_value "$PROCESSED_METRIC")"

    if python3 - "$generated" "$consumed" "$forwarded" "$processed" <<'PY'
import sys
vals = [float(v) for v in sys.argv[1:]]
raise SystemExit(0 if all(v > 0 for v in vals) else 1)
PY
    then
      echo "generated=$generated consumed=$consumed forwarded=$forwarded processed=$processed"
      return 0
    fi

    sleep 1
  done

  echo "generated=$generated consumed=$consumed forwarded=$forwarded processed=$processed"
  echo "[FAIL] metrics did not become positive in time" >&2
  return 1
}

echo "[1/4] Starting stack"
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d --build

echo "[2/4] Waiting for health"
wait_for_health "kafka-reader" "$KAFKA_READER_HEALTH"
wait_for_health "business-service" "$BUSINESS_HEALTH"

echo "[3/4] Triggering burst producer and waiting for positive metrics"
trigger_burst
metrics_ready

echo "[4/4] Verifying data flow"
python3 - "$generated" "$consumed" "$forwarded" "$processed" <<'PY'
import sys

generated = float(sys.argv[1])
consumed = float(sys.argv[2])
forwarded = float(sys.argv[3])
processed = float(sys.argv[4])

if generated <= 0:
    raise SystemExit("FAIL: generated metric is not positive")
if consumed <= 0:
    raise SystemExit("FAIL: consumed metric is not positive")
if forwarded <= 0:
    raise SystemExit("FAIL: forwarded metric is not positive")
if processed <= 0:
    raise SystemExit("FAIL: processed metric is not positive")

print("PASS: kafka-reader -> business-service flow is active")
PY

echo "Smoke test completed successfully."
