# Kafka Reading Backpressure Demo (HTTP/WebFlux)

Демо-проект показывает сценарий:

- `kafka-reader` быстро пишет сообщения в Kafka и сразу вычитывает их в том же приложении;
- для каждого вычитанного сообщения `kafka-reader` вызывает `business-service` по HTTP;
- `business-service` искусственно замедлен и при перегрузе возвращает `429`.

Это база для обсуждения, как строить backpressure-control поверх HTTP/WebFlux без RSocket `request-n`.

## Модули

- `kafka-reader` — быстрый producer + reader из Kafka + HTTP client к `business-service`.
- `business-service` — медленный обработчик с `max-in-flight` ограничением.

## Быстрый старт

```bash
mvn -q -DskipTests package
docker compose up -d --build
```

Остановка:

```bash
docker compose down
```

## Smoke test

```bash
bash scripts/smoke-test.sh
```

Проверяются health и метрики:

- `reader.kafka.generated`
- `reader.kafka.consumed`
- `reader.http.failed`
- `reader.kafka.dlq.sent`
- `business.requests.rejected`

### Ручной запуск burst через REST

Burst-генератор в `kafka-reader` запускается по HTTP:

```bash
curl -X POST http://localhost:8080/api/v1/burst-producer/burst
```

Ответ:

```json
{"produced":100}
```

`produced` — число сообщений, отправленных в Kafka за один вызов (берется из `READER_BURST_PRODUCER_BATCH_SIZE`).

## Environment Variables

### kafka-reader

- `SERVER_PORT` - HTTP-порт сервиса (по умолчанию: `8080`)
- `KAFKA_BOOTSTRAP_SERVERS` - адрес Kafka broker (по умолчанию: `localhost:9092`)
- `READER_TOPIC` - Kafka topic для генерации/чтения (по умолчанию: `letters`)
- `READER_DLQ_TOPIC` - Kafka topic для сообщений, сброшенных в DLQ (по умолчанию: `letters.dlq`)
- `READER_GROUP_ID` - consumer group id (по умолчанию: `kafka-reader`)
- `READER_BUSINESS_URL` - URL `business-service` (по умолчанию: `http://localhost:8081/process`)
- `READER_BUSINESS_READ_TIMEOUT_MS` - таймаут чтения ответа от `business-service` в миллисекундах (по умолчанию: `5000`)
- `READER_BUSINESS_CONNECTION_TIMEOUT_MS` - таймаут установления соединения с `business-service` в миллисекундах (по умолчанию: `1000`)
- `READER_MAX_IN_FLIGHT` - лимит одновременных HTTP-вызовов в `business-service` (по умолчанию: `64`)
- `READER_RETRY_MAX_ATTEMPTS` - количество retry при `429/5xx` (по умолчанию: `6`)
- `READER_RETRY_FIRST_BACKOFF_MS` - начальный backoff retry в миллисекундах (по умолчанию: `200`)
- `READER_BURST_PRODUCER_ENABLED` - включить генерацию нагрузки (по умолчанию: `true`)
- `READER_BURST_PRODUCER_BATCH_SIZE` - число сообщений за один burst (по умолчанию: `100`)
- `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` - актюатор endpoint'ы (по умолчанию: `health,info,metrics,prometheus`)

### business-service

- `SERVER_PORT` - HTTP-порт сервиса (по умолчанию: `8081`)
- `BUSINESS_PROCESSING_TIME` - искусственная задержка обработки в миллисекундах (по умолчанию: `250`)
- `BUSINESS_MAX_IN_FLIGHT` - лимит параллельной обработки. При превышении сервис вернет `429` (по умолчанию: `32`)
- `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` - актюатор endpoint'ы (по умолчанию: `health,info,metrics,prometheus`)

## Локальный запуск без Docker

Сначала поднимите Kafka (например через docker compose только сервис `kafka`), затем:

```bash
mvn -pl business-service spring-boot:run
mvn -pl kafka-reader spring-boot:run
```
