# Kafka Reading Backpressure Demo (RSocket)

Демо-проект показывает сценарий:

- `kafka-reader` быстро пишет сообщения в Kafka и сразу вычитывает их в том же приложении;
- для каждого вычитанного сообщения `kafka-reader` вызывает `business-service` по RSocket request-channel;
- `business-service` искусственно замедлен, а скорость доставки регулируется transport-level backpressure через `request-n`.

## Модули

- `kafka-reader` — быстрый producer + reader из Kafka + RSocket client к `business-service`.
- `business-service` — медленный обработчик c RSocket endpoint'ом и `max-in-flight` ограничением обработки.

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
- `reader.http.forwarded`
- `business.requests.processed`

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
- `READER_BUSINESS_URL` - адрес `business-service` для обработки сообщений (по умолчанию: `tcp://localhost:7000`)
- `READER_RETRY_MAX_ATTEMPTS` - количество retry при `429/5xx` (по умолчанию: `6`)
- `READER_RETRY_FIRST_BACKOFF_MS` - начальный backoff retry в миллисекундах (по умолчанию: `200`)
- `READER_BURST_PRODUCER_ENABLED` - включить генерацию нагрузки (по умолчанию: `true`)
- `READER_BURST_PRODUCER_BATCH_SIZE` - число сообщений за один burst (по умолчанию: `100`)
- `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` - актюатор endpoint'ы (по умолчанию: `health,info,metrics,prometheus`)
- `SPRING_KAFKA_CONSUMER_PROPERTIES_SESSION_TIMEOUT_MS` - таймаут сессии consumer group в миллисекундах, после которого старый consumer считается недоступным (по умолчанию: `6000`)
- `SPRING_KAFKA_CONSUMER_PROPERTIES_HEARTBEAT_INTERVAL_MS` - интервал heartbeat consumer в миллисекундах для поддержания membership в группе (по умолчанию: `2000`)

### business-service

- `SERVER_PORT` - HTTP-порт сервиса (по умолчанию: `8081`)
- `BUSINESS_PROCESSING_TIME` - искусственная задержка обработки в миллисекундах (по умолчанию: `250`)
- `BUSINESS_MAX_IN_FLIGHT` - лимит параллельной обработки входящего RSocket потока (по умолчанию: `32`)
- `SPRING_RSOCKET_SERVER_TRANSPORT` - транспорт RSocket-сервера `business-service` (по умолчанию: `tcp`)
- `SPRING_RSOCKET_SERVER_PORT` - порт RSocket-сервера `business-service` (по умолчанию: `7000`)
- `MANAGEMENT_ENDPOINTS_WEB_EXPOSURE_INCLUDE` - актюатор endpoint'ы (по умолчанию: `health,info,metrics,prometheus`)

## Backpressure в этом проекте

- `kafka-reader` публикует Kafka records в RSocket request-channel `process.channel`.
- `business-service` ограничивает реальную обработку через `flatMap(..., BUSINESS_MAX_IN_FLIGHT)`.
- RSocket передает спрос обратно через `request-n`, поэтому upstream не переполняет downstream.
- Kafka offset подтверждается только после подтверждения обработки сообщения от `business-service`.

## Локальный запуск без Docker

Сначала поднимите Kafka (например через docker compose только сервис `kafka`), затем:

```bash
mvn -pl business-service spring-boot:run
mvn -pl kafka-reader spring-boot:run
```
