package dev.rost.kafkareader;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static reactor.util.retry.Retry.backoff;

@Slf4j
@Component
@RequiredArgsConstructor
class KafkaToHttpBridge {
    @Value("${reader.retry.max-attempts}")
    private final int readerRetryMaxAttempts;
    @Value("${reader.retry.first-backoff-ms}")
    private final long readerRetryFirstBackoffMs;
    @Value("${reader.dlq-topic}")
    private final String readerDlqTopic;
    private final KafkaReceiver<String, String> kafkaReceiver;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BusinessServiceClient businessServiceClient;
    private final Map<String, PendingRecord> pendingRecords = new ConcurrentHashMap<>();

    private Disposable subscription;
    private Counter consumedCounter;
    private Counter forwardedCounter;
    private Counter failedCounter;
    private Counter dlqSentCounter;


    @Autowired
    private void initMeters(MeterRegistry meterRegistry) {
        this.consumedCounter = meterRegistry.counter("reader.kafka.consumed");
        this.forwardedCounter = meterRegistry.counter("reader.http.forwarded");
        this.failedCounter = meterRegistry.counter("reader.http.failed");
        this.dlqSentCounter = meterRegistry.counter("reader.kafka.dlq.sent");
    }


    @EventListener(ApplicationStartedEvent.class)
    void run() {
        var requests = kafkaReceiver.receive()
                .doOnNext(__ -> consumedCounter.increment())
                .map(record -> {
                    var messageId = "%s:%d:%d".formatted(record.topic(), record.partition(), record.offset());
                    pendingRecords.put(messageId, new PendingRecord(record));
                    return new Request(
                            messageId,
                            record.value(),
                            Instant.now());
                });

        this.subscription = businessServiceClient.process(requests)
                .doOnNext(this::handleResponse)
                .retryWhen(backoff(readerRetryMaxAttempts, Duration.ofMillis(readerRetryFirstBackoffMs)))
                .doOnError(error -> {
                    log.error("RSocket pipeline failed after retries, restarting", error);
                    drainPendingRecords("rsocket_error:%s".formatted(error.getClass().getSimpleName()));
                })
                .onErrorResume(__ -> Flux.empty())
                .repeatWhen(repeat -> repeat.delayElements(Duration.ofMillis(readerRetryFirstBackoffMs)))
                .subscribe();
    }

    private void handleResponse(String messageId) {
        var pendingRecord = pendingRecords.remove(messageId);
        if (pendingRecord == null)
            return;
        forwardedCounter.increment();
        pendingRecord.record().receiverOffset().acknowledge();
    }

    private void sendToDlq(String key, String payload, String reason) {
        var dlqPayload = reason + "|" + payload;
        kafkaTemplate.send(readerDlqTopic, key, dlqPayload)
                .whenComplete((__, error) -> {
                    if (error != null) {
                        log.error("Failed to send event to DLQ topic {}", readerDlqTopic, error);
                        return;
                    }
                    dlqSentCounter.increment();
                });
    }

    @PreDestroy
    void stop() {
        drainPendingRecords("bridge_shutdown");
        if (subscription != null)
            subscription.dispose();
    }

    private void drainPendingRecords(String reason) {
        pendingRecords.values().stream()
                .toList()
                .forEach(pendingRecord -> {
                    failedCounter.increment();
                    var record = pendingRecord.record();
                    sendToDlq(record.key(), record.value(), reason);
                    record.receiverOffset().acknowledge();
                });
        pendingRecords.clear();
    }

    private record PendingRecord(
            ReceiverRecord<String, String> record
    ) {}
}
