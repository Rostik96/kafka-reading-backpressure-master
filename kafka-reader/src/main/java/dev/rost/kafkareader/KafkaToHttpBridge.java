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
import org.springframework.web.client.RestClientResponseException;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.springframework.http.HttpStatus.TOO_MANY_REQUESTS;
import static reactor.core.publisher.Mono.fromRunnable;
import static reactor.util.retry.Retry.backoff;

@Slf4j
@Component
@RequiredArgsConstructor
class KafkaToHttpBridge {
    @Value("${reader.max-in-flight}")
    private final int readerMaxInFlight;
    @Value("${reader.retry.max-attempts}")
    private final int readerRetryMaxAttempts;
    @Value("${reader.retry.first-backoff-ms}")
    private final long readerRetryFirstBackoffMs;
    @Value("${reader.dlq-topic}")
    private final String readerDlqTopic;
    private final KafkaReceiver<String, String> kafkaReceiver;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BusinessServiceClient businessServiceClient;

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
        this.subscription = kafkaReceiver.receive()
                .doOnNext(__ -> consumedCounter.increment())
                .onBackpressureDrop(record -> {
                    log.warn("Dropping event due to backpressure");
                    failedCounter.increment();
                    sendToDlq(record.key(), record.value(), "backpressure_drop");
                    // Acknowledge dropped records to avoid reprocessing loops under overload.
                    record.receiverOffset().acknowledge();
                })
                .flatMap(record -> forward(record.key(), record.value())
                                .then(fromRunnable(record.receiverOffset()::acknowledge)),
                        readerMaxInFlight)
                .subscribe();
    }

    private Mono<Void> forward(String key, String payload) {
        var request = new Request(UUID.randomUUID().toString(), payload, Instant.now());
        return businessServiceClient.process(request)
                .doOnSuccess(__ -> forwardedCounter.increment())
                .retryWhen(backoff(readerRetryMaxAttempts, Duration.ofMillis(readerRetryFirstBackoffMs))
                        .filter(this::isRetriable))
                .onErrorResume(error -> {
                    log.warn("Dropping event after retries due to {}", error.getClass().getSimpleName());
                    failedCounter.increment();
                    sendToDlq(key, payload, "retries_exhausted");
                    return Mono.empty();
                });
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

    private boolean isRetriable(Throwable error) {
        if (error instanceof RestClientResponseException respEx) {
            var status = respEx.getStatusCode();
            return status == TOO_MANY_REQUESTS || status.is5xxServerError();
        }
        return true;
    }

    @PreDestroy
    void stop() {
        if (subscription != null)
            subscription.dispose();
    }
}
