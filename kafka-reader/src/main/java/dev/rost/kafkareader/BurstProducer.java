package dev.rost.kafkareader;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "reader.burst-producer", name = "enabled", havingValue = "true")
class BurstProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private Counter generatedCounter;
    @Value("${reader.burst-producer.batch-size}")
    private final int readerProduceBatchSize;
    @Value("${reader.topic}")
    private final String readerTopic;
    private final AtomicLong sequence = new AtomicLong();

    @Autowired
    private void initMeters(MeterRegistry meterRegistry) {
        this.generatedCounter = meterRegistry.counter("reader.kafka.generated");
    }

    int produceBurst() {
        for (var i = 0; i < readerProduceBatchSize; i++) {
            var id = sequence.incrementAndGet();
            var key = "k-" + id;
            var value = "event-" + id + "-p" + ThreadLocalRandom.current().nextInt(1_000);
            kafkaTemplate.send(readerTopic, key, value);
            generatedCounter.increment();
        }
        return readerProduceBatchSize;
    }
}
