package dev.rost.businessservice;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.MessageMapping;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@org.springframework.stereotype.Controller
@RequiredArgsConstructor
class Controller {
    @Value("${business.processing-time-ms}")
    private final long processingTimeMs;
    @Value("${business.max-in-flight}")
    private final int maxInFlight;
    private Counter processedCounter;
    private final AtomicInteger inFlight = new AtomicInteger();

    @Autowired
    private void initMeters(MeterRegistry meterRegistry) {
        this.processedCounter = meterRegistry.counter("business.requests.processed");
        Gauge.builder("business.requests.inflight", inFlight, AtomicInteger::get)
                .register(meterRegistry);
    }


    @MessageMapping("process.channel")
    Flux<String> process(Flux<Request> requestFlux) {
        return requestFlux.flatMap(this::processOne, maxInFlight);
    }

    private Mono<String> processOne(Request request) {
        inFlight.incrementAndGet();
        return Mono.delay(Duration.ofMillis(processingTimeMs))
                .doOnNext(__ -> processedCounter.increment())
                .thenReturn(request.messageId())
                .doFinally(__ -> inFlight.decrementAndGet());
    }
}
