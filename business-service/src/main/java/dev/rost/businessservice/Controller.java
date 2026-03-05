package dev.rost.businessservice;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.springframework.http.HttpStatus.TOO_MANY_REQUESTS;

@RestController
@RequiredArgsConstructor
class Controller {
    @Value("${business.processing-time-ms}")
    private final long processingTimeMs;
    @Value("${business.max-in-flight}")
    private final int maxInFlight;
    private Counter processedCounter;
    private Counter rejectedCounter;
    private final AtomicInteger inFlight = new AtomicInteger();

    @Autowired
    private void initMeters(MeterRegistry meterRegistry) {
        this.processedCounter = meterRegistry.counter("business.requests.processed");
        this.rejectedCounter = meterRegistry.counter("business.requests.rejected");
        Gauge.builder("business.requests.inflight", inFlight, AtomicInteger::get)
                .register(meterRegistry);
    }


    @PostMapping("/process")
    Mono<ResponseEntity<Void>> process(@RequestBody Mono<Request> requestMono) {
        int current = inFlight.incrementAndGet();
        if (current > maxInFlight) {
            inFlight.decrementAndGet();
            rejectedCounter.increment();
            return Mono.just(ResponseEntity.status(TOO_MANY_REQUESTS).build());
        }

        return requestMono
                .delayElement(Duration.ofMillis(processingTimeMs))
                .doOnNext(__ -> processedCounter.increment())
                .thenReturn(ResponseEntity.accepted().<Void>build())
                .doFinally(ignore -> inFlight.decrementAndGet());
    }
}
