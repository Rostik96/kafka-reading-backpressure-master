package dev.rost.kafkareader;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import reactor.core.publisher.Mono;

import static reactor.core.scheduler.Schedulers.boundedElastic;

@Service
@RequiredArgsConstructor
class BusinessServiceClient {
    private final RestClient businessRestClient;

    Mono<Void> process(Request request) {
        return Mono.fromRunnable(() -> {
                    businessRestClient.post()
                            .body(request)
                            .retrieve()
                            .toBodilessEntity();
                })
                .subscribeOn(boundedElastic())
                .then();
    }
}
