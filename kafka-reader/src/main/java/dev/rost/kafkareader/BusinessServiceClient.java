package dev.rost.kafkareader;

import lombok.RequiredArgsConstructor;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
class BusinessServiceClient {
    private final RSocketRequester requester;

    Flux<String> process(Flux<Request> requests) {
        return requester.route("process.channel")
                .data(requests, Request.class)
                .retrieveFlux(String.class);
    }
}
