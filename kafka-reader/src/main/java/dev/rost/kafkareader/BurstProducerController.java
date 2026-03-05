package dev.rost.kafkareader;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/burst-producer")
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "reader.burst-producer", name = "enabled", havingValue = "true")
class BurstProducerController {

    private final BurstProducer burstProducer;


    @PostMapping("/burst")
    BurstResponse produceBurst() {
        var produced = burstProducer.produceBurst();
        return new BurstResponse(produced);
    }

    record BurstResponse(int produced) {
    }
}
