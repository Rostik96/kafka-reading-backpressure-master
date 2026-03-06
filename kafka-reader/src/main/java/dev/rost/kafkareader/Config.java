package dev.rost.kafkareader;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.RSocketRequester;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.net.URI;
import java.util.HashMap;
import java.util.Set;

@Configuration("kafkaReaderConfig")
@RequiredArgsConstructor
class Config {
    @Bean
    ReceiverOptions<String, String> receiverOptions(KafkaProperties kafkaProperties,
                                                    @Value("${reader.group-id}") String readerGroupId,
                                                    @Value("${reader.topic}") String readerTopic) {
        return ReceiverOptions.<String, String>create(new HashMap<>(kafkaProperties.buildConsumerProperties(null)) {{
                    put(ConsumerConfig.GROUP_ID_CONFIG, readerGroupId);
                    put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                }})
                .subscription(Set.of(readerTopic));
    }

    @Bean
    KafkaReceiver<String, String> kafkaReceiver(ReceiverOptions<String, String> receiverOptions) {
        return KafkaReceiver.create(receiverOptions);
    }

    @Bean
    RSocketRequester requester(RSocketRequester.Builder builder,
                               @Value("${reader.business.url}") String readerBusinessRSocketUrl) {
        var uri = URI.create(readerBusinessRSocketUrl);
        return builder.tcp(uri.getHost(), uri.getPort());
    }
}
