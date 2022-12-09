package com.company.config;

import com.company.model.MessageDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    @Value("${com.kafka.address}")
    private String kafkaAddress;

    @Value("${com.kafka.group.id}")
    private String groupId;

    @Value("${topics.dead-letter}")
    private String deadLetter;

    private final KafkaTemplate<String, MessageDto> kafkaTemplate;

    @Bean
    public ConsumerFactory<String, MessageDto> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MessageDto.class);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageDto> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MessageDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }

    public DefaultErrorHandler errorHandler() {
        ExponentialBackOffWithMaxRetries exponentialBackOff = new ExponentialBackOffWithMaxRetries(2); // total retries: 1 + 2
        exponentialBackOff.setInitialInterval(10_000);
        exponentialBackOff.setMultiplier(3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(dltPublishingRecoverer(), exponentialBackOff);

        setNonRetryableExceptions(errorHandler);
        setRetryableExceptions(errorHandler);
        logRetryListener(errorHandler);

        return errorHandler;
    }

    private void setRetryableExceptions(DefaultErrorHandler errorHandler) {
        errorHandler.addRetryableExceptions(
                NetworkException.class, NullPointerException.class
        );
    }

    private void logRetryListener(DefaultErrorHandler errorHandler) {
        errorHandler.setRetryListeners((consumerRecord, ex, nthAttempt) ->
                log.info("Failed Record - Retry Listener data : {}, exception : {} , nthAttempt : {} ",
                        consumerRecord, ex.getMessage(), nthAttempt)
        );
    }

    private void setNonRetryableExceptions(DefaultErrorHandler errorHandler) {
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class
        );
    }

    public DeadLetterPublishingRecoverer dltPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (failedRecord, e) -> {
                    if (e.getCause() instanceof NetworkException) {
                        log.info("Network failure occurred in consuming message : {}", failedRecord.toString());
                        return new TopicPartition(failedRecord.topic(), failedRecord.partition());
                    }
                    log.info("Other failure occurred in consuming, move to dead letter topic, message : {}, exception : {}", failedRecord, e);
                    return new TopicPartition(deadLetter, failedRecord.partition());
                });
    }


}
