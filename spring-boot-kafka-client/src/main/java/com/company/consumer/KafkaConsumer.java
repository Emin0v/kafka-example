package com.company.consumer;

import com.company.model.MessageDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${com.kafka.topic}", groupId = "${com.kafka.group.id}")
    public void receivedMessage(MessageDto messageDto) {
        log.info("Message received .. MessageId: {}, Message: {}, Date: {}",
                messageDto.getMessageId(),
                messageDto.getMessage(),
                messageDto.getLocalDate());
    }

}
