package com.company.producer;

import com.company.model.MessageDto;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/kafka")
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, MessageDto> kafkaTemplate;

    @Value("${com.kafka.topic}")
    private String topic;

    @PostMapping("/message")
    public String sendMessage(@RequestBody MessageDto messageDto){
        try {
             kafkaTemplate.send(topic, UUID.randomUUID().toString(), messageDto);
        }catch (Exception exception){
            exception.printStackTrace();
        }

        return "Message sent successfully";
    }

}
