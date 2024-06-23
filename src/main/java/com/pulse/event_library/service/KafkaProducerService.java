package com.pulse.event_library.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Kafka로 메시지를 전송한다 (키 없이).
     * @param topic
     * @param payloadJson
     */
    public void send(String topic, String payloadJson) {
        Message<String> message = MessageBuilder
                .withPayload(payloadJson)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();

        kafkaTemplate.send(message);
    }

    /**
     * Kafka로 메시지를 전송한다 (키 포함).
     * @param topic
     * @param key
     * @param payloadJson
     */
    public void send(String topic, String key, String payloadJson) {
        Message<String> message = MessageBuilder
                .withPayload(payloadJson)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .build();

        kafkaTemplate.send(message);
    }

}