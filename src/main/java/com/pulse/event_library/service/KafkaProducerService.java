package com.pulse.event_library.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Kafka로 메시지를 전송합니다.
     *
     * @param topic       전송할 Kafka 토픽
     * @param payloadJson 전송할 메시지
     * @return 전송 결과를 나타내는 CompletableFuture
     */
    public CompletableFuture<SendResult<String, String>> send(String topic, String payloadJson) {
        Message<String> message = MessageBuilder
                .withPayload(payloadJson)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();

        return kafkaTemplate.send(message);
    }

    /**
     * Kafka로 메시지를 전송합니다.
     *
     * @param topic       전송할 Kafka 토픽
     * @param key         메시지 키
     * @param payloadJson 전송할 메시지
     * @return 전송 결과를 나타내는 CompletableFuture
     */
    public CompletableFuture<SendResult<String, String>> send(String topic, String key, String payloadJson) {
        Message<String> message = MessageBuilder
                .withPayload(payloadJson)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .build();

        return kafkaTemplate.send(message);
    }

    /**
     * 재시도 로직을 포함하여 Kafka로 메시지를 전송합니다. 성공 시 메시지 전송 성공 로그를 출력합니다.
     * 메시지 전송이 실패할 경우 최대 3회 재시도하며, 재시도 간격은 5초입니다.
     * whenComplete는 CompletableFuture의 메서드로, 비동기 작업이 완료된 후에 호출되는 콜백을 설정하는 것입니다.
     * whenComplete는 작업이 성공하든 실패하든 상관없이 호출됩니다.
     *
     * @param topic       전송할 Kafka 토픽
     * @param payloadJson 전송할 메시지
     */
    @Retryable(retryFor = Exception.class, maxAttempts = 3, backoff = @Backoff(delay = 5000))
    public void sendWithRetry(String topic, String payloadJson) {
        send(topic, payloadJson).whenComplete((result, ex) -> {
            // 실패시 예외를 던져서 처리합니다.
            if (ex != null) {
                throw new RuntimeException("Failed to send message to Kafka", ex);
            }
            // 성공시 전송된 메시지의 오프셋을 로그에 기록합니다.
            RecordMetadata metadata = result.getRecordMetadata();
            System.out.println("Sent message=[" + payloadJson + "] with offset=[" + metadata.offset() + "]");
        });
    }

    /**
     * 재시도 로직을 포함하여 Kafka로 메시지를 전송합니다. 성공 시 메시지 전송 성공 로그를 출력합니다.
     * 메시지 전송이 실패할 경우 최대 3회 재시도하며, 재시도 간격은 5초입니다.
     *
     * @param topic       전송할 Kafka 토픽
     * @param key         메시지 키
     * @param payloadJson 전송할 메시지
     */
    @Retryable(retryFor = Exception.class, maxAttempts = 3, backoff = @Backoff(delay = 5000))
    public void sendWithRetry(String topic, String key, String payloadJson) {
        send(topic, key, payloadJson).whenComplete((result, ex) -> {
            // 실패시 예외를 던져서 처리합니다.
            if (ex != null) {
                throw new RuntimeException("Failed to send message to Kafka", ex);
            }
            // 성공시 전송된 메시지의 오프셋을 로그에 기록합니다.
            RecordMetadata metadata = result.getRecordMetadata();
            System.out.println("Sent message=[" + payloadJson + "] with offset=[" + metadata.offset() + "]");
        });
    }

}
