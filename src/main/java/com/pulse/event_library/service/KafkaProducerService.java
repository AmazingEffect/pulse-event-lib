package com.pulse.event_library.service;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * KafkaProducerService는 Kafka로 메시지를 전송하는 서비스입니다.
 * KafkaProducerService에서 트레이스 컨텍스트를 메시지에 포함시켜 전송해야 합니다.
 * 이를 위해 OpenTelemetry의 Tracer를 사용합니다.
 */
@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Tracer tracer = GlobalOpenTelemetry.getTracer("kafka-producer");

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Kafka 메시지에 트레이스 컨텍스트를 주입하기 위한 TextMapSetter
    private static final TextMapSetter<ProducerRecord<String, String>> setter =
            (carrier, key, value) -> carrier.headers().add(key, value.getBytes(StandardCharsets.UTF_8));

    /**
     * Kafka로 메시지를 전송합니다.
     *
     * @param topic       전송할 Kafka 토픽
     * @param payloadJson 전송할 메시지
     * @param context     전송에 사용될 컨텍스트
     * @return 전송 결과를 나타내는 CompletableFuture
     */
    public CompletableFuture<SendResult<String, String>> send(String topic, String payloadJson, Context context) {
        // 1. Span을 생성합니다. ("kafka-send"라는 이름을 가지며, 파라미터로 주어진 context를 부모로 설정합니다.)
        Span span = tracer.spanBuilder("[kafka] : message-produce").setParent(context).startSpan();

        // 2. Span을 현재 컨텍스트에 설정합니다.
        try (Scope scope = span.makeCurrent()) {

            // 2-1. Kafka 메시지 레코드를 생성합니다.
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, payloadJson);

            // 2-2. Traceparent 헤더를 Kafka 레코드에 주입하여 트레이스를 연결합니다. (리스너에서 가져다 사용)
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(context, record, setter);

            // 2-3. Kafka 메시지를 전송합니다.
            return kafkaTemplate.send(record);

        } finally {
            // 3. Span을 종료합니다.
            span.end();
        }
    }

    /**
     * Kafka로 메시지를 전송합니다.
     *
     * @param topic       전송할 Kafka 토픽
     * @param key         메시지 키
     * @param payloadJson 전송할 메시지
     * @param context     전송에 사용될 컨텍스트
     * @return 전송 결과를 나타내는 CompletableFuture
     */
    public CompletableFuture<SendResult<String, String>> send(String topic, String key, String payloadJson, Context context) {
        Span span = tracer.spanBuilder("[kafka] : message-produce").setParent(context).startSpan();

        try (Scope scope = span.makeCurrent()) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payloadJson);
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(context, record, setter);
            return kafkaTemplate.send(record);
        } finally {
            span.end();
        }
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
    public void sendWithRetry(String topic, String payloadJson, Context context) {
        send(topic, payloadJson, context).whenComplete((result, ex) -> {
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
    public void sendWithRetry(String topic, String key, String payloadJson, Context context) {
        send(topic, payloadJson, context).whenComplete((result, ex) -> {
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
