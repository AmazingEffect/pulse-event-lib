package com.pulse.event_library.listener;

import com.pulse.event_library.service.OutboxKafkaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Outbox 테이블의 상태를 업데이트하는 리스너
 */
@Component
public class OutboxStatusChangeListener {

    private final OutboxKafkaService outboxKafkaService;

    public OutboxStatusChangeListener(OutboxKafkaService outboxKafkaService) {
        this.outboxKafkaService = outboxKafkaService;
    }

    /**
     * outbox로 끝나는 토픽만 처리하는 listen 메서드
     *
     * @param message
     * @param ack
     */
    @KafkaListener(topics = ".*outbox$")
    public void listen(
            String message,
            Acknowledgment ack
    ) {
        try {
            // Kafka로부터 수신한 메시지를 Outbox 테이블의 상태로 업데이트한다.
            outboxKafkaService.updateOutboxStatus();
            // ack 처리
            ack.acknowledge();
        } catch (Exception e) {
            // 재시도 트리거
            throw e;
        }
    }

}
