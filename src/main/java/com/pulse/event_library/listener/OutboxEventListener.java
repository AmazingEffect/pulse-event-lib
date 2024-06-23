package com.pulse.event_library.listener;

import com.pulse.event_library.event.OutboxEvent;
import com.pulse.event_library.service.OutboxService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Outbox 테이블과 관련된 스프링 이벤트를 처리하는 리스너
 */
@Component
public class OutboxEventListener {

    private final OutboxService outboxService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxEventListener(OutboxService outboxService, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxService = outboxService;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 이벤트가 발행되면 트랜잭션이 커밋되기 전에 Outbox 테이블에 저장하고 커밋한다.
     * @param event
     */
    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void handleOutboxEvent(OutboxEvent event) {
        // Outbox 테이블에 이벤트를 저장한다.
        outboxService.saveOutboxEvent(event);
    }

    /**
     * 트랜잭션이 성공적으로 커밋되면 Kafka로 이벤트를 발행한다.
     * @param event
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void sendToKafka(OutboxEvent event) {
        // 1. Outbox 테이블에서 이벤트 ID(payload)를 조회한다.
        Long outboxId = outboxService.getOutboxId(event);

        // 2. Kafka로 이벤트를 발행한다.
        try {
            String topic = outboxService.getKafkaTopic(event);
            kafkaTemplate.send(topic, String.valueOf(outboxId));
            outboxService.markOutboxEventProcessed(event);
        } catch (Exception e) {
            outboxService.markOutboxEventFailed(event);
        }
    }

}