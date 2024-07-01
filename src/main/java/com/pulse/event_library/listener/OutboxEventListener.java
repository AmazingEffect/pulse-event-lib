package com.pulse.event_library.listener;

import com.pulse.event_library.event.OutboxEvent;
import com.pulse.event_library.service.KafkaProducerService;
import com.pulse.event_library.service.OutboxService;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Outbox 테이블과 관련된 스프링 이벤트를 처리하는 리스너
 */
@Component
public class OutboxEventListener {

    private final OutboxService outboxService;
    private final KafkaProducerService kafkaProducerService;
    private final Tracer tracer = GlobalOpenTelemetry.getTracer("outbox-event-listener");


    public OutboxEventListener(OutboxService outboxService, KafkaProducerService kafkaProducerService) {
        this.outboxService = outboxService;
        this.kafkaProducerService = kafkaProducerService;
    }

    /**
     * 이벤트가 발행되면 트랜잭션이 커밋되기 전에 Outbox 테이블에 저장하고 커밋한다.
     *
     * @param event
     */
    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void handleOutboxEvent(OutboxEvent event) {
        // 1. Span을 생성하고 현재 컨텍스트에 설정
        Span span = tracer.spanBuilder("save-outbox-event").startSpan();

        // 2. Span을 현재 컨텍스트에 설정
        try (Scope scope = span.makeCurrent()) {
            Context context = Context.current().with(span);
            try {
                // Outbox 테이블에 이벤트를 저장한다.
                outboxService.saveOutboxEvent(event);
            } catch (Exception e) {
                span.recordException(e);
                throw e;
            } finally {
                span.end();
            }
        }
    }

    /**
     * 트랜잭션이 성공적으로 커밋되면 Kafka로 이벤트를 발행한다.
     *
     * @param event
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void sendToKafka(OutboxEvent event) {
        // 현재 컨텍스트를 가져와 Span을 생성
        Span span = tracer.spanBuilder("send-to-kafka").startSpan();

        // Span을 현재 컨텍스트에 설정
        try (Scope scope = span.makeCurrent()) {
            Context context = Context.current().with(span);
            try {
                Long eventId = event.getId();
                String topic = outboxService.getKafkaTopic(event);
                kafkaProducerService.sendWithRetry(topic, String.valueOf(eventId), context);
                outboxService.markOutboxEventProcessed(event);
            } catch (Exception e) {
                outboxService.markOutboxEventFailed(event);
                span.recordException(e);
            }
        } finally {
            span.end();
        }
    }

}
