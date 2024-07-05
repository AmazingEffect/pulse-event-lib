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
     * 트랜잭션이 성공적으로 커밋되면 Kafka로 이벤트를 발행합니다.
     *
     * @param event 전송할 Outbox 이벤트
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void sendToKafka(OutboxEvent event) {
        // 1. 현재 컨텍스트를 가져와 Span을 생성
        Span span = tracer.spanBuilder("send-to-kafka").startSpan();

        // 2. Span을 현재 컨텍스트에 설정
        try (Scope scope = span.makeCurrent()) {
            Context context = Context.current().with(span);
            try {
                // 2-1. 메시지로 보낼 payload와 전송할 Kafka의 토픽 정보를 가져옵니다.
                Long message = event.getId();
                String topic = outboxService.getKafkaTopic(event);

                // 2-2. 추출한 토픽에 Kafka 메시지를 전송합니다.
                kafkaProducerService.sendWithRetry(topic, String.valueOf(message), context);

                // 2-3. 메시지 전송 후 Outbox 이벤트에 메시지를 처리된 상태로 변경합니다.
                outboxService.markOutboxEventProcessed(event);
            } catch (Exception e) {
                // exception: 예외 발생 시 Outbox 이벤트를 실패 상태로 변경하고 Span에 예외를 기록합니다.
                outboxService.markOutboxEventFailed(event);
                span.recordException(e);
            }
        } finally {
            // Span을 종료합니다.
            span.end();
        }
    }

}
