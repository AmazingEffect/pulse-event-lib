package com.pulse.event_library.service;

import com.pulse.event_library.event.OutboxEvent;

/**
 * OutboxService의 인터페이스를 정의
 */
public interface OutboxService {

    // OutboxEvent를 저장
    void saveOutboxEvent(OutboxEvent event);

    // OutboxEvent를 처리 완료로 표시
    void markOutboxEventProcessed(OutboxEvent event);

    // OutboxEvent를 처리 성공으로 표시
    void markOutboxEventSuccess(OutboxEvent event);

    // OutboxEvent를 처리 실패로 표시
    void markOutboxEventFailed(OutboxEvent event);

    // OutboxEvent의 Kafka 토픽을 반환
    String getKafkaTopic(OutboxEvent event);

}