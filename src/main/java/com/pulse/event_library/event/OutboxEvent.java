package com.pulse.event_library.event;

public interface OutboxEvent {
    String getEventType();
    Long getId();
}