package com.pulse.event_library.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * 이벤트 라이브러리의 자동 설정을 담당합니다. (타 서버에서 빈 등록을 위해 사용)
 */
@Configuration
@ComponentScan(basePackages = "com.pulse.event_library")
public class EventLibraryAutoConfig {
}
