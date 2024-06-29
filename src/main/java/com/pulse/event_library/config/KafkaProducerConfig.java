package com.pulse.event_library.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 프로듀서의 설정을 담당합니다.
 */
@Configuration
@ConfigurationProperties(prefix = "spring.kafka.producer")
public class KafkaProducerConfig {

    private String bootstrapServers;

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Kafka 프로듀서 팩토리를 설정합니다.
     * 이 팩토리는 Kafka 브로커에 메시지를 전송하는 프로듀서를 생성하는 데 사용됩니다.
     * 프로듀서 팩토리는 프로듀서 구성 설정을 포함합니다.
     *
     * @return Kafka 프로듀서 팩토리
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Kafka 템플릿을 설정합니다.
     * Kafka 템플릿은 메시지를 Kafka 토픽으로 전송하는 데 사용됩니다.
     * 프로듀서 팩토리를 통해 생성된 프로듀서를 사용하여 메시지를 전송합니다.
     *
     * @return Kafka 템플릿
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * 재시도 템플릿을 설정합니다.
     * @Retryable 어노테이션을 사용하면 이것을 사용하지 않아도 됩니다.
     * 재시도 템플릿은 Kafka 메시지 전송 및 수신 시 재시도 로직을 구성하는 데 사용됩니다.
     *
     * @return 재시도 템플릿
     */
    @Bean
    public RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        // 필요한 재시도 설정 추가
        return retryTemplate;
    }

}
