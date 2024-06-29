package com.pulse.event_library.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 컨슈머의 설정을 담당합니다.
 */
@Configuration
@EnableKafka
@ConfigurationProperties(prefix = "spring.kafka.consumer")
public class KafkaConsumerConfig {

    private String bootstrapServers;
    private String groupId;

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * Kafka 컨슈머 팩토리를 설정합니다.
     * 이 팩토리는 Kafka 브로커로부터 메시지를 수신하는 컨슈머를 생성하는 데 사용됩니다.
     * 컨슈머 팩토리는 컨슈머 구성 설정을 포함합니다.
     *
     * @return Kafka 컨슈머 팩토리
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Kafka 리스너 컨테이너 팩토리를 설정합니다.
     * 리스너 컨테이너 팩토리는 Kafka 리스너가 메시지를 수신할 때 사용하는 설정을 제공합니다.
     * 컨슈머 팩토리를 통해 생성된 컨슈머를 사용하여 메시지를 수신합니다.
     *
     * 이 설정은 수동 ACK 모드와 오류 핸들러를 포함합니다.
     * MANUAL_IMMEDIATE ACK 모드는 메시지가 처리된 후 수동으로 즉시 ACK를 전송합니다. 리스너 메서드에서 Acknowledgment.acknowledge() 메서드를 호출하여 수동으로 ACK를 전송합니다.
     * 이 모드는 메시지가 제대로 처리되었을 때만 Kafka 브로커에 ACK를 보내므로, 메시지의 신뢰성을 높입니다.
     *
     * @return Kafka 리스너 컨테이너 팩토리
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(kafkaErrorHandler());
        return factory;
    }

    /**
     * Kafka 오류 핸들러를 설정합니다.
     * 이 핸들러는 Kafka 리스너에서 메시지 처리 중에 발생하는 예외를 처리하는 데 사용됩니다.
     * 예외 발생 시 재시도 로직 또는 사용자 정의 예외 처리 로직을 추가할 수 있습니다.
     *
     * FixedBackOff를 사용하여 5초 간격으로 최대 3회 재시도를 수행하도록 설정합니다.
     * 재시도 중 예외가 발생할 경우 로그에 기록합니다.
     *
     * @return Kafka 오류 핸들러
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(5000L, 3));
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            System.err.println("Failed to process record: " + record + " after " + deliveryAttempt + " attempts");
            System.err.println("Exception: " + ex.getMessage());
        });
        return errorHandler;
    }

}
