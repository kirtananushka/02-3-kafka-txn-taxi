package com.tananushka.mom.taxi.config;

import com.tananushka.mom.taxi.dto.VehicleSignal;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String producerBootstrapServers;

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String consumerBootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private boolean enableAutoCommit;

    @Value("${spring.kafka.consumer.isolation.level}")
    private String isolationLevel;

    @Bean
    public ProducerFactory<String, VehicleSignal> vehicleSignalProducerFactory() {
        Map<String, Object> configProps = getProducerConfigProps();
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "vehicle-signal-producer-transactional-id");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean(name = "kafkaVehicleSignalTemplate")
    public KafkaTemplate<String, VehicleSignal> kafkaVehicleSignalTemplate() {
        return new KafkaTemplate<>(vehicleSignalProducerFactory());
    }

    @Bean
    public ProducerFactory<String, Double> doubleProducerFactory() {
        Map<String, Object> configProps = getProducerConfigProps();
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "double-producer-transactional-id");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean(name = "kafkaDoubleTemplate")
    public KafkaTemplate<String, Double> kafkaDoubleTemplate() {
        return new KafkaTemplate<>(doubleProducerFactory());
    }

    @Bean
    public KafkaTransactionManager<String, VehicleSignal> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(vehicleSignalProducerFactory());
    }

    @Bean
    public KafkaTransactionManager<String, Double> kafkaDoubleTransactionManager() {
        return new KafkaTransactionManager<>(doubleProducerFactory());
    }

    @Bean
    public ConsumerFactory<String, VehicleSignal> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(getConsumerConfigProps(), new StringDeserializer(), new JsonDeserializer<>(VehicleSignal.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VehicleSignal> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, VehicleSignal> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Double> doubleConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(getConsumerConfigProps(), new StringDeserializer(), new JsonDeserializer<>(Double.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Double> doubleKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Double> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(doubleConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public KafkaListenerErrorHandler kafkaListenerErrorHandler() {
        return (message, exception) -> {
            log.error("Error in Kafka listener: {}", exception.getMessage());
            return null;
        };
    }

    private Map<String, Object> getProducerConfigProps() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerBootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return configProps;
    }

    private Map<String, Object> getConsumerConfigProps() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerBootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        return configProps;
    }
}
