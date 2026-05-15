package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.acceptance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

@TestConfiguration
class KafkaTestConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.application.name}")
    private String groupId;

    @SuppressWarnings("unchecked")
    @Bean
    @Primary
    public ProducerFactory<String, Object> testProducerFactory() {
        JsonMapper jsonMapper = JsonMapper.builder().findAndAddModules().build();
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JacksonJsonSerializer<>(jsonMapper));
    }

    @Bean
    @Primary
    public KafkaTemplate<String, Object> testKafkaTemplate(ProducerFactory<String, Object> testProducerFactory) {
        return new KafkaTemplate<>(testProducerFactory);
    }

    @SuppressWarnings("unchecked")
    @Bean
    @Primary
    public ConsumerFactory<String, Object> testConsumerFactory() {
        JsonMapper jsonMapper = JsonMapper.builder().findAndAddModules().build();
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JacksonJsonDeserializer<Object> deserializer = new JacksonJsonDeserializer<>(Object.class, jsonMapper);
        deserializer.trustedPackages("com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models");
        deserializer.setUseTypeHeaders(true);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> testConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(testConsumerFactory);
        return factory;
    }
}