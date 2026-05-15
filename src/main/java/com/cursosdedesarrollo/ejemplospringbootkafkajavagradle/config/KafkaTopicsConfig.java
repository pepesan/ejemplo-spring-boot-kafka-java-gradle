package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.config;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams.OrderPaymentJoinTopology;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams.PaymentStreamsTopology;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic productsTopic() {
        return TopicBuilder.name("products").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("orders").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsTopic() {
        return TopicBuilder.name(PaymentStreamsTopology.INPUT_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsHighValueTopic() {
        return TopicBuilder.name(PaymentStreamsTopology.HIGH_VALUE_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic ordersPaymentsTopic() {
        return TopicBuilder.name(OrderPaymentJoinTopology.OUTPUT_TOPIC).partitions(1).replicas(1).build();
    }
}