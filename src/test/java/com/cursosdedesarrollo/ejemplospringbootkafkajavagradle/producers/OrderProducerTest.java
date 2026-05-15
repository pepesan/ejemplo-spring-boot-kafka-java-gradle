package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private OrderProducer orderProducer;

    @SuppressWarnings("unchecked")
    @Test
    void send_delegatesToKafkaTemplate() {
        OrderMessage order = OrderMessage.builder()
                .id("ORD-001")
                .customerId("CUST-42")
                .lines(List.of(
                        OrderMessage.OrderLine.builder()
                                .productId("1")
                                .productName("TV 4K")
                                .quantity(2)
                                .unitPrice(new BigDecimal("499.99"))
                                .build()
                ))
                .total(new BigDecimal("999.98"))
                .createdAt(LocalDateTime.of(2026, 5, 14, 19, 0))
                .build();

        when(kafkaTemplate.send(eq("orders"), eq("ORD-001"), any()))
                .thenReturn(CompletableFuture.completedFuture(org.mockito.Mockito.mock(SendResult.class)));

        orderProducer.send(order);

        verify(kafkaTemplate).send("orders", "ORD-001", order);
    }
}