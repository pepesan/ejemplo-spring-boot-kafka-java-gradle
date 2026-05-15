package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderConsumerTest {

    private final OrderConsumer consumer = new OrderConsumer();

    @Test
    void consume_storesOrderInMemory() {
        OrderMessage order = buildOrder("ORD-001");

        consumer.consume(order);

        assertThat(consumer.getReceivedOrders()).hasSize(1).contains(order);
    }

    @Test
    void consume_accumulatesMultipleOrders() {
        OrderMessage o1 = buildOrder("ORD-001");
        OrderMessage o2 = buildOrder("ORD-002");

        consumer.consume(o1);
        consumer.consume(o2);

        assertThat(consumer.getReceivedOrders()).hasSize(2).containsExactly(o1, o2);
    }

    private OrderMessage buildOrder(String id) {
        return OrderMessage.builder()
                .id(id)
                .customerId("CUST-42")
                .lines(List.of(
                        OrderMessage.OrderLine.builder()
                                .productId("1")
                                .productName("TV 4K")
                                .quantity(1)
                                .unitPrice(new BigDecimal("499.99"))
                                .build()
                ))
                .total(new BigDecimal("499.99"))
                .createdAt(Instant.parse("2026-05-14T19:00:00Z"))
                .build();
    }
}