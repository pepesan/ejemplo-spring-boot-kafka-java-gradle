package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories.OrderMessageRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderConsumerTest {

    @Mock
    private OrderMessageRepository repository;

    @InjectMocks
    private OrderConsumer consumer;

    @Test
    void consume_storesOrderInDb() {
        OrderMessage order = buildOrder("ORD-001");
        when(repository.findAll()).thenReturn(List.of(order));

        consumer.consume(order);

        verify(repository).save(order);
        assertThat(consumer.getReceivedOrders()).hasSize(1).contains(order);
    }

    @Test
    void consume_accumulatesMultipleOrders() {
        OrderMessage o1 = buildOrder("ORD-001");
        OrderMessage o2 = buildOrder("ORD-002");
        when(repository.findAll()).thenReturn(List.of(o1, o2));

        consumer.consume(o1);
        consumer.consume(o2);

        assertThat(consumer.getReceivedOrders()).hasSize(2).containsExactlyInAnyOrder(o1, o2);
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