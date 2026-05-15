package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderPaymentEvent;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories.OrderPaymentEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderPaymentConsumerTest {

    @Mock
    private OrderPaymentEventRepository repository;

    @InjectMocks
    private OrderPaymentConsumer consumer;

    @Test
    void consume_almacenaEventoEnDb() {
        OrderPaymentEvent event = buildEvent("ORD-001", "PAY-001");
        when(repository.findAll()).thenReturn(List.of(event));

        consumer.consume(event);

        verify(repository).save(event);
        assertThat(consumer.getReceivedEvents()).hasSize(1).contains(event);
    }

    @Test
    void consume_acumulaVariosEventos() {
        OrderPaymentEvent e1 = buildEvent("ORD-001", "PAY-001");
        OrderPaymentEvent e2 = buildEvent("ORD-002", "PAY-002");
        when(repository.findAll()).thenReturn(List.of(e1, e2));

        consumer.consume(e1);
        consumer.consume(e2);

        assertThat(consumer.getReceivedEvents()).hasSize(2).containsExactlyInAnyOrder(e1, e2);
    }

    private OrderPaymentEvent buildEvent(String orderId, String paymentId) {
        return OrderPaymentEvent.builder()
                .orderId(orderId)
                .customerId("CUST-42")
                .orderTotal(new BigDecimal("999.98"))
                .paymentId(paymentId)
                .paymentAmount(new BigDecimal("999.98"))
                .paymentCurrency("EUR")
                .paymentStatus("APPROVED")
                .build();
    }
}