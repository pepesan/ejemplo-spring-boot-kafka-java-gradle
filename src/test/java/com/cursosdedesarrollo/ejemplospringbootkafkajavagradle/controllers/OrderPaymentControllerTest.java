package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.OrderPaymentConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderPaymentEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.when;

@WebFluxTest(OrderPaymentController.class)
class OrderPaymentControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private OrderPaymentConsumer consumer;

    @Test
    void getConsumed_devuelveEventosJoin() {
        OrderPaymentEvent event = buildEvent("ORD-001", "PAY-001");
        when(consumer.getReceivedEvents()).thenReturn(List.of(event));

        webTestClient.get()
                .uri("/orders-payments/consumed")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(OrderPaymentEvent.class)
                .hasSize(1)
                .contains(event);
    }

    @Test
    void getConsumed_devuelveListaVaciaSinEventos() {
        when(consumer.getReceivedEvents()).thenReturn(List.of());

        webTestClient.get()
                .uri("/orders-payments/consumed")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(OrderPaymentEvent.class)
                .hasSize(0);
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