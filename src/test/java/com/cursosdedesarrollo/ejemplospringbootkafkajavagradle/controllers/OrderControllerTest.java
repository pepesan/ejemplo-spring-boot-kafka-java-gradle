package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.OrderConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.OrderProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@WebFluxTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private OrderProducer orderProducer;

    @MockitoBean
    private OrderConsumer orderConsumer;

    @Test
    void sendOrder_returns202AndOrder() {
        OrderMessage order = buildOrder();

        doNothing().when(orderProducer).send(any());

        webTestClient.post()
                .uri("/orders")
                .bodyValue(order)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(OrderMessage.class)
                .isEqualTo(order);

        verify(orderProducer).send(order);
    }

    @Test
    void getConsumedOrders_returnsListFromConsumer() {
        OrderMessage order = buildOrder();
        when(orderConsumer.getReceivedOrders()).thenReturn(List.of(order));

        webTestClient.get()
                .uri("/orders/consumed")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(OrderMessage.class)
                .hasSize(1)
                .contains(order);
    }

    private OrderMessage buildOrder() {
        return OrderMessage.builder()
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
                .createdAt(Instant.parse("2026-05-14T19:00:00Z"))
                .build();
    }
}