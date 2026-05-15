package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.PaymentProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@WebFluxTest(PaymentController.class)
class PaymentControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private PaymentProducer paymentProducer;

    @Test
    void sendPayment_returns202AndPayment() {
        PaymentEvent payment = buildPayment();
        doNothing().when(paymentProducer).send(any());

        webTestClient.post()
                .uri("/payments")
                .bodyValue(payment)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(PaymentEvent.class)
                .isEqualTo(payment);

        verify(paymentProducer).send(payment);
    }

    @Test
    void sendPayment_withNullAmount_returns400() {
        PaymentEvent payment = PaymentEvent.builder()
                .id("PAY-001")
                .orderId("ORD-001")
                .amount(null)
                .currency("EUR")
                .status("APPROVED")
                .build();

        webTestClient.post()
                .uri("/payments")
                .bodyValue(payment)
                .exchange()
                .expectStatus().isBadRequest();

        verifyNoInteractions(paymentProducer);
    }

    @Test
    void sendPayment_withInvalidStatus_returns400() {
        PaymentEvent payment = PaymentEvent.builder()
                .id("PAY-001")
                .orderId("ORD-001")
                .amount(new BigDecimal("100.00"))
                .currency("EUR")
                .status("DESCONOCIDO")
                .build();

        webTestClient.post()
                .uri("/payments")
                .bodyValue(payment)
                .exchange()
                .expectStatus().isBadRequest();

        verifyNoInteractions(paymentProducer);
    }

    private PaymentEvent buildPayment() {
        return PaymentEvent.builder()
                .id("PAY-001")
                .orderId("ORD-001")
                .amount(new BigDecimal("1500.00"))
                .currency("EUR")
                .status("APPROVED")
                .build();
    }
}