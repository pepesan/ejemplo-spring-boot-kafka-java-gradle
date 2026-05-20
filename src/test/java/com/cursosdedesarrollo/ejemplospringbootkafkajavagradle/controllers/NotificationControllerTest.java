package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.NotificationConsumerPartition0;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.NotificationConsumerPartition1;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.NotificationMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.NotificationProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@WebFluxTest(NotificationController.class)
class NotificationControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private NotificationProducer notificationProducer;

    @MockitoBean
    private NotificationConsumerPartition0 consumerPartition0;

    @MockitoBean
    private NotificationConsumerPartition1 consumerPartition1;

    @Test
    void send_returns202AndNotification() {
        NotificationMessage notification = buildNotification("NOTIF-001");
        doNothing().when(notificationProducer).send(any());

        webTestClient.post()
                .uri("/notifications")
                .bodyValue(notification)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(NotificationMessage.class)
                .isEqualTo(notification);

        verify(notificationProducer).send(notification);
    }

    @Test
    void getConsumedPartition0_returnsListFromConsumer() {
        NotificationMessage notification = buildNotification("NOTIF-001");
        when(consumerPartition0.getReceived()).thenReturn(List.of(notification));

        webTestClient.get()
                .uri("/notifications/consumed/partition/0")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(NotificationMessage.class)
                .hasSize(1)
                .contains(notification);
    }

    @Test
    void getConsumedPartition1_returnsListFromConsumer() {
        NotificationMessage notification = buildNotification("NOTIF-002");
        when(consumerPartition1.getReceived()).thenReturn(List.of(notification));

        webTestClient.get()
                .uri("/notifications/consumed/partition/1")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(NotificationMessage.class)
                .hasSize(1)
                .contains(notification);
    }

    @Test
    void send_withBlankRecipient_returns400() {
        NotificationMessage notification = NotificationMessage.builder()
                .id("NOTIF-001")
                .recipient("")
                .subject("Bienvenido")
                .body("Tu cuenta ha sido creada")
                .createdAt(Instant.parse("2026-05-20T10:00:00Z"))
                .build();

        webTestClient.post()
                .uri("/notifications")
                .bodyValue(notification)
                .exchange()
                .expectStatus().isBadRequest();

        verifyNoInteractions(notificationProducer);
    }

    @Test
    void send_withBlankSubject_returns400() {
        NotificationMessage notification = NotificationMessage.builder()
                .id("NOTIF-001")
                .recipient("user@example.com")
                .subject("")
                .body("Tu cuenta ha sido creada")
                .createdAt(Instant.parse("2026-05-20T10:00:00Z"))
                .build();

        webTestClient.post()
                .uri("/notifications")
                .bodyValue(notification)
                .exchange()
                .expectStatus().isBadRequest();

        verifyNoInteractions(notificationProducer);
    }

    @Test
    void send_withBlankBody_returns400() {
        NotificationMessage notification = NotificationMessage.builder()
                .id("NOTIF-001")
                .recipient("user@example.com")
                .subject("Bienvenido")
                .body("")
                .createdAt(Instant.parse("2026-05-20T10:00:00Z"))
                .build();

        webTestClient.post()
                .uri("/notifications")
                .bodyValue(notification)
                .exchange()
                .expectStatus().isBadRequest();

        verifyNoInteractions(notificationProducer);
    }

    private NotificationMessage buildNotification(String id) {
        return NotificationMessage.builder()
                .id(id)
                .recipient("user@example.com")
                .subject("Bienvenido")
                .body("Tu cuenta ha sido creada")
                .createdAt(Instant.parse("2026-05-20T10:00:00Z"))
                .build();
    }
}
