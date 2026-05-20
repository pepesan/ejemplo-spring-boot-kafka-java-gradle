package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.NotificationMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationConsumerPartition0Test {

    private NotificationConsumerPartition0 consumer;

    @BeforeEach
    void setUp() {
        consumer = new NotificationConsumerPartition0();
    }

    @Test
    void consume_addsToReceivedList() {
        NotificationMessage notification = buildNotification("NOTIF-001");

        consumer.consume(notification);

        assertThat(consumer.getReceived()).hasSize(1).contains(notification);
    }

    @Test
    void consume_accumulatesMultipleNotifications() {
        NotificationMessage n1 = buildNotification("NOTIF-001");
        NotificationMessage n2 = buildNotification("NOTIF-002");

        consumer.consume(n1);
        consumer.consume(n2);

        assertThat(consumer.getReceived()).hasSize(2).containsExactly(n1, n2);
    }

    @Test
    void clearAll_emptiesTheList() {
        consumer.consume(buildNotification("NOTIF-001"));
        consumer.consume(buildNotification("NOTIF-002"));

        consumer.clearAll();

        assertThat(consumer.getReceived()).isEmpty();
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
