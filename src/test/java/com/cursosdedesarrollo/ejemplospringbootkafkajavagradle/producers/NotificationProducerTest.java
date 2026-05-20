package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.NotificationMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private NotificationProducer notificationProducer;

    @SuppressWarnings("unchecked")
    @Test
    void send_delegatesToKafkaTemplate() {
        NotificationMessage notification = buildNotification("NOTIF-001");

        when(kafkaTemplate.send(eq("notifications"), eq("NOTIF-001"), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        notificationProducer.send(notification);

        verify(kafkaTemplate).send("notifications", "NOTIF-001", notification);
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
