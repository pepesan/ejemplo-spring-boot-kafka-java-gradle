package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationProducer {

    static final String TOPIC = "notifications";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(NotificationMessage notification) {
        log.info("[PRODUCER] Enviando mensaje al topic '{}' | key={} | recipient={}",
                TOPIC, notification.getId(), notification.getRecipient());
        kafkaTemplate.send(TOPIC, notification.getId(), notification)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[PRODUCER] Error al enviar notificación id={} | error={}",
                                notification.getId(), ex.getMessage());
                    } else {
                        log.info("[PRODUCER] Mensaje enviado correctamente | topic={} | partition={} | offset={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
