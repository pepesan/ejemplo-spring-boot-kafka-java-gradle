package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    static final String TOPIC = "orders";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(OrderMessage order) {
        log.info("[PRODUCER] Enviando mensaje al topic '{}' | key={} | order={}", TOPIC, order.getId(), order);
        kafkaTemplate.send(TOPIC, order.getId(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[PRODUCER] Error al enviar pedido id={} | error={}", order.getId(), ex.getMessage());
                    } else {
                        log.info("[PRODUCER] Mensaje enviado correctamente | topic={} | partition={} | offset={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}