package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProducer {

    static final String TOPIC = "payments";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(PaymentEvent payment) {
        log.info("[PRODUCER] Enviando mensaje al topic '{}' | key={} | payment={}", TOPIC, payment.getId(), payment);
        kafkaTemplate.send(TOPIC, payment.getId(), payment)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[PRODUCER] Error al enviar pago id={} | error={}", payment.getId(), ex.getMessage());
                    } else {
                        log.info("[PRODUCER] Mensaje enviado | topic={} | partition={} | offset={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}