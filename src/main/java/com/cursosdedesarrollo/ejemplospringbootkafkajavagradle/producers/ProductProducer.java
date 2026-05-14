package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductProducer {

    static final String TOPIC = "products";

    private final KafkaTemplate<String, ProductMessage> kafkaTemplate;

    public void send(ProductMessage product) {
        log.info("Sending product: {}", product);
        kafkaTemplate.send(TOPIC, product.getId(), product);
    }
}