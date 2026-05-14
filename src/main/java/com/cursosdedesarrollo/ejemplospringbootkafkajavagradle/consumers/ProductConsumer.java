package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class ProductConsumer {

    @Getter
    private final List<ProductMessage> receivedProducts = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "products", groupId = "${spring.application.name}")
    public void consume(ProductMessage product) {
        log.info("[CONSUMER] Mensaje recibido del topic 'products' | key={} | product={}", product.getId(), product);
        receivedProducts.add(product);
    }
}