package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories.ProductMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductConsumer {

    private final ProductMessageRepository repository;

    @KafkaListener(topics = "products", groupId = "${spring.application.name}")
    public void consume(ProductMessage product) {
        log.info("[CONSUMER] Mensaje recibido del topic 'products' | key={} | product={}", product.getId(), product);
        repository.save(product);
    }

    public List<ProductMessage> getReceivedProducts() {
        return repository.findAll();
    }

    public void clearAll() {
        repository.deleteAll();
    }
}