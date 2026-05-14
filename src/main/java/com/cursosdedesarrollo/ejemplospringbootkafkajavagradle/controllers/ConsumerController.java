package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.ProductConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@Slf4j
@RestController
@RequestMapping("/consumed")
@RequiredArgsConstructor
public class ConsumerController {

    private final ProductConsumer productConsumer;

    @GetMapping("/products")
    public Flux<ProductMessage> getConsumedProducts() {
        log.info("[CONTROLLER] GET /consumed/products | total={}", productConsumer.getReceivedProducts().size());
        return Flux.fromIterable(productConsumer.getReceivedProducts());
    }
}