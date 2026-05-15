package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.ProductProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
public class
ProductController {

    private final ProductProducer productProducer;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<ProductMessage> send(@Valid @RequestBody ProductMessage product) {
        log.info("[CONTROLLER] POST /products | key={} | product={}", product.getId(), product);
        return Mono.fromRunnable(() -> productProducer.send(product))
                .thenReturn(product);
    }
}