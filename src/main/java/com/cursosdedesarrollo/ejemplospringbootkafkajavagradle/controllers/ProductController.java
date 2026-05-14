package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.ProductProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
public class ProductController {

    private final ProductProducer productProducer;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<ProductMessage> send(@RequestBody ProductMessage product) {
        return Mono.fromRunnable(() -> productProducer.send(product))
                .thenReturn(product);
    }
}