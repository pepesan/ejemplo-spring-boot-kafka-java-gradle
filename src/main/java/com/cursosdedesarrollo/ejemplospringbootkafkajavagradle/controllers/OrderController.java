package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.OrderConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.OrderProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;
    private final OrderConsumer orderConsumer;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<OrderMessage> send(@Valid @RequestBody OrderMessage order) {
        log.info("[CONTROLLER] POST /orders | key={} | order={}", order.getId(), order);
        return Mono.fromRunnable(() -> orderProducer.send(order))
                .thenReturn(order);
    }

    @GetMapping("/consumed")
    public Flux<OrderMessage> getConsumedOrders() {
        log.info("[CONTROLLER] GET /orders/consumed | total={}", orderConsumer.getReceivedOrders().size());
        return Flux.fromIterable(orderConsumer.getReceivedOrders());
    }
}