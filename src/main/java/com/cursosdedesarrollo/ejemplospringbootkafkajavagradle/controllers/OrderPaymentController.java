package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.OrderPaymentConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderPaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@Slf4j
@RestController
@RequestMapping("/orders-payments")
@RequiredArgsConstructor
public class OrderPaymentController {

    private final OrderPaymentConsumer consumer;

    @GetMapping("/consumed")
    public Flux<OrderPaymentEvent> getConsumed() {
        log.info("[CONTROLLER] GET /orders-payments/consumed | total={}", consumer.getReceivedEvents().size());
        return Flux.fromIterable(consumer.getReceivedEvents());
    }
}