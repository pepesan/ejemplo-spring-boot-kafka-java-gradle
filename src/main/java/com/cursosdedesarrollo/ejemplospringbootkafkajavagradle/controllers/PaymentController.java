package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.PaymentProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentProducer paymentProducer;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<PaymentEvent> send(@Valid @RequestBody PaymentEvent payment) {
        log.info("[CONTROLLER] POST /payments | id={} | amount={}", payment.getId(), payment.getAmount());
        return Mono.fromRunnable(() -> paymentProducer.send(payment))
                .thenReturn(payment);
    }
}