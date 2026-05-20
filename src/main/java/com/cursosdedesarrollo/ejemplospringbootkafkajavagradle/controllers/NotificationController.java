package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.NotificationConsumerPartition0;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.NotificationConsumerPartition1;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.NotificationMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.NotificationProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationProducer notificationProducer;
    private final NotificationConsumerPartition0 consumerPartition0;
    private final NotificationConsumerPartition1 consumerPartition1;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<NotificationMessage> send(@Valid @RequestBody NotificationMessage notification) {
        log.info("[CONTROLLER] POST /notifications | key={} | recipient={}", notification.getId(), notification.getRecipient());
        return Mono.fromRunnable(() -> notificationProducer.send(notification))
                .thenReturn(notification);
    }

    @GetMapping("/consumed/partition/0")
    public Flux<NotificationMessage> getConsumedPartition0() {
        log.info("[CONTROLLER] GET /notifications/consumed/partition/0 | total={}", consumerPartition0.getReceived().size());
        return Flux.fromIterable(consumerPartition0.getReceived());
    }

    @GetMapping("/consumed/partition/1")
    public Flux<NotificationMessage> getConsumedPartition1() {
        log.info("[CONTROLLER] GET /notifications/consumed/partition/1 | total={}", consumerPartition1.getReceived().size());
        return Flux.fromIterable(consumerPartition1.getReceived());
    }
}
