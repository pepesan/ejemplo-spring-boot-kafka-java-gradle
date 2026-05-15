package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories.OrderMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderConsumer {

    private final OrderMessageRepository repository;

    @KafkaListener(topics = "orders", groupId = "${spring.application.name}")
    public void consume(OrderMessage order) {
        log.info("[CONSUMER] Mensaje recibido del topic 'orders' | key={} | order={}", order.getId(), order);
        repository.save(order);
    }

    public List<OrderMessage> getReceivedOrders() {
        return repository.findAll();
    }

    public void clearAll() {
        repository.deleteAll();
    }
}