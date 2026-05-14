package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class OrderConsumer {

    @Getter
    private final List<OrderMessage> receivedOrders = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "orders", groupId = "${spring.application.name}")
    public void consume(OrderMessage order) {
        log.info("[CONSUMER] Mensaje recibido del topic 'orders' | key={} | order={}", order.getId(), order);
        receivedOrders.add(order);
    }
}