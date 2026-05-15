package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderPaymentEvent;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories.OrderPaymentEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderPaymentConsumer {

    private final OrderPaymentEventRepository repository;

    @KafkaListener(topics = "orders-payments", groupId = "${spring.application.name}")
    public void consume(OrderPaymentEvent event) {
        log.info("[CONSUMER] orders-payments | orderId={} | paymentId={} | status={}",
                event.getOrderId(), event.getPaymentId(), event.getPaymentStatus());
        repository.save(event);
    }

    public List<OrderPaymentEvent> getReceivedEvents() {
        return repository.findAll();
    }

    public void clearAll() {
        repository.deleteAll();
    }
}