package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private PaymentProducer paymentProducer;

    @SuppressWarnings("unchecked")
    @Test
    void send_delegatesToKafkaTemplate() {
        PaymentEvent payment = PaymentEvent.builder()
                .id("PAY-001")
                .orderId("ORD-001")
                .amount(new BigDecimal("1500.00"))
                .currency("EUR")
                .status("APPROVED")
                .build();

        when(kafkaTemplate.send(eq("payments"), eq("PAY-001"), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        paymentProducer.send(payment);

        verify(kafkaTemplate).send("payments", "PAY-001", payment);
    }
}