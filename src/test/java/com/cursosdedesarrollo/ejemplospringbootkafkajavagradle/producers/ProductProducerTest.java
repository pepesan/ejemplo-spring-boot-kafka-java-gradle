package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private ProductProducer productProducer;

    @Test
    void send_delegatesToKafkaTemplate() {
        ProductMessage product = ProductMessage.builder()
                .id("1")
                .name("TV 4K")
                .price(new BigDecimal("499.99"))
                .stock(10)
                .build();

        when(kafkaTemplate.send(eq("products"), eq("1"), any()))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        productProducer.send(product);

        verify(kafkaTemplate).send("products", "1", product);
    }

    private <T> T mock(Class<T> clazz) {
        return org.mockito.Mockito.mock(clazz);
    }
}