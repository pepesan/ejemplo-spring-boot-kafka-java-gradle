package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories.ProductMessageRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductConsumerTest {

    @Mock
    private ProductMessageRepository repository;

    @InjectMocks
    private ProductConsumer consumer;

    @Test
    void consume_storesProductInDb() {
        ProductMessage product = ProductMessage.builder()
                .id("1")
                .name("TV 4K")
                .price(new BigDecimal("499.99"))
                .stock(10)
                .build();
        when(repository.findAll()).thenReturn(List.of(product));

        consumer.consume(product);

        verify(repository).save(product);
        assertThat(consumer.getReceivedProducts()).hasSize(1).contains(product);
    }

    @Test
    void consume_accumulatesMultipleProducts() {
        ProductMessage p1 = ProductMessage.builder().id("1").name("TV 4K").build();
        ProductMessage p2 = ProductMessage.builder().id("2").name("Altavoz").build();
        when(repository.findAll()).thenReturn(List.of(p1, p2));

        consumer.consume(p1);
        consumer.consume(p2);

        assertThat(consumer.getReceivedProducts()).hasSize(2).containsExactlyInAnyOrder(p1, p2);
    }
}