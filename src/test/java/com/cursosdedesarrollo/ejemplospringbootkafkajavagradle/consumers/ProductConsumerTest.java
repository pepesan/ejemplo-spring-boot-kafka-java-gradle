package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class ProductConsumerTest {

    private final ProductConsumer consumer = new ProductConsumer();

    @Test
    void consume_storesProductInMemory() {
        ProductMessage product = ProductMessage.builder()
                .id("1")
                .name("TV 4K")
                .price(new BigDecimal("499.99"))
                .stock(10)
                .build();

        consumer.consume(product);

        assertThat(consumer.getReceivedProducts()).hasSize(1).contains(product);
    }

    @Test
    void consume_accumulatesMultipleProducts() {
        ProductMessage p1 = ProductMessage.builder().id("1").name("TV 4K").build();
        ProductMessage p2 = ProductMessage.builder().id("2").name("Altavoz").build();

        consumer.consume(p1);
        consumer.consume(p2);

        assertThat(consumer.getReceivedProducts()).hasSize(2).containsExactly(p1, p2);
    }
}