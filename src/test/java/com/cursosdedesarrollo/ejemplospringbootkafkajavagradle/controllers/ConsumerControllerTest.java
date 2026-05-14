package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.ProductConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.when;

@WebFluxTest(ConsumerController.class)
class ConsumerControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private ProductConsumer productConsumer;

    @Test
    void getConsumedProducts_returnsListFromConsumer() {
        ProductMessage product = ProductMessage.builder()
                .id("1")
                .name("TV 4K")
                .description("Televisor 55 pulgadas")
                .price(new BigDecimal("499.99"))
                .stock(10)
                .build();
        when(productConsumer.getReceivedProducts()).thenReturn(List.of(product));

        webTestClient.get()
                .uri("/consumed/products")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ProductMessage.class)
                .hasSize(1)
                .contains(product);
    }

    @Test
    void getConsumedProducts_returnsEmptyListWhenNoneConsumed() {
        when(productConsumer.getReceivedProducts()).thenReturn(List.of());

        webTestClient.get()
                .uri("/consumed/products")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ProductMessage.class)
                .hasSize(0);
    }
}