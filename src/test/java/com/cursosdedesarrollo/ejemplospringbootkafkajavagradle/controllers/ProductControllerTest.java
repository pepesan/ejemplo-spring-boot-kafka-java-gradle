package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.ProductProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@WebFluxTest(ProductController.class)
class ProductControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockitoBean
    private ProductProducer productProducer;

    @Test
    void sendProduct_returns202AndProduct() {
        ProductMessage product = ProductMessage.builder()
                .id("1")
                .name("TV 4K")
                .description("Televisor 55 pulgadas")
                .price(new BigDecimal("499.99"))
                .stock(10)
                .build();

        doNothing().when(productProducer).send(any());

        webTestClient.post()
                .uri("/products")
                .bodyValue(product)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(ProductMessage.class)
                .isEqualTo(product);

        verify(productProducer).send(product);
    }

    @Test
    void sendProduct_withBlankId_returns400() {
        ProductMessage product = ProductMessage.builder()
                .id("")
                .name("TV 4K")
                .price(new BigDecimal("499.99"))
                .stock(10)
                .build();

        webTestClient.post()
                .uri("/products")
                .bodyValue(product)
                .exchange()
                .expectStatus().isBadRequest();

        verifyNoInteractions(productProducer);
    }

    @Test
    void sendProduct_withNullPrice_returns400() {
        ProductMessage product = ProductMessage.builder()
                .id("1")
                .name("TV 4K")
                .price(null)
                .stock(10)
                .build();

        webTestClient.post()
                .uri("/products")
                .bodyValue(product)
                .exchange()
                .expectStatus().isBadRequest();

        verifyNoInteractions(productProducer);
    }
}
