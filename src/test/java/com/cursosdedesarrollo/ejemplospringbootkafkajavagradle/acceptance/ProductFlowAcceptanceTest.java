package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.acceptance;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.ProductConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.kafka.streams.state-dir=/tmp/kafka-streams-product-test")
@EmbeddedKafka(partitions = 1, topics = {"products"})
@DirtiesContext
@Import(KafkaTestConfig.class)
class ProductFlowAcceptanceTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ProductConsumer productConsumer;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
        productConsumer.getReceivedProducts().clear();
    }

    @Test
    void sendProduct_isConsumedAndAvailableViaRest() {
        ProductMessage product = ProductMessage.builder()
                .id("ACC-1")
                .name("TV 4K")
                .description("Televisor 55 pulgadas")
                .price(new BigDecimal("499.99"))
                .stock(10)
                .build();

        // Enviar producto via REST → llega al topic de Kafka
        webTestClient.post()
                .uri("/products")
                .bodyValue(product)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(ProductMessage.class)
                .isEqualTo(product);

        // Esperar a que el consumer reciba el mensaje del topic
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(productConsumer.getReceivedProducts())
                                .hasSize(1)
                                .first()
                                .extracting(ProductMessage::getId)
                                .isEqualTo("ACC-1")
                );

        // Verificar que el endpoint del consumer lo devuelve
        webTestClient.get()
                .uri("/consumed/products")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ProductMessage.class)
                .hasSize(1)
                .contains(product);
    }

    @Test
    void sendMultipleProducts_allConsumed() {
        ProductMessage p1 = ProductMessage.builder().id("ACC-2").name("TV 4K").price(new BigDecimal("499.99")).stock(5).build();
        ProductMessage p2 = ProductMessage.builder().id("ACC-3").name("Altavoz").price(new BigDecimal("99.99")).stock(20).build();

        webTestClient.post().uri("/products").bodyValue(p1).exchange().expectStatus().isAccepted();
        webTestClient.post().uri("/products").bodyValue(p2).exchange().expectStatus().isAccepted();

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(productConsumer.getReceivedProducts()).hasSize(2)
                );
    }
}