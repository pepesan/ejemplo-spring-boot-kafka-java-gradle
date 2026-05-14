package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.acceptance;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers.OrderConsumer;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
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
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"orders"})
@DirtiesContext
@Import(KafkaTestConfig.class)
class OrderFlowAcceptanceTest {

    @LocalServerPort
    private int port;

    @Autowired
    private OrderConsumer orderConsumer;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
        orderConsumer.getReceivedOrders().clear();
    }

    @Test
    void sendOrder_isConsumedAndAvailableViaRest() {
        OrderMessage order = OrderMessage.builder()
                .id("ORD-ACC-001")
                .customerId("CUST-42")
                .lines(List.of(
                        OrderMessage.OrderLine.builder()
                                .productId("1")
                                .productName("TV 4K")
                                .quantity(2)
                                .unitPrice(new BigDecimal("499.99"))
                                .build()
                ))
                .total(new BigDecimal("999.98"))
                .createdAt(LocalDateTime.of(2026, 5, 14, 19, 0))
                .build();

        // Enviar pedido via REST → llega al topic de Kafka
        webTestClient.post()
                .uri("/orders")
                .bodyValue(order)
                .exchange()
                .expectStatus().isAccepted()
                .expectBody(OrderMessage.class)
                .isEqualTo(order);

        // Esperar a que el consumer reciba el mensaje del topic
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(orderConsumer.getReceivedOrders())
                                .hasSize(1)
                                .first()
                                .extracting(OrderMessage::getId)
                                .isEqualTo("ORD-ACC-001")
                );

        // Verificar que el endpoint del consumer lo devuelve
        webTestClient.get()
                .uri("/orders/consumed")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(OrderMessage.class)
                .hasSize(1)
                .contains(order);
    }
}