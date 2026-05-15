package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.acceptance;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.streams.state-dir=/tmp/kafka-streams-payment-streams-test"
)
@EmbeddedKafka(partitions = 1, topics = {"payments", "payments-high-value", "orders", "orders-payments"})
@DirtiesContext
@Import(KafkaTestConfig.class)
class PaymentStreamsAcceptanceTest {

    @LocalServerPort
    private int port;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(15))
                .build();

        // Esperar a que Kafka Streams alcance estado RUNNING antes de cada test
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
                    return streams != null && streams.state() == KafkaStreams.State.RUNNING;
                });
    }

    @Test
    void pagoAprobado_incrementaContadorEnStateStore() {
        sendPayment("PAY-STREAM-001", "APPROVED", "500.00");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        webTestClient.get().uri("/payments/count/APPROVED")
                                .exchange()
                                .expectStatus().isOk()
                                .expectBody(Long.class).isEqualTo(1L)
                );
    }

    @Test
    void pagoAltoValor_seCuentaIgualEnStateStore() {
        // amount > 1000 → también va al topic payments-high-value, pero sigue contándose
        sendPayment("PAY-HIGH-001", "PENDING", "2000.00");

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        webTestClient.get().uri("/payments/count/PENDING")
                                .exchange()
                                .expectStatus().isOk()
                                .expectBody(Long.class).isEqualTo(1L)
                );
    }

    private void sendPayment(String id, String status, String amount) {
        webTestClient.post().uri("/payments")
                .bodyValue(PaymentEvent.builder()
                        .id(id)
                        .orderId("ORD-001")
                        .amount(new BigDecimal(amount))
                        .currency("EUR")
                        .status(status)
                        .build())
                .exchange()
                .expectStatus().isAccepted();
    }
}