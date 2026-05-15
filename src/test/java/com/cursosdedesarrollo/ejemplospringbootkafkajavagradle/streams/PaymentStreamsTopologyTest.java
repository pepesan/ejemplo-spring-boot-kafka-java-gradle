package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentStreamsTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, PaymentEvent> inputTopic;
    private TestOutputTopic<String, PaymentEvent> highValueOutputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new PaymentStreamsTopology().paymentsStream(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-payment-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(builder.build(), props);

        Serde<PaymentEvent> serde = PaymentStreamsTopology.paymentSerde();
        inputTopic = testDriver.createInputTopic(
                PaymentStreamsTopology.INPUT_TOPIC, new StringSerializer(), serde.serializer());
        highValueOutputTopic = testDriver.createOutputTopic(
                PaymentStreamsTopology.HIGH_VALUE_TOPIC, new StringDeserializer(), serde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void pagoSuperiorAlUmbral_seEnrutaAlTopicDeAltoValor() {
        PaymentEvent payment = buildPayment("PAY-001", new BigDecimal("1500.00"), "APPROVED");
        inputTopic.pipeInput("PAY-001", payment);

        assertThat(highValueOutputTopic.readValuesToList())
                .hasSize(1)
                .contains(payment);
    }

    @Test
    void pagoIgualAlUmbral_noSeEnruta() {
        inputTopic.pipeInput("PAY-002", buildPayment("PAY-002", new BigDecimal("1000.00"), "APPROVED"));

        assertThat(highValueOutputTopic.isEmpty()).isTrue();
    }

    @Test
    void pagoInferiorAlUmbral_noSeEnruta() {
        inputTopic.pipeInput("PAY-003", buildPayment("PAY-003", new BigDecimal("200.00"), "PENDING"));

        assertThat(highValueOutputTopic.isEmpty()).isTrue();
    }

    @Test
    void contadorDePagos_seAgregaPorEstado() {
        inputTopic.pipeInput("PAY-001", buildPayment("PAY-001", new BigDecimal("100"), "APPROVED"));
        inputTopic.pipeInput("PAY-002", buildPayment("PAY-002", new BigDecimal("200"), "APPROVED"));
        inputTopic.pipeInput("PAY-003", buildPayment("PAY-003", new BigDecimal("300"), "REJECTED"));
        inputTopic.pipeInput("PAY-004", buildPayment("PAY-004", new BigDecimal("400"), "PENDING"));

        KeyValueStore<String, Long> store = testDriver.getKeyValueStore(PaymentStreamsTopology.COUNT_STORE);
        assertThat(store.get("APPROVED")).isEqualTo(2L);
        assertThat(store.get("REJECTED")).isEqualTo(1L);
        assertThat(store.get("PENDING")).isEqualTo(1L);
    }

    @Test
    void pagoDeAltoValor_seContabilizaYSeEnruta() {
        PaymentEvent payment = buildPayment("PAY-005", new BigDecimal("2000.00"), "APPROVED");
        inputTopic.pipeInput("PAY-005", payment);

        assertThat(highValueOutputTopic.readValuesToList()).hasSize(1);
        KeyValueStore<String, Long> store = testDriver.getKeyValueStore(PaymentStreamsTopology.COUNT_STORE);
        assertThat(store.get("APPROVED")).isEqualTo(1L);
    }

    private PaymentEvent buildPayment(String id, BigDecimal amount, String status) {
        return PaymentEvent.builder()
                .id(id)
                .orderId("ORD-001")
                .amount(amount)
                .currency("EUR")
                .status(status)
                .build();
    }
}