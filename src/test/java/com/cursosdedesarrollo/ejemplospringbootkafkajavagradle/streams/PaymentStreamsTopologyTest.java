package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
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

    @Test
    void ventanaTemporal_pagosDentroDeUnaVentanaSeContabilizan() {
        Instant base = Instant.ofEpochMilli(0);

        inputTopic.pipeInput("PAY-1", buildPayment("PAY-1", new BigDecimal("100"), "APPROVED"), base.plusSeconds(10));
        inputTopic.pipeInput("PAY-2", buildPayment("PAY-2", new BigDecimal("200"), "APPROVED"), base.plusSeconds(20));
        inputTopic.pipeInput("PAY-3", buildPayment("PAY-3", new BigDecimal("50"),  "REJECTED"), base.plusSeconds(30));

        WindowStore<String, Long> windowStore = testDriver.getWindowStore(PaymentStreamsTopology.WINDOW_COUNT_STORE);

        try (WindowStoreIterator<Long> iter = windowStore.fetch("APPROVED", base, base.plusSeconds(59))) {
            assertThat(iter.hasNext()).isTrue();
            KeyValue<Long, Long> kv = iter.next();
            assertThat(kv.value).isEqualTo(2L);
        }

        try (WindowStoreIterator<Long> iter = windowStore.fetch("REJECTED", base, base.plusSeconds(59))) {
            assertThat(iter.hasNext()).isTrue();
            assertThat(iter.next().value).isEqualTo(1L);
        }
    }

    @Test
    void ventanaTemporal_ventanasDistintasSeContabilizanPorSeparado() {
        Instant window1 = Instant.ofEpochMilli(0);
        Instant window2 = Instant.ofEpochMilli(60_000);

        // 2 pagos en ventana 1 (0–59s)
        inputTopic.pipeInput("PAY-1", buildPayment("PAY-1", new BigDecimal("100"), "APPROVED"), window1.plusSeconds(5));
        inputTopic.pipeInput("PAY-2", buildPayment("PAY-2", new BigDecimal("200"), "APPROVED"), window1.plusSeconds(10));

        // 1 pago en ventana 2 (60–119s)
        inputTopic.pipeInput("PAY-3", buildPayment("PAY-3", new BigDecimal("150"), "APPROVED"), window2.plusSeconds(5));

        WindowStore<String, Long> windowStore = testDriver.getWindowStore(PaymentStreamsTopology.WINDOW_COUNT_STORE);

        try (WindowStoreIterator<Long> iter = windowStore.fetch("APPROVED", window1, window1.plusSeconds(59))) {
            assertThat(iter.hasNext()).isTrue();
            assertThat(iter.next().value).isEqualTo(2L);
        }

        try (WindowStoreIterator<Long> iter = windowStore.fetch("APPROVED", window2, window2.plusSeconds(59))) {
            assertThat(iter.hasNext()).isTrue();
            assertThat(iter.next().value).isEqualTo(1L);
        }
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