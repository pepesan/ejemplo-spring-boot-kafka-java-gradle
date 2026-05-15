package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderPaymentEvent;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OrderPaymentJoinTopologyTest {

    @TempDir
    Path stateDir;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, OrderMessage> ordersTopic;
    private TestInputTopic<String, PaymentEvent> paymentsTopic;
    private TestOutputTopic<String, OrderPaymentEvent> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new OrderPaymentJoinTopology().ordersPaymentsJoinStream(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-join-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toAbsolutePath().toString());

        testDriver = new TopologyTestDriver(builder.build(), props);

        Serde<OrderMessage>      orderSerde  = OrderPaymentJoinTopology.orderMessageSerde();
        Serde<PaymentEvent>      paymentSerde = PaymentStreamsTopology.paymentSerde();
        Serde<OrderPaymentEvent> joinedSerde  = OrderPaymentJoinTopology.orderPaymentSerde();

        ordersTopic = testDriver.createInputTopic(
                OrderPaymentJoinTopology.ORDERS_TOPIC, new StringSerializer(), orderSerde.serializer());
        paymentsTopic = testDriver.createInputTopic(
                PaymentStreamsTopology.INPUT_TOPIC, new StringSerializer(), paymentSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
                OrderPaymentJoinTopology.OUTPUT_TOPIC, new StringDeserializer(), joinedSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void pagoConPedidoConocido_produceEventoJoin() {
        ordersTopic.pipeInput("ORD-001", buildOrder("ORD-001", "CUST-42", new BigDecimal("999.98")));
        paymentsTopic.pipeInput("PAY-001", buildPayment("PAY-001", "ORD-001", new BigDecimal("999.98"), "APPROVED"));

        List<OrderPaymentEvent> results = outputTopic.readValuesToList();

        assertThat(results).hasSize(1);
        OrderPaymentEvent joined = results.get(0);
        assertThat(joined.getOrderId()).isEqualTo("ORD-001");
        assertThat(joined.getCustomerId()).isEqualTo("CUST-42");
        assertThat(joined.getOrderTotal()).isEqualByComparingTo("999.98");
        assertThat(joined.getPaymentId()).isEqualTo("PAY-001");
        assertThat(joined.getPaymentAmount()).isEqualByComparingTo("999.98");
        assertThat(joined.getPaymentStatus()).isEqualTo("APPROVED");
        assertThat(joined.getPaymentCurrency()).isEqualTo("EUR");
    }

    @Test
    void pagoSinPedidoConocido_noProduceOutput() {
        paymentsTopic.pipeInput("PAY-002", buildPayment("PAY-002", "ORD-DESCONOCIDO", new BigDecimal("500"), "PENDING"));

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void variosPagesPorElMismoPedido_cadaPagoProduceUnEvento() {
        ordersTopic.pipeInput("ORD-001", buildOrder("ORD-001", "CUST-42", new BigDecimal("1500.00")));

        paymentsTopic.pipeInput("PAY-001", buildPayment("PAY-001", "ORD-001", new BigDecimal("500.00"), "APPROVED"));
        paymentsTopic.pipeInput("PAY-002", buildPayment("PAY-002", "ORD-001", new BigDecimal("1000.00"), "APPROVED"));

        List<OrderPaymentEvent> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(2);
        assertThat(results).extracting(OrderPaymentEvent::getPaymentId)
                .containsExactly("PAY-001", "PAY-002");
    }

    @Test
    void pedidoActualizado_pagoUsaNuevosDatos() {
        ordersTopic.pipeInput("ORD-001", buildOrder("ORD-001", "CUST-ORIGINAL", new BigDecimal("100.00")));
        ordersTopic.pipeInput("ORD-001", buildOrder("ORD-001", "CUST-UPDATED", new BigDecimal("200.00")));

        paymentsTopic.pipeInput("PAY-001", buildPayment("PAY-001", "ORD-001", new BigDecimal("200.00"), "APPROVED"));

        List<OrderPaymentEvent> results = outputTopic.readValuesToList();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).getCustomerId()).isEqualTo("CUST-UPDATED");
        assertThat(results.get(0).getOrderTotal()).isEqualByComparingTo("200.00");
    }

    private OrderMessage buildOrder(String id, String customerId, BigDecimal total) {
        return OrderMessage.builder()
                .id(id)
                .customerId(customerId)
                .total(total)
                .createdAt(Instant.parse("2026-05-15T10:00:00Z"))
                .build();
    }

    private PaymentEvent buildPayment(String id, String orderId, BigDecimal amount, String status) {
        return PaymentEvent.builder()
                .id(id)
                .orderId(orderId)
                .amount(amount)
                .currency("EUR")
                .status(status)
                .build();
    }
}