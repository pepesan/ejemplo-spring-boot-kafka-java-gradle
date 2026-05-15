package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderPaymentEvent;
import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "spring.kafka.streams.application-id")
public class OrderPaymentJoinTopology {

    public static final String ORDERS_TOPIC  = "orders";
    public static final String OUTPUT_TOPIC  = "orders-payments";

    /**
     * KStream-KTable join:
     *   orders (KTable, keyed by order.id)
     *   ⋈ payments (KStream rekeyed por payment.orderId)
     *   → orders-payments
     *
     * Para que el join produzca resultado, el pedido debe haberse publicado
     * en el topic 'orders' ANTES de que llegue el pago.
     */
    @Bean
    public KStream<String, PaymentEvent> ordersPaymentsJoinStream(StreamsBuilder builder) {
        Serde<OrderMessage>      orderSerde   = orderMessageSerde();
        Serde<PaymentEvent>      paymentSerde = PaymentStreamsTopology.paymentSerde();
        Serde<OrderPaymentEvent> joinedSerde  = orderPaymentSerde();

        // KTable: pedidos indexados por su id
        KTable<String, OrderMessage> ordersTable = builder.table(
                ORDERS_TOPIC, Consumed.with(Serdes.String(), orderSerde));

        // KStream: pagos re-keyed por orderId para coincidir con la clave de la KTable
        KStream<String, PaymentEvent> paymentsRekeyed = builder
                .stream(PaymentStreamsTopology.INPUT_TOPIC, Consumed.with(Serdes.String(), paymentSerde))
                .selectKey((key, payment) -> payment.getOrderId());

        // Join: enriquece cada pago con los datos del pedido.
        // Joined.with() es necesario para que Kafka Streams resuelva los serdes del
        // topic de repartición interna que crea selectKey().
        paymentsRekeyed
                .join(ordersTable,
                        (payment, order) -> OrderPaymentEvent.builder()
                                .orderId(order.getId())
                                .customerId(order.getCustomerId())
                                .orderTotal(order.getTotal())
                                .paymentId(payment.getId())
                                .paymentAmount(payment.getAmount())
                                .paymentCurrency(payment.getCurrency())
                                .paymentStatus(payment.getStatus())
                                .build(),
                        Joined.with(Serdes.String(), paymentSerde, orderSerde))
                .peek((key, joined) -> log.info("[JOIN] orderId={} | paymentId={} | status={}",
                        joined.getOrderId(), joined.getPaymentId(), joined.getPaymentStatus()))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), joinedSerde));

        return paymentsRekeyed;
    }

    static Serde<OrderMessage> orderMessageSerde() {
        JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
        Serializer<OrderMessage> serializer = (topic, data) -> {
            try { return mapper.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException("Error serializando OrderMessage", e); }
        };
        Deserializer<OrderMessage> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try { return mapper.readValue(bytes, OrderMessage.class); }
            catch (Exception e) { throw new RuntimeException("Error deserializando OrderMessage", e); }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }

    static Serde<OrderPaymentEvent> orderPaymentSerde() {
        JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
        Serializer<OrderPaymentEvent> serializer = (topic, data) -> {
            try { return mapper.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException("Error serializando OrderPaymentEvent", e); }
        };
        Deserializer<OrderPaymentEvent> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try { return mapper.readValue(bytes, OrderPaymentEvent.class); }
            catch (Exception e) { throw new RuntimeException("Error deserializando OrderPaymentEvent", e); }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }
}