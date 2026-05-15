package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.PaymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Duration;

@Slf4j
@Configuration
@EnableKafkaStreams
@ConditionalOnProperty(name = "spring.kafka.streams.application-id")
public class PaymentStreamsTopology {

    public static final String INPUT_TOPIC        = "payments";
    public static final String HIGH_VALUE_TOPIC   = "payments-high-value";
    public static final String COUNT_STORE        = "payment-count-by-status";
    public static final String WINDOW_COUNT_STORE = "payment-count-by-status-window";
    public static final Duration WINDOW_SIZE      = Duration.ofMinutes(1);
    static final BigDecimal HIGH_VALUE_THRESHOLD  = new BigDecimal("1000");

    @Bean
    public KStream<String, PaymentEvent> paymentsStream(StreamsBuilder builder) {
        Serde<PaymentEvent> serde = paymentSerde();

        KStream<String, PaymentEvent> payments = builder.stream(
                INPUT_TOPIC, Consumed.with(Serdes.String(), serde));

        // Rama 1: pagos de alto valor → topic payments-high-value
        payments
                .filter((key, payment) -> payment.getAmount().compareTo(HIGH_VALUE_THRESHOLD) > 0)
                .peek((key, payment) -> log.info("[STREAMS] Alto valor: id={} amount={}", payment.getId(), payment.getAmount()))
                .to(HIGH_VALUE_TOPIC, Produced.with(Serdes.String(), serde));

        // Rama 2: contador de pagos por estado → state store consultable via REST
        payments
                .groupBy(
                        (key, payment) -> payment.getStatus(),
                        Grouped.with(Serdes.String(), serde))
                .count(Materialized.as(COUNT_STORE));

        // Rama 3: contador por ventana temporal (tumbling 1 min) → state store consultable via REST
        payments
                .groupBy(
                        (key, payment) -> payment.getStatus(),
                        Grouped.with(Serdes.String(), serde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
                .count(Materialized.as(WINDOW_COUNT_STORE));

        return payments;
    }

    // Serde construido con Jackson nativo (tools.jackson) para evitar importar las clases
    // de Spring Kafka (JsonSerde, JsonDeserializer) cuya bytecode es incompatible con javac 25.
    static Serde<PaymentEvent> paymentSerde() {
        JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();

        Serializer<PaymentEvent> serializer = (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializando PaymentEvent", e);
            }
        };

        Deserializer<PaymentEvent> deserializer = (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return mapper.readValue(bytes, PaymentEvent.class);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializando PaymentEvent", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}