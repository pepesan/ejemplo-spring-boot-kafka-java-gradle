package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams.PaymentStreamsTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
public class PaymentStreamsController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @GetMapping("/count/{status}")
    public Mono<Long> getCountByStatus(@PathVariable String status) {
        try {
            KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
            ReadOnlyKeyValueStore<String, Long> store = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            PaymentStreamsTopology.COUNT_STORE,
                            QueryableStoreTypes.keyValueStore()));
            Long count = store.get(status);
            log.info("[STREAMS] Consulta count | status={} | count={}", status, count);
            return Mono.justOrEmpty(count).defaultIfEmpty(0L);
        } catch (InvalidStateStoreException e) {
            log.warn("[STREAMS] State store no disponible | store={} | estado={}", PaymentStreamsTopology.COUNT_STORE, e.getMessage());
            return Mono.error(new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka Streams arrancando, intenta de nuevo en unos segundos"));
        }
    }

    @GetMapping("/count/{status}/windowed")
    public Mono<Map<String, Long>> getWindowedCountByStatus(@PathVariable String status) {
        try {
            KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
            ReadOnlyWindowStore<String, Long> windowStore = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            PaymentStreamsTopology.WINDOW_COUNT_STORE,
                            QueryableStoreTypes.windowStore()));

            Instant to = Instant.now();
            Instant from = to.minus(Duration.ofHours(1));

            Map<String, Long> result = new LinkedHashMap<>();
            try (WindowStoreIterator<Long> iterator = windowStore.fetch(status, from, to)) {
                while (iterator.hasNext()) {
                    KeyValue<Long, Long> kv = iterator.next();
                    result.put(Instant.ofEpochMilli(kv.key).toString(), kv.value);
                }
            }
            log.info("[STREAMS] Consulta windowed count | status={} | ventanas={}", status, result.size());
            return Mono.just(result);
        } catch (InvalidStateStoreException e) {
            log.warn("[STREAMS] State store no disponible | store={} | estado={}", PaymentStreamsTopology.WINDOW_COUNT_STORE, e.getMessage());
            return Mono.error(new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka Streams arrancando, intenta de nuevo en unos segundos"));
        }
    }
}