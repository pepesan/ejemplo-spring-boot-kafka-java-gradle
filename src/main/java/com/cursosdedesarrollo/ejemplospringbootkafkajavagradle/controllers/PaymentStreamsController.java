package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.streams.PaymentStreamsTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/payments")
@RequiredArgsConstructor
public class PaymentStreamsController {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @GetMapping("/count/{status}")
    public Mono<Long> getCountByStatus(@PathVariable String status) {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        PaymentStreamsTopology.COUNT_STORE,
                        QueryableStoreTypes.keyValueStore()));
        Long count = store.get(status);
        log.info("[STREAMS] Consulta count | status={} | count={}", status, count);
        return Mono.justOrEmpty(count).defaultIfEmpty(0L);
    }
}