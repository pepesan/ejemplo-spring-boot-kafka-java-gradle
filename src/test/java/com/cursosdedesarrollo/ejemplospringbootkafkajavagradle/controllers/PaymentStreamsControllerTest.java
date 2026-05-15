package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentStreamsControllerTest {

    @Mock
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @InjectMocks
    private PaymentStreamsController controller;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(controller).build();
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCountByStatus_devuelveConteoDesdeStateStore() {
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<String, Long> store = mock(ReadOnlyKeyValueStore.class);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(mockStreams);
        when(mockStreams.store(any())).thenReturn(store);
        when(store.get("APPROVED")).thenReturn(3L);

        webTestClient.get()
                .uri("/payments/count/APPROVED")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Long.class)
                .isEqualTo(3L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void getCountByStatus_devuelveCeroCuandoNoHayDatos() {
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<String, Long> store = mock(ReadOnlyKeyValueStore.class);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(mockStreams);
        when(mockStreams.store(any())).thenReturn(store);
        when(store.get("PENDING")).thenReturn(null);

        webTestClient.get()
                .uri("/payments/count/PENDING")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Long.class)
                .isEqualTo(0L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void getWindowedCountByStatus_devuelveVentanasDesdeStateStore() {
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        ReadOnlyWindowStore<String, Long> windowStore = mock(ReadOnlyWindowStore.class);
        WindowStoreIterator<Long> iterator = mock(WindowStoreIterator.class);

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(mockStreams);
        when(mockStreams.store(any())).thenReturn(windowStore);
        when(windowStore.fetch(any(), any(Instant.class), any(Instant.class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(KeyValue.pair(Instant.parse("2026-05-15T10:00:00Z").toEpochMilli(), 3L));

        webTestClient.get()
                .uri("/payments/count/APPROVED/windowed")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .value(map -> assertThat(map).hasSize(1).containsValue(3));
    }

    @Test
    void getCountByStatus_devuelve503CuandoStreamsEstaArrancando() {
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(mockStreams);
        when(mockStreams.store(any())).thenThrow(new InvalidStateStoreException("stream thread is STARTING, not RUNNING"));

        webTestClient.get()
                .uri("/payments/count/APPROVED")
                .exchange()
                .expectStatus().isEqualTo(503);
    }

    @Test
    void getWindowedCountByStatus_devuelve503CuandoStreamsEstaArrancando() {
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(mockStreams);
        when(mockStreams.store(any())).thenThrow(new InvalidStateStoreException("stream thread is STARTING, not RUNNING"));

        webTestClient.get()
                .uri("/payments/count/APPROVED/windowed")
                .exchange()
                .expectStatus().isEqualTo(503);
    }

    @Test
    @SuppressWarnings("unchecked")
    void getWindowedCountByStatus_devuelveMapaVacioSinVentanas() {
        KafkaStreams mockStreams = mock(KafkaStreams.class);
        ReadOnlyWindowStore<String, Long> windowStore = mock(ReadOnlyWindowStore.class);
        WindowStoreIterator<Long> iterator = mock(WindowStoreIterator.class);

        when(streamsBuilderFactoryBean.getKafkaStreams()).thenReturn(mockStreams);
        when(mockStreams.store(any())).thenReturn(windowStore);
        when(windowStore.fetch(any(), any(Instant.class), any(Instant.class))).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        webTestClient.get()
                .uri("/payments/count/PENDING/windowed")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Map.class)
                .value(map -> assertThat(map).isEmpty());
    }
}