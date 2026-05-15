package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.web.reactive.server.WebTestClient;

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
}