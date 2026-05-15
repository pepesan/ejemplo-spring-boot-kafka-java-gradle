package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(properties = "spring.kafka.streams.state-dir=/tmp/kafka-streams-ctx-test")
@DirtiesContext
class EjemploSpringBootKafkaJavaGradleApplicationTests {

    @Test
    void contextLoads() {
    }

}
