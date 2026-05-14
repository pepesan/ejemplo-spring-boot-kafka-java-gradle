# Proyecto de ejemplo de Spring Boot Kafka 4 

## Arranque del servidor Kafka

#### Creación automática de topics

El broker tiene `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` en el `compose.yaml`. Al arrancar la aplicación, el `@KafkaListener` de `ProductConsumer` provoca que Kafka cree automáticamente el topic `products` con la configuración por defecto (1 partición, replication factor 1).

Por eso el script `docker/04_create_topic.sh` usa `--if-not-exists`: evita el error `TopicExistsException` si el topic ya fue creado por la app.

### Scripts docker

| Script                          | Descripción                                        |
|---------------------------------|----------------------------------------------------|
| `01_launch.sh`                  | Levanta el entorno Docker                          |
| `04_create_topic.sh`            | Crea el topic `products` (idempotente)             |
| `05_list_topic.sh`              | Lista los topics existentes                        |
| `06_produce_message_topic.sh`   | Produce un mensaje manual al topic                 |
| `07_consume_message_topic.sh`   | Consume mensajes del topic                         |
| `08_detail_topic.sh`            | Detalle del topic                                  |
| `09_list_consumer_group.sh`     | Lista los consumer groups                          |
| `10_describe_consumer_group.sh` | Describe el consumer group                         |
| `20_destroy.sh`                 | Para y elimina el entorno Docker                   |

### URLs principales

| Recurso                    | URL                    |
|----------------------------|------------------------|
| Kafka (solo applicaciones) | localhost:9092         |
| Kafka UI                   | http://localhost:8081/ |

## Arranque de la aplicación
```shell
./gradlew bootRun
```

O en la config de Intellij -> Edit Configurations
Run: **bootRun**

Debería arrancar en el puerto 8080

### URLs principales

| Recurso          | URL                                    |
|------------------|----------------------------------------|
| Aplicación       | http://localhost:8080/                 |
| Actuator (raíz)  | http://localhost:8080/actuator         |
| Health           | http://localhost:8080/actuator/health  |
| Info             | http://localhost:8080/actuator/info    |
| Metrics          | http://localhost:8080/actuator/metrics |
| Env              | http://localhost:8080/actuator/env     |

> Por defecto solo `health` e `info` están expuestos. Para exponer todos los endpoints añade en `application.yaml`:
> ```yaml
> management:
>   endpoints:
>     web:
>       exposure:
>         include: "*"
> ```

## Tests

### Todas las pruebas a la vez

```bash
./gradlew test
```

### Solo tests unitarios

Los tests unitarios cubren producers, consumers y controllers de forma aislada (sin levantar Kafka ni el contexto completo de Spring).

```bash
./gradlew test --tests "*.producers.*" --tests "*.consumers.*" --tests "*.controllers.*"
```

### Solo tests de aceptación

Los tests de aceptación levantan el contexto completo de Spring Boot con un Kafka embebido y verifican el flujo end-to-end (REST → Kafka → Consumer → REST).

```bash
./gradlew test --tests "*.acceptance.*"
```

### Una prueba específica

Para ejecutar una clase de test concreta:

```bash
./gradlew test --tests "com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.OrderProducerTest"
```

Para ejecutar un método de test concreto:

```bash
./gradlew test --tests "com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.producers.OrderProducerTest.send_delegatesToKafkaTemplate"
```

> También se puede usar el comodín `*` en el nombre del paquete o de la clase:
> ```bash
> ./gradlew test --tests "*OrderProducerTest*"
> ```

### Estructura de los tests

| Paquete                  | Tipo        | Descripción                                              |
|--------------------------|-------------|----------------------------------------------------------|
| `producers`              | Unitario    | Verifica que el producer delega en `KafkaTemplate`       |
| `consumers`              | Unitario    | Verifica que el consumer almacena mensajes en memoria    |
| `controllers`            | Unitario    | Verifica los endpoints REST con `@WebFluxTest` y mocks   |
| `acceptance`             | Aceptación  | Flujo completo con Kafka embebido vía `@EmbeddedKafka`   |

---





## Flujo completo productor → consumer

1. Enviar un producto al topic:

```bash
./scripts/01_send_product.sh
```

Respuesta esperada:
```json
{"id":"1","name":"TV 4K","description":"Televisor 55 pulgadas","price":499.99,"stock":10}
```

2. Consultar los productos consumidos:

```bash
./scripts/02_get_consumed.sh
```

Respuesta esperada:
```json
[{"id":"1","name":"TV 4K","description":"Televisor 55 pulgadas","price":499.99,"stock":10}]
```

> Los mensajes se almacenan en memoria. Al reiniciar la aplicación la lista se vacía, pero los mensajes siguen disponibles en el topic de Kafka.

### Scripts de la aplicación

| Script                       | Descripción                                      |
|------------------------------|--------------------------------------------------|
| `scripts/01_send_product.sh` | Envía un producto de ejemplo al topic `products` |
| `scripts/02_get_consumed.sh` | Consulta los productos recibidos por el consumer |

### Endpoints REST

| Método | URL                                       | Descripción                             |
|--------|-------------------------------------------|-----------------------------------------|
| POST   | `http://localhost:8080/products`          | Envía un producto al topic              |
| GET    | `http://localhost:8080/consumed/products` | Lista los productos consumidos          |

### Verificar mensajes con Kafka UI

1. Abre http://localhost:8081/
2. Selecciona el cluster `demo-cluster`
3. Ve a **Topics** → `products`
4. Haz clic en la pestaña **Messages** para ver los mensajes enviados

## Reference Documentation

For further reference, please consider the following sections:

* [Official Gradle documentation](https://docs.gradle.org)
* [Spring Boot Gradle Plugin Reference Guide](https://docs.spring.io/spring-boot/4.0.6/gradle-plugin)
* [Create an OCI image](https://docs.spring.io/spring-boot/4.0.6/gradle-plugin/packaging-oci-image.html)
* [Spring Boot DevTools](https://docs.spring.io/spring-boot/4.0.6/reference/using/devtools.html)
* [Spring for Apache Kafka](https://docs.spring.io/spring-boot/4.0.6/reference/messaging/kafka.html)
* [Apache Kafka Streams Support](https://docs.spring.io/spring-kafka/reference/streams.html)
* [Apache Kafka Streams Binding Capabilities of Spring Cloud Stream](https://docs.spring.io/spring-cloud-stream/reference/kafka/kafka-streams-binder/usage.html)
* [Spring Web](https://docs.spring.io/spring-boot/4.0.6/reference/web/servlet.html)
* [Spring Reactive Web](https://docs.spring.io/spring-boot/4.0.6/reference/web/reactive.html)

## Guides

The following guides illustrate how to use some features concretely:

* [Samples for using Apache Kafka Streams with Spring Cloud stream](https://github.com/spring-cloud/spring-cloud-stream-samples/tree/main/kafka-streams-samples)
* [Building a RESTful Web Service](https://spring.io/guides/gs/rest-service/)
* [Serving Web Content with Spring MVC](https://spring.io/guides/gs/serving-web-content/)
* [Building REST services with Spring](https://spring.io/guides/tutorials/rest/)
* [Building a Reactive RESTful Web Service](https://spring.io/guides/gs/reactive-rest-service/)

## Additional Links

These additional references should also help you:

* [Gradle Build Scans – insights for your project's build](https://scans.gradle.com#gradle)



