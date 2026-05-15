# Proyecto de ejemplo de Spring Boot Kafka 4 

## Arranque del servidor Kafka

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

### Arranque de los servicios

```shell
cd docker
./01_launch.sh
```

### URLs principales

| Recurso                    | URL                    |
|----------------------------|------------------------|
| Kafka (solo applicaciones) | localhost:9092         |
| Kafka UI                   | http://localhost:8081/ |

### Creación automática de topics

El broker tiene `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` en el `compose.yaml`. Al arrancar la aplicación, el `@KafkaListener` de `ProductConsumer` provoca que Kafka cree automáticamente el topic `products` con la configuración por defecto (1 partición, replication factor 1).

Por eso el script `docker/04_create_topic.sh` usa `--if-not-exists`: evita el error `TopicExistsException` si el topic ya fue creado por la app.





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





## Flujo Products — productor → consumer

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

> Los mensajes se persisten en la base de datos H2 en memoria. Al reiniciar la aplicación la base de datos se vacía, pero los mensajes siguen disponibles en el topic de Kafka.

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

## Flujo Orders — productor → consumer

```
POST /orders
    → OrderProducer → topic: orders
        → OrderConsumer (persiste en H2 vía JPA)
            ↑
    GET /orders/consumed
```

### Modelo `OrderMessage`

| Campo        | Tipo                | Descripción                          |
|--------------|---------------------|--------------------------------------|
| `id`         | `String`            | Identificador del pedido             |
| `customerId` | `String`            | Identificador del cliente            |
| `lines`      | `List<OrderLine>`   | Líneas del pedido                    |
| `total`      | `BigDecimal`        | Importe total del pedido             |
| `createdAt`  | `LocalDateTime`     | Fecha y hora de creación             |

Cada `OrderLine` contiene: `productId`, `productName`, `quantity`, `unitPrice`.

### Endpoints REST

| Método | URL                                    | Descripción                          |
|--------|----------------------------------------|--------------------------------------|
| POST   | `http://localhost:8080/orders`         | Envía un pedido al topic `orders`    |
| GET    | `http://localhost:8080/orders/consumed` | Lista los pedidos consumidos         |

### Scripts de la aplicación

| Script                              | Descripción                              |
|-------------------------------------|------------------------------------------|
| `scripts/03_send_order.sh`          | Envía un pedido de ejemplo               |
| `scripts/04_get_consumed_orders.sh` | Lista los pedidos recibidos por el consumer |

### Ejemplos de uso

1. Enviar un pedido:

```bash
./scripts/03_send_order.sh
```

O directamente con curl:

```bash
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ORD-001",
    "customerId": "CUST-42",
    "lines": [
      { "productId": "1", "productName": "TV 4K",          "quantity": 2, "unitPrice": 499.99 },
      { "productId": "2", "productName": "Mando Universal", "quantity": 1, "unitPrice": 19.99 }
    ],
    "total": 1019.97,
    "createdAt": "2026-05-14T19:00:00"
  }' | jq .
```

Respuesta esperada (HTTP 202 Accepted):
```json
{
  "id": "ORD-001",
  "customerId": "CUST-42",
  "lines": [
    { "productId": "1", "productName": "TV 4K",           "quantity": 2, "unitPrice": 499.99 },
    { "productId": "2", "productName": "Mando Universal",  "quantity": 1, "unitPrice": 19.99 }
  ],
  "total": 1019.97,
  "createdAt": "2026-05-14T19:00:00"
}
```

1. Consultar los pedidos consumidos:

```bash
./scripts/04_get_consumed_orders.sh
```

O directamente con curl:

```bash
curl -s http://localhost:8080/orders/consumed | jq .
```

> Los mensajes se persisten en la base de datos H2 en memoria. Al reiniciar la aplicación la base de datos se vacía, pero los mensajes siguen disponibles en el topic de Kafka.

### Verificar mensajes con Kafka UI

1. Abre http://localhost:8081/
2. Selecciona el cluster `demo-cluster`
3. Ve a **Topics** → `orders`
4. Haz clic en la pestaña **Messages** para ver los pedidos enviados

---

## Kafka Streams — Flujo de pagos

La topología de Kafka Streams procesa los mensajes del topic `payments` en tiempo real y realiza tres operaciones en paralelo:

```
POST /payments
    → PaymentProducer → topic: payments
        → PaymentStreamsTopology (Kafka Streams)
            ├─ amount > 1000 → topic: payments-high-value
            ├─ agrupa por status → state store: payment-count-by-status
            │                           ↑
            │                GET /payments/count/{status}
            └─ agrupa por status + ventana 1 min → state store: payment-count-by-status-window
                                                        ↑
                                         GET /payments/count/{status}/windowed
```

### Modelo `PaymentEvent`

| Campo      | Tipo         | Descripción                            |
|------------|--------------|----------------------------------------|
| `id`       | `String`     | Identificador del pago                 |
| `orderId`  | `String`     | Identificador del pedido asociado      |
| `amount`   | `BigDecimal` | Importe del pago                       |
| `currency` | `String`     | Divisa (p.ej. `EUR`)                   |
| `status`   | `String`     | Estado: `PENDING`, `APPROVED`, `REJECTED` |

### Endpoints REST

| Método | URL                                                   | Descripción                                                        |
|--------|-------------------------------------------------------|--------------------------------------------------------------------|
| POST   | `http://localhost:8080/payments`                      | Envía un pago al topic `payments`                                  |
| GET    | `http://localhost:8080/payments/count/{status}`       | Contador total de pagos por estado (state store global)            |
| GET    | `http://localhost:8080/payments/count/{status}/windowed` | Contador de pagos por estado agrupado por ventanas de 1 minuto  |

### Scripts de la aplicación

| Script                                      | Descripción                                                  |
|---------------------------------------------|--------------------------------------------------------------|
| `scripts/05_send_payment.sh`                | Envía un pago de ejemplo (`amount: 1500` → alto valor)       |
| `scripts/06_get_payment_count.sh [STATUS]`  | Consulta el contador total por estado (`APPROVED` por defecto) |
| `scripts/07_get_windowed_count.sh [STATUS]` | Consulta el contador por ventanas temporales (`APPROVED` por defecto) |

### Ejemplos de uso

1. Enviar un pago (amount > 1000, se enruta también a `payments-high-value`):

```bash
./scripts/05_send_payment.sh
```

O directamente con curl:

```bash
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "id": "PAY-001",
    "orderId": "ORD-001",
    "amount": 1500.00,
    "currency": "EUR",
    "status": "APPROVED"
  }' | jq .
```

1. Consultar cuántos pagos hay con un estado concreto:

```bash
./scripts/06_get_payment_count.sh APPROVED
./scripts/06_get_payment_count.sh PENDING
./scripts/06_get_payment_count.sh REJECTED
```

O directamente con curl:

```bash
curl -s http://localhost:8080/payments/count/APPROVED | jq .
```

Respuesta esperada: `2`

1. Consultar el contador de pagos por ventanas temporales de 1 minuto:

```bash
./scripts/07_get_windowed_count.sh APPROVED
```

O directamente con curl:

```bash
curl -s http://localhost:8080/payments/count/APPROVED/windowed | jq .
```

Respuesta esperada (un objeto cuyas claves son los instantes de inicio de cada ventana):
```json
{
  "2026-05-15T10:00:00Z": 3,
  "2026-05-15T10:01:00Z": 1
}
```

Cada clave es el timestamp ISO-8601 del inicio de la ventana de 1 minuto. El valor es el número de pagos con ese estado recibidos durante ese minuto. Se devuelven las ventanas de la última hora.

### Verificar en Kafka UI

1. Abre http://localhost:8081/
2. Ve a **Topics** → `payments` para ver todos los pagos
3. Ve a **Topics** → `payments-high-value` para ver solo los pagos con `amount > 1000`

### Detalles de implementación

- La topología se activa solo si `spring.kafka.streams.application-id` está definido en `application.yaml`. Esto evita que se cargue en los tests slice (`@WebFluxTest`) donde no hay Kafka.
- El Serde usa Jackson nativo (`tools.jackson`) en lugar de `JsonSerde` de Spring Kafka, por incompatibilidad de las clases de Spring Kafka con el compilador Java 25.
- El state store `payment-count-by-status` es una tabla en memoria mantenida automáticamente por Kafka Streams y consultable vía REST.

---

## Kafka Streams — Join orders + payments

La topología de join combina en tiempo real los pedidos con sus pagos, produciendo un evento enriquecido en el topic `orders-payments`.

```
POST /orders  →  topic: orders  ──────────────────────────────────┐
                                                                   ▼
POST /payments → topic: payments → (re-key por orderId) → KStream-KTable JOIN
                                                                   │
                                                  topic: orders-payments
                                                                   │
                                                   OrderPaymentConsumer (en memoria)
                                                                   ↑
                                             GET /orders-payments/consumed
```

**Tipo de join:** KStream-KTable. Por cada pago que llega, se busca el pedido correspondiente en la KTable (indexada por `orderId`). Si no existe el pedido, el pago se descarta silenciosamente.

**Requisito:** el pedido debe publicarse en `orders` **antes** de que llegue el pago con ese `orderId`.

### Modelo `OrderPaymentEvent`

| Campo             | Tipo         | Descripción                               |
|-------------------|--------------|-------------------------------------------|
| `orderId`         | `String`     | ID del pedido                             |
| `customerId`      | `String`     | Cliente del pedido                        |
| `orderTotal`      | `BigDecimal` | Importe total del pedido                  |
| `paymentId`       | `String`     | ID del pago                               |
| `paymentAmount`   | `BigDecimal` | Importe del pago                          |
| `paymentCurrency` | `String`     | Divisa del pago                           |
| `paymentStatus`   | `String`     | Estado: `PENDING`, `APPROVED`, `REJECTED` |

### Endpoints REST

| Método | URL                                             | Descripción                               |
|--------|-------------------------------------------------|-------------------------------------------|
| GET    | `http://localhost:8080/orders-payments/consumed` | Lista los eventos join persistidos en H2 |

### Scripts de la aplicación

| Script                                  | Descripción                                    |
|-----------------------------------------|------------------------------------------------|
| `scripts/08_get_joined_orders_payments.sh` | Lista los eventos join persistidos en H2 |

### Ejemplo de uso

1. Enviar un pedido (quedará en la KTable de orders):

```bash
./scripts/03_send_order.sh
```

2. Enviar un pago con el mismo `orderId` (`ORD-001`):

```bash
./scripts/05_send_payment.sh
```

3. Consultar los eventos join producidos:

```bash
./scripts/08_get_joined_orders_payments.sh
```

Respuesta esperada:
```json
[
  {
    "orderId": "ORD-001",
    "customerId": "CUST-42",
    "orderTotal": 1019.97,
    "paymentId": "PAY-001",
    "paymentAmount": 1500.00,
    "paymentCurrency": "EUR",
    "paymentStatus": "APPROVED"
  }
]
```

> Si el pedido no existía en el topic `orders` cuando llegó el pago, el join no produce resultado (el pago se descarta). Reenvía el pedido y vuelve a intentarlo.

---

## Persistencia con H2

Los consumers de Kafka persisten los mensajes recibidos en una base de datos H2 embebida en memoria usando Spring Data JPA.

### Tablas creadas automáticamente

| Tabla               | Entidad              | Descripción                          |
|---------------------|----------------------|--------------------------------------|
| `product_message`   | `ProductMessage`     | Productos recibidos del topic        |
| `order_messages`    | `OrderMessage`       | Pedidos recibidos del topic          |
| `order_lines`       | `OrderLine`          | Líneas de cada pedido (colección)    |
| `order_payment_event` | `OrderPaymentEvent` | Eventos join orders-payments        |

### Consola H2

Disponible en http://localhost:8080/h2-console mientras la aplicación está en ejecución.

- **JDBC URL**: `jdbc:h2:mem:kafkadb`
- **Usuario**: `sa`
- **Contraseña**: *(vacía)*

### Persistencia en fichero

Para conservar los datos entre reinicios, cambia en `application.yaml`:

```yaml
spring:
  datasource:
    # url: jdbc:h2:mem:kafkadb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE  # en memoria
    url: jdbc:h2:file:./data/kafka-events  # en fichero (crea ./data/kafka-events.mv.db)
```

> Los datos se pierden igualmente si cambias `ddl-auto: create-drop` (por defecto) a `update` cuando uses el modo fichero.

---

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



