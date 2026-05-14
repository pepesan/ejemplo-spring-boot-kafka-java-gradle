# Getting Started

### Reference Documentation

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

### Guides

The following guides illustrate how to use some features concretely:

* [Samples for using Apache Kafka Streams with Spring Cloud stream](https://github.com/spring-cloud/spring-cloud-stream-samples/tree/main/kafka-streams-samples)
* [Building a RESTful Web Service](https://spring.io/guides/gs/rest-service/)
* [Serving Web Content with Spring MVC](https://spring.io/guides/gs/serving-web-content/)
* [Building REST services with Spring](https://spring.io/guides/tutorials/rest/)
* [Building a Reactive RESTful Web Service](https://spring.io/guides/gs/reactive-rest-service/)

### Additional Links

These additional references should also help you:

* [Gradle Build Scans – insights for your project's build](https://scans.gradle.com#gradle)


### Arranque de la aplicación
```shell
./gradlew bootRun
```

O en la config de Intellij -> Edit Configurations
Run: **bootRun**

Debería arrancar en el puerto 8080

### URLs principales

| Recurso | URL |
|---|---|
| Aplicación | http://localhost:8080/ |
| Actuator (raíz) | http://localhost:8080/actuator |
| Health | http://localhost:8080/actuator/health |
| Info | http://localhost:8080/actuator/info |
| Metrics | http://localhost:8080/actuator/metrics |
| Env | http://localhost:8080/actuator/env |

> Por defecto solo `health` e `info` están expuestos. Para exponer todos los endpoints añade en `application.properties`:
> ```properties
> management.endpoints.web.exposure.include=*
> ```




