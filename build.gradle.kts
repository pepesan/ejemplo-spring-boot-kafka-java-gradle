import java.io.File
import java.net.ServerSocket
import java.net.SocketException

plugins {
    java
    id("org.springframework.boot") version "4.0.6"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.cursosdedesarrollo"
version = "0.0.1-SNAPSHOT"
description = "ejemplo-spring-boot-kafka-java-gradle"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.apache.kafka:kafka-streams")
    implementation("com.h2database:h2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    compileOnly("org.projectlombok:lombok")
    developmentOnly("org.springframework.boot:spring-boot-devtools")
    annotationProcessor("org.projectlombok:lombok")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testCompileOnly("org.projectlombok:lombok")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testAnnotationProcessor("org.projectlombok:lombok")
}

tasks.withType<Test> {
    useJUnitPlatform()
    reports {
        html.required = true      // build/reports/tests/test/index.html
        junitXml.required = true  // build/test-results/test/*.xml
    }
}

tasks.register("serveTestReport") {
    group = "verification"
    description = "Sirve el informe HTML de tests en http://localhost:8888 (Ctrl+C para parar)"

    doLast {
        val reportDir = layout.buildDirectory.dir("reports/tests/test").get().asFile
        if (!reportDir.exists()) {
            throw GradleException("No hay informe de tests. Ejecuta './gradlew test' primero.")
        }

        val port = 8888
        val serverSocket = ServerSocket(port)

        logger.lifecycle("Informe de tests disponible en http://localhost:$port")
        logger.lifecycle("Pulsa Ctrl+C para detener el servidor")

        runCatching { ProcessBuilder("xdg-open", "http://localhost:$port").start() }

        while (!serverSocket.isClosed) {
            try {
                val client = serverSocket.accept()
                Thread {
                    client.use { socket ->
                        val requestLine = socket.inputStream.bufferedReader().readLine() ?: return@Thread
                        val rawPath = requestLine.split(" ").getOrElse(1) { "/" }.split("?")[0]
                        val path = if (rawPath == "/") "/index.html" else rawPath

                        val file = File(reportDir, path)
                        val out = socket.outputStream

                        if (file.exists() && file.isFile &&
                            file.canonicalPath.startsWith(reportDir.canonicalPath)) {
                            val bytes = file.readBytes()
                            val contentType = when (file.extension.lowercase()) {
                                "html" -> "text/html; charset=utf-8"
                                "css"  -> "text/css"
                                "js"   -> "application/javascript"
                                "png"  -> "image/png"
                                "gif"  -> "image/gif"
                                "svg"  -> "image/svg+xml"
                                else   -> "application/octet-stream"
                            }
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: $contentType\r\nContent-Length: ${bytes.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(bytes)
                        } else {
                            val body = "404 Not Found".toByteArray()
                            out.write("HTTP/1.1 404 Not Found\r\nContent-Length: ${body.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(body)
                        }
                        out.flush()
                    }
                }.start()
            } catch (_: SocketException) {
                break
            }
        }
    }
}
