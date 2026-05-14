package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.controllers;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class MainController {
    @GetMapping("/")
    public String home() {
        log.info("[CONTROLLER] GET / | health check");
        return "SERVER OK";
    }
}
