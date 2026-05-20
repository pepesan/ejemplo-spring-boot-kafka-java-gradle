package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationMessage {
    @NotBlank
    private String id;
    @NotBlank
    private String recipient;
    @NotBlank
    private String subject;
    @NotBlank
    private String body;
    private Instant createdAt;
}
