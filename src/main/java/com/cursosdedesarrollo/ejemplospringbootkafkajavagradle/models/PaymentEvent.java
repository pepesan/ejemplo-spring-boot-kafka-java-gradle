package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    @NotBlank
    private String id;
    @NotBlank
    private String orderId;
    @NotNull @DecimalMin("0.01")
    private BigDecimal amount;
    @NotBlank
    private String currency;
    @NotBlank
    @Pattern(regexp = "PENDING|APPROVED|REJECTED", message = "status debe ser PENDING, APPROVED o REJECTED")
    private String status;
}